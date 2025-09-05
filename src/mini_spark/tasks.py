from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from .algorithms import external_merge_join, external_sort
from .constants import (
    SHUFFLE_FOLDER,
    SHUFFLE_PARTITIONS,
    Columns,
    ColumnType,
    Row,
    Schema,
)
from .io import BlockFile
from .jobs import Job, JoinJob, LoadShuffleFilesJob, OutputFile, ScanJob
from .sql import BinaryOperatorColumn, Col
from .utils import convert_rows_to_columns, create_temp_file, nice_schema, trace, trace_yield

if TYPE_CHECKING:
    from collections.abc import Iterable


JoinType = Literal["inner", "left", "right", "outer"]


@trace("project_col")
def project_column(col: Col, chunk: Columns, schema: Schema) -> list[Any]:
    col = col.schema_executor(schema)
    return [col.execute_row(row) for row in zip(*chunk, strict=True)]


@dataclass
class Task(ABC):
    parent_task: Task = field(repr=False)
    inferred_schema: Schema | None = None

    @abstractmethod
    def explain(self, lvl: int = 0) -> None: ...

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    @property
    def task_chain(self) -> Iterable[Task]:
        if type(self) is VoidTask:
            return
        yield from self.parent_task.task_chain
        yield self


@dataclass(kw_only=True)
class ProducerTask(Task):
    @abstractmethod
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]: ...


@dataclass(kw_only=True)
class ConsumerTask(Task):
    @abstractmethod
    def execute(self, chunk: Columns | None, *, is_last: bool) -> tuple[Columns | None, bool]: ...


@dataclass(kw_only=True)
class WriterTask(Task):
    @abstractmethod
    def write(self, chunk: Columns | None, stage_id: str) -> list[OutputFile]: ...


@dataclass(kw_only=True)
class ProjectTask(ConsumerTask):
    columns: list[Col]

    @trace("ProjectTask")
    def execute(self, chunk: Columns | None, *, is_last: bool) -> tuple[Columns | None, bool]:
        if chunk is None:
            return None, is_last
        assert self.parent_task.inferred_schema is not None
        return tuple(project_column(col, chunk, self.parent_task.inferred_schema) for col in self.columns), is_last

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        # expand * to all columns from previous schema
        self.columns = [
            sub
            for col in self.columns
            for sub in ([Col(name) for name, _ in schema] if type(col) is Col and col.name == "*" else [col])
        ]

        referenced_column_names = {
            col.name for column in self.columns for col in column.all_nested_columns if type(col) is Col
        }
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in projection: {unknown_cols}")
        return [(col.name, col.infer_type(schema)) for col in self.columns]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} Project({', '.join(str(col) for col in self.columns)}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadTableBlockTask(ProducerTask):
    file_path: Path

    @trace_yield("LoadTableBlockTask")
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]:
        assert type(job) is ScanJob
        yield BlockFile(job.file_path).read_block_data_columns_by_id(job.block_id), True

    @cached_property
    def file_schema(self) -> Schema:
        return BlockFile(self.file_path).file_schema

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        return self.file_schema

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} LoadTableBlockTask({self.file_path}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadShuffleFilesTask(ProducerTask):
    @trace_yield("LoadShuffleFileTask")
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]:
        assert type(job) is LoadShuffleFilesJob
        for shuffle_file in job.shuffle_files:
            for block in BlockFile(shuffle_file.file_path).read_block_data_columns_sequentially():
                yield block, False
        yield None, True

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} LoadShuffleFile():{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class FilterTask(ConsumerTask):
    condition: Col

    def __post_init__(self) -> None:
        assert type(self.condition) is BinaryOperatorColumn, type(self.condition)

    @trace("FilterTask")
    def execute(self, chunk: Columns | None, *, is_last: bool) -> tuple[Columns | None, bool]:
        if chunk is None:
            return None, is_last
        assert self.parent_task.inferred_schema is not None
        condition_col = project_column(
            self.condition,
            chunk,
            self.parent_task.inferred_schema,
        )
        return tuple([val for val, cond in zip(col, condition_col, strict=True) if cond] for col in chunk), is_last

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = [col.name for col in self.condition.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Filter: {unknown_cols}")
        return schema

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Filter({self.condition}):{nice_schema(self.inferred_schema)}")  # noqa: T201
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class JoinTask(ProducerTask):
    right_side_task: Task
    join_condition: Col
    how: JoinType = "inner"
    left_key: Col | None = None
    right_key: Col | None = None

    @trace_yield("JoinTask")
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]:
        assert self.left_key
        assert self.right_key
        assert type(job) is JoinJob
        left_sorted_file = self.sort_shuffle_files(
            [file.file_path for file in job.left_shuffle_files],
            self.left_key,
        )
        right_sorted_file = self.sort_shuffle_files(
            [file.file_path for file in job.right_shuffle_files],
            self.right_key,
        )
        joined_rows: list[Row] = []
        if left_sorted_file and right_sorted_file:
            # TODO(david): does not hold for all join types
            joined_rows = list(
                external_merge_join(
                    left_sorted_file,
                    right_sorted_file,
                    self.left_key.execute,
                    self.right_key.execute,
                    self.how,
                ),
            )
        if left_sorted_file is not None:
            left_sorted_file.unlink(missing_ok=True)
        if right_sorted_file is not None:
            right_sorted_file.unlink(missing_ok=True)
        assert self.inferred_schema
        # TODO(david): should be chunked, not all data at once
        yield convert_rows_to_columns(joined_rows, self.inferred_schema), True

    @trace("Sort shuffle file")
    def sort_shuffle_files(self, shuffle_files: list[Path], key: Col) -> Path | None:
        if not shuffle_files:
            return None
        shuffle_file = BlockFile(create_temp_file()).merge_files(shuffle_files).file
        output_file = create_temp_file()
        tmp_file = create_temp_file()
        external_sort(shuffle_file, key.execute, output_file, tmp_file)
        shuffle_file.unlink(missing_ok=True)
        tmp_file.unlink(missing_ok=True)
        return output_file

    def validate_schema(self) -> Schema:
        left_schema = self.parent_task.validate_schema()
        right_schema = self.right_side_task.validate_schema()
        referenced_column_names = [col.name for col in self.join_condition.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in left_schema + right_schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Join: {unknown_cols}")
        return left_schema + right_schema

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f'{indent} Join({self.join_condition}, "{self.how}"):{nice_schema(self.inferred_schema)}'
        )
        self.parent_task.explain(lvl + 1)
        self.right_side_task.explain(lvl + 1)


@dataclass(kw_only=True)
class AggregateCountTask(ConsumerTask):
    group_by_column: Col
    counter: dict[Any, int] = field(default_factory=lambda: Counter())

    @trace("AggregateCountTask")
    def execute(self, chunk: Columns | None, *, is_last: bool) -> tuple[Columns | None, bool]:
        if is_last and chunk is None:
            return (list(self.counter.keys()), list(self.counter.values())), True
        assert chunk is not None
        assert self.parent_task.inferred_schema is not None
        group_column = project_column(self.group_by_column, chunk, self.parent_task.inferred_schema)
        self.counter |= Counter(group_column)
        return None, False

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert self.group_by_column.name in {col_name for col_name, _ in schema}
        return [
            (self.group_by_column.name, self.group_by_column.infer_type(schema)),
            ("count", ColumnType.INTEGER),
        ]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} AggregateCount():{nice_schema(self.inferred_schema)}")  # noqa: T201
        self.parent_task.explain(lvl + 1)


@dataclass
class WriteToShufflePartitions(WriterTask):
    key_column: Col | None = None

    @trace("WriteToShufflePartitions")
    def write(self, chunk: Columns | None, stage_id: str) -> list[OutputFile]:
        if chunk is None:
            return []
        assert self.key_column is not None
        assert self.parent_task.inferred_schema is not None
        key_column = project_column(
            self.key_column,
            chunk,
            self.parent_task.inferred_schema,
        )
        final_output: tuple[list[list[Any]], ...] = tuple([] for _ in range(SHUFFLE_PARTITIONS))
        for col in chunk:
            col_buckets: tuple[list[Any], ...] = tuple([] for _ in range(SHUFFLE_PARTITIONS))
            for val, key in zip(col, key_column, strict=True):
                destination = hash(key) % SHUFFLE_PARTITIONS
                col_buckets[destination].append(val)
            for shuffle, col_bucket in zip(final_output, col_buckets, strict=True):
                shuffle.append(col_bucket)
        shuffle_files = []
        for partition, full_data in enumerate(final_output):
            if len(full_data[0]) == 0:
                continue
            shuffle_file = Path(SHUFFLE_FOLDER / stage_id / f"{partition}.bin")
            shuffle_file.parent.mkdir(parents=True, exist_ok=True)
            data_in_rows = list(zip(*full_data, strict=True))
            BlockFile(shuffle_file, self.parent_task.inferred_schema).append_tuples(data_in_rows)
            shuffle_files.append(OutputFile("local", shuffle_file, partition))
        return shuffle_files

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        if self.key_column is None:
            return schema
        referenced_column_names = [col.name for col in self.key_column.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in GroupBy: {unknown_cols}")
        if self.key_column.name in schema_cols:
            return schema
        return [(self.key_column.name, self.key_column.infer_type(schema)), *schema]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} WriteToShufflePartitions({self.key_column}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class WriteToLocalFileTask(WriterTask):
    @trace("WriteToLocalFileTask")
    def write(self, chunk: Columns | None, stage_id: str) -> list[OutputFile]:
        if chunk is None:
            return []
        assert self.parent_task.inferred_schema is not None
        if len(chunk) == 0 or len(chunk[0]) == 0:
            return []
        output_file = Path(SHUFFLE_FOLDER / stage_id / "result.bin")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        BlockFile(output_file, schema=self.parent_task.inferred_schema).append_data(chunk)
        return [OutputFile("local", output_file)]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} WriteToLocalFileTask():{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass
class VoidTask(Task):
    parent_task: Task | None = None  # type:ignore[assignment]

    def validate_schema(self) -> Schema:
        return []

    def explain(self, lvl: int = 0) -> None:
        pass
