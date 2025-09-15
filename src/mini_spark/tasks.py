from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from .constants import (
    MAX_INT,
    MIN_INT,
    SHUFFLE_FOLDER,
    SHUFFLE_PARTITIONS,
    Columns,
    ColumnTypePython,
    NumericColumnTypes,
    Schema,
)
from .io import BlockFile
from .jobs import Job, JoinJob, LoadShuffleFilesJob, OutputFile, ScanJob
from .sql import AggCol, BinaryOperatorColumn, Col, LikeColumn
from .utils import nice_schema, trace, trace_yield

if TYPE_CHECKING:
    from collections.abc import Iterable


JoinType = Literal["inner", "left", "right", "outer"]


@trace("project_col")
def project_column(col: Col, chunk: Columns, schema: Schema) -> list[ColumnTypePython]:
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
    alias: str = ""

    @trace_yield("LoadTableBlockTask")
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]:
        assert type(job) is ScanJob
        yield BlockFile(job.file_path).read_block_data_columns_by_id(job.block_id), False
        yield None, True

    @cached_property
    def file_schema(self) -> Schema:
        return BlockFile(self.file_path).file_schema

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        if not self.alias:
            return self.file_schema
        return [(f"{self.alias}.{col_name}", col_type) for col_name, col_type in self.file_schema]

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
        assert type(self.condition) in {BinaryOperatorColumn, LikeColumn}, type(self.condition)

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
class BroadcastHashJoinTask(ProducerTask):
    right_side_task: Task
    join_condition: Col
    how: JoinType = "inner"
    left_key: Col | None = None
    right_key: Col | None = None
    left_schema: Schema | None = None
    right_schema: Schema | None = None
    left_row_map: dict[Any, list[int]] = field(default_factory=lambda: defaultdict(list))

    @trace_yield("JoinTask")
    def generate_chunks(self, job: Job) -> Iterable[tuple[Columns | None, bool]]:  # noqa: C901
        assert self.left_key
        assert self.right_key
        assert self.inferred_schema
        assert self.left_schema
        assert self.right_schema
        assert type(job) is JoinJob

        # build hashmap from left side
        load_left = LoadShuffleFilesTask(VoidTask())
        left_columns: Columns = tuple([] for _ in self.left_schema)
        for chunk, is_last in load_left.generate_chunks(LoadShuffleFilesJob(shuffle_files=job.left_shuffle_files)):
            if chunk is None or is_last:
                continue
            key_column = project_column(self.left_key, chunk, self.left_schema)
            for key, row_idx in zip(key_column, range(len(key_column)), strict=True):
                self.left_row_map[key].append(row_idx)
            for i, col in enumerate(chunk):
                left_columns[i].extend(col)

        # generate joined chunks
        load_right = LoadShuffleFilesTask(VoidTask())
        for chunk, is_last in load_right.generate_chunks(LoadShuffleFilesJob(shuffle_files=job.right_shuffle_files)):
            if chunk is None or is_last:
                yield None, True
                return
            output_columns: Columns = tuple([] for _ in self.inferred_schema)
            key_column = project_column(self.right_key, chunk, self.right_schema)
            for right_row_idx, key in enumerate(key_column):
                left_rows_idx = self.left_row_map[key]
                for left_row_idx in left_rows_idx:
                    output_col_idx = 0
                    for left_col in left_columns:
                        output_columns[output_col_idx].append(left_col[left_row_idx])
                        output_col_idx += 1
                    for right_col in chunk:
                        output_columns[output_col_idx].append(right_col[right_row_idx])
                        output_col_idx += 1
            yield output_columns, False

    def validate_schema(self) -> Schema:
        self.left_schema = self.parent_task.validate_schema()
        self.right_schema = self.right_side_task.validate_schema()
        referenced_column_names = [col.name for col in self.join_condition.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in self.left_schema + self.right_schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Join: {unknown_cols}")
        return self.left_schema + self.right_schema

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f'{indent} Join({self.join_condition}, "{self.how}"):{nice_schema(self.inferred_schema)}'
        )
        self.parent_task.explain(lvl + 1)
        self.right_side_task.explain(lvl + 1)


@dataclass(kw_only=True)
class AggregateTask(ConsumerTask):
    group_by_column: Col
    agg_columns: list[AggCol]
    per_column_aggregator: list[dict[Any, Any]] = field(default_factory=list, repr=False)
    before_shuffle: bool = True

    @trace("AggregateTask")
    def execute(self, chunk: Columns | None, *, is_last: bool) -> tuple[Columns | None, bool]:
        if is_last and chunk is None:
            all_keys = list({key for col_aggregator in self.per_column_aggregator for key in col_aggregator})
            columns = (
                all_keys,
                *[[col_aggregator.get(key, 0) for key in all_keys] for col_aggregator in self.per_column_aggregator],
            )
            return columns, True
        assert chunk is not None
        assert self.parent_task.inferred_schema is not None
        if not self.per_column_aggregator:
            self.per_column_aggregator = [{} for _ in self.agg_columns]

        if self.before_shuffle:
            group_column = project_column(self.group_by_column, chunk, self.parent_task.inferred_schema)
            agg_expr_columns = tuple(
                project_column(agg_col, chunk, self.parent_task.inferred_schema) for agg_col in self.agg_columns
            )
            self.fill_aggregators(group_column, agg_expr_columns)
        else:
            # after shuffle we don't need to project, just merge previous results
            self.fill_aggregators(chunk[0], chunk[1:])
        return None, False

    def fill_aggregators(self, group_column: list[ColumnTypePython], agg_expr_columns: Columns) -> None:
        for counter, agg_col, agg_expr_col in zip(
            self.per_column_aggregator, self.agg_columns, agg_expr_columns, strict=True
        ):
            if agg_col.type == "sum":
                for group, expr in zip(group_column, agg_expr_col, strict=True):
                    assert type(expr) in NumericColumnTypes
                    counter[group] = counter.get(group, 0) + expr
            if agg_col.type == "min":
                for group, expr in zip(group_column, agg_expr_col, strict=True):
                    assert type(expr) in NumericColumnTypes
                    counter[group] = min(counter.get(group, MAX_INT), expr)
            if agg_col.type == "max":
                for group, expr in zip(group_column, agg_expr_col, strict=True):
                    assert type(expr) in NumericColumnTypes
                    counter[group] = max(counter.get(group, MIN_INT), expr)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        if not self.before_shuffle:
            # we already validated the schema before, after shuffle the schema should be the same
            return schema
        # TODO(david): check if agg_column type is compatible with inferred type (sum over strings not allowed)
        referenced_column_names = {
            col.name
            for column in [*self.agg_columns, self.group_by_column]
            for col in column.all_nested_columns
            if type(col) is Col
        }
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in aggregation: {unknown_cols}")
        return [
            (self.group_by_column.name, self.group_by_column.infer_type(schema)),
            *[(agg_col.name, agg_col.infer_type(schema)) for agg_col in self.agg_columns],
        ]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} AggregateTask(group_by: {self.group_by_column}, agg: {self.agg_columns}, "
            f"before_shuffle:{self.before_shuffle}):"
            f"{nice_schema(self.inferred_schema)}"
        )
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
            shuffle_files.append(OutputFile(shuffle_file, partition))
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
        return [OutputFile(output_file)]

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
