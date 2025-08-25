from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, Iterable, Literal
from collections import Counter, defaultdict
from .io import (
    deserialize_block_starts,
    deserialize_block,
    append_rows,
    BlockFile,
)
from .utils import create_temp_file, nice_schema
from .sql import Col, BinaryOperatorColumn
from .constants import Row, SHUFFLE_FOLDER, Schema, ColumnType
from .algorithms import SORT_BLOCK_SIZE, external_sort, external_merge_join

JoinType = Literal["inner", "left", "right", "outer"]


@dataclass
class Job:
    task: "Task"
    block_id: int
    worker_count: int = -1
    worker_id: int = -1
    current_stage: int = -1
    shuffle_file: Path | None = None

    def execute(self) -> Iterable[Row]:
        yield from self.task.execute(self)


@dataclass
class Task(ABC):
    parent_task: "Task"
    inferred_schema: Schema | None = None

    @abstractmethod
    def execute(self, job: Job) -> Iterable[Row]: ...

    @abstractmethod
    def explain(self, lvl: int = 0) -> None: ...

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    def create_jobs(self, full_task: "Task", worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)


@dataclass(kw_only=True)
class ProjectTask(Task):
    columns: list[Col]

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            yield {col.name: col.execute(row) for col in self.columns}

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        # expand * to all columns from previous schema
        self.columns = [
            sub
            for col in self.columns
            for sub in (
                [Col(name) for name, _ in schema]
                if type(col) is Col and col.name == "*"
                else [col]
            )
        ]

        referenced_column_names = {
            col.name
            for column in self.columns
            for col in column.all_nested_columns
            if type(col) is Col
        }
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in projection: {unknown_cols}")
        return [(col.name, col.infer_type(schema)) for col in self.columns]

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(
            f"{indent} Project({', '.join(str(col) for col in self.columns)}):{nice_schema(self.inferred_schema)}"
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadTableTask(Task):
    file_path: Path

    def execute(self, job: Job) -> Iterable[Row]:
        block_start = self.block_starts[job.block_id]
        with self.file_path.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, self.file_schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, self.file_schema)}

    @cached_property
    def file_schema(self) -> Schema:
        return BlockFile(self.file_path).file_schema

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file_path.open(mode="rb") as f:
            return deserialize_block_starts(f)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        return self.file_schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)
        for block_id in range(len(self.block_starts)):
            yield Job(full_task, block_id)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(
            f"{indent} LoadTable({self.file_path}):{nice_schema(self.inferred_schema)}"
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadShuffleFileTask(Task):
    stage_to_load: int = -1

    def execute(self, job: Job) -> Iterable[Row]:
        assert job.shuffle_file is not None
        block_start = self.block_starts(job.shuffle_file)[job.block_id]
        schema = self.load_schema(job.shuffle_file)
        with job.shuffle_file.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, schema)}

    def load_schema(self, shuffle_file: Path) -> Schema:
        return BlockFile(shuffle_file).file_schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        assert self.stage_to_load != -1
        for worker_from in range(worker_count):
            for worker_to in range(worker_count):
                shuffle_file = (
                    SHUFFLE_FOLDER
                    / f"{self.stage_to_load}_{worker_from}_{worker_to}.bin"
                )
                if not shuffle_file.exists():
                    continue
                for block_id in range(len(self.block_starts(shuffle_file))):
                    yield Job(
                        full_task,
                        block_id,
                        worker_id=worker_to,
                        shuffle_file=shuffle_file,
                    )
        yield from []

    def block_starts(self, shuffle_file: Path) -> list[int]:
        with shuffle_file.open(mode="rb") as f:
            return deserialize_block_starts(f)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(
            f"{indent} LoadShuffleFile(stage_to_load={self.stage_to_load}):{nice_schema(self.inferred_schema)}"
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class FilterTask(Task):
    column: Col

    def __post_init__(self):
        assert type(self.column) is BinaryOperatorColumn

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            if self.column.execute(row):
                yield row

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = [
            col.name for col in self.column.all_nested_columns if type(col) is Col
        ]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Filter: {unknown_cols}")
        return schema

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Filter({self.column}):{nice_schema(self.inferred_schema)}")
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class GroupByTask(Task):
    column: Col | None = None

    def execute(self, job: Job) -> Iterable[Row]:
        assert self.column is not None
        result: dict[str, list[Row]] = defaultdict(list)
        for row in self.parent_task.execute(job):
            result[self.column.execute(row)].append(row)
        for key, rows in result.items():
            yield Row(key=key, rows=rows)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        if self.column is None:
            return schema
        referenced_column_names = [
            col.name for col in self.column.all_nested_columns if type(col) is Col
        ]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in GroupBy: {unknown_cols}")
        return [
            (self.column.name, self.column.infer_type(schema)),
        ]

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} GroupBy({self.column}):{nice_schema(self.inferred_schema)}")
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class JoinTask(Task):
    right_side_task: Task
    join_condition: Col
    how: JoinType = "inner"
    left_shuffle_stage: int = -1
    right_shuffle_stage: int = -1
    left_key: Col | None = None
    right_key: Col | None = None

    def execute(self, job: Job) -> Iterable[Row]:
        assert self.left_key
        assert self.right_key
        left_sorted_file = self.sort_shuffle_files(
            self.left_shuffle_stage, self.left_key, job
        )
        right_sorted_file = self.sort_shuffle_files(
            self.right_shuffle_stage, self.right_key, job
        )
        if left_sorted_file and right_sorted_file:
            # TODO: does not hold for all join types
            yield from external_merge_join(
                left_sorted_file,
                right_sorted_file,
                self.left_key.execute,
                self.right_key.execute,
                self.how,
            )
        if left_sorted_file is not None:
            left_sorted_file.unlink(missing_ok=True)
        if right_sorted_file is not None:
            right_sorted_file.unlink(missing_ok=True)

    def sort_shuffle_files(self, stage: int, key: Col, job: Job) -> Path | None:
        shuffle_files = list(
            self.collect_shuffle_files(stage, job.worker_id, job.worker_count)
        )
        if shuffle_files == []:
            return None
        shuffle_file = (
            BlockFile(create_temp_file(), SORT_BLOCK_SIZE)
            .merge_files(shuffle_files)
            .file
        )
        output_file = create_temp_file()
        tmp_file = create_temp_file()
        external_sort(shuffle_file, key.execute, output_file, tmp_file)
        shuffle_file.unlink(missing_ok=True)
        tmp_file.unlink(missing_ok=True)
        return output_file

    def collect_shuffle_files(
        self, stage: int, worker_id: int, worker_count: int
    ) -> Iterable[Path]:
        for worker_from in range(worker_count):
            shuffle_file = SHUFFLE_FOLDER / f"{stage}_{worker_from}_{worker_id}.bin"
            if not shuffle_file.exists():
                continue
            yield shuffle_file

    def validate_schema(self) -> Schema:
        left_schema = self.parent_task.validate_schema()
        right_schema = self.right_side_task.validate_schema()
        referenced_column_names = [
            col.name
            for col in self.join_condition.all_nested_columns
            if type(col) is Col
        ]
        schema_cols = {col_name for col_name, _ in left_schema + right_schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in Join: {unknown_cols}")
        assert type(self.join_condition) is BinaryOperatorColumn
        self.left_key = self.join_condition.left_side
        self.right_key = self.join_condition.right_side
        left_grandparent = self.parent_task.parent_task
        assert type(left_grandparent) is GroupByTask
        left_grandparent.column = self.left_key
        right_grandparent = self.right_side_task.parent_task
        assert type(right_grandparent) is GroupByTask
        right_grandparent.column = self.right_key
        return left_schema + right_schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)
        for i in range(worker_count):
            yield Job(full_task, block_id=-1, worker_id=i, worker_count=worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(
            f'{indent} Join({self.join_condition}, "{self.how}", left_shuffle: {self.left_shuffle_stage}, right_shuffle: {self.right_shuffle_stage}):{nice_schema(self.inferred_schema)}'
        )
        self.parent_task.explain(lvl + 1)
        self.right_side_task.explain(lvl + 1)


@dataclass(kw_only=True)
class CountTask(Task):
    group_by_column: Col
    counter: dict[Any, int] = field(default_factory=lambda: Counter())

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            self.counter[self.group_by_column.execute(row)] += 1
        return [
            {self.group_by_column.name: key, "count": count}
            for key, count in self.counter.items()
        ]

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert self.group_by_column.name in {col_name for col_name, _ in schema}
        return [
            (self.group_by_column.name, self.group_by_column.infer_type(schema)),
            ("count", ColumnType.INTEGER),
        ]

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Count():{nice_schema(self.inferred_schema)}")
        self.parent_task.explain(lvl + 1)


@dataclass
class ShuffleToFileTask(Task):
    def execute(self, job: Job) -> Iterable[Row]:
        shuffle_buckets: dict[int, list[Row]] = defaultdict(list)
        for row in self.parent_task.execute(job):
            destination_shuffle = hash(row["key"]) % job.worker_count
            shuffle_buckets[destination_shuffle].extend(row["rows"])
        for shuffle_bucket, rows in shuffle_buckets.items():
            shuffle_file = Path(
                SHUFFLE_FOLDER
                / f"{job.current_stage}_{job.worker_id}_{shuffle_bucket}.bin"
            )
            print("writing", shuffle_file.absolute())
            append_rows(rows, shuffle_file)
        return []

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} ShuffleToFile():{nice_schema(self.inferred_schema)}")
        self.parent_task.explain(lvl + 1)


@dataclass
class VoidTask(Task):
    parent_task: Task = None  # type: ignore

    def execute(self, job: Job) -> Iterable[Row]:
        raise NotImplementedError()

    def explain(self, lvl: int = 0):
        pass

    def validate_schema(self) -> Schema:
        return []

    def create_jobs(self, full_task: "Task", worker_count: int) -> Iterable[Job]:
        return []
