from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, Iterable
from collections import Counter
from collections import defaultdict
from .io import (
    ColumnType,
    deserialize_block_starts,
    deserialize_schema,
    deserialize_block,
    Schema,
    append_rows,
)
from .sql import Col, BinaryOperatorColumn
from .constants import Row


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

    @abstractmethod
    def execute(self, job: Job) -> Iterable[Row]: ...

    @abstractmethod
    def explain(self, lvl: int = 0) -> None: ...

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    def create_jobs(
        self, full_task: "Task", worker_count: int, stage_count: int
    ) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count, stage_count)


@dataclass
class ProjectTask(Task):
    columns: list[Col]

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            yield {col.name: col.execute(row) for col in self.columns}

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
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
        return [(col.name, col.type) for col in self.columns]

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Project({', '.join(str(col) for col in self.columns)})")
        self.parent_task.explain(lvl + 1)


@dataclass
class LoadTableTask(Task):
    file_path: Path

    def execute(self, job: Job) -> Iterable[Row]:
        block_start = self.block_starts[job.block_id]
        with self.file_path.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, self.schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, self.schema)}

    @cached_property
    def schema(self) -> Schema:
        with self.file_path.open(mode="rb") as f:
            return list(deserialize_schema(f))

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file_path.open(mode="rb") as f:
            return deserialize_block_starts(f)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        return self.schema

    def create_jobs(
        self, full_task: Task, worker_count: int, stage_count: int
    ) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count, stage_count)
        for block_id in range(len(self.block_starts)):
            yield Job(full_task, block_id)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} LoadTable({self.file_path})")
        self.parent_task.explain(lvl + 1)


@dataclass
class LoadShuffleFileTask(Task):
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
        with shuffle_file.open(mode="rb") as f:
            return list(deserialize_schema(f))

    def create_jobs(
        self, full_task: Task, worker_count: int, stage_count: int
    ) -> Iterable[Job]:
        for worker_from in range(worker_count):
            for worker_to in range(worker_count):
                shuffle_file = Path(
                    f"shuffle/{stage_count - 1}_{worker_from}_{worker_to}.bin"
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
        print(f"{indent} LoadShuffleFile()")
        self.parent_task.explain(lvl + 1)


@dataclass
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
            raise Exception(f"Unknown columns in filter: {unknown_cols}")
        return schema

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Filter({self.column})")
        self.parent_task.explain(lvl + 1)


@dataclass
class GroupByTask(Task):
    column: Col

    def execute(self, job: Job) -> Iterable[Row]:
        result: dict[str, list[Row]] = defaultdict(list)
        for row in self.parent_task.execute(job):
            result[self.column.execute(row)].append(row)
        for key, rows in result.items():
            yield Row(key=key, key_col_name=self.column.name, rows=rows)

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
            raise Exception(f"Unknown columns in GroupBy: {unknown_cols}")
        return schema

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} GroupBy({self.column})")
        self.parent_task.explain(lvl + 1)


@dataclass
class CountTask(Task):
    group_by_column: Col
    counter: dict[Any, int] = field(default_factory=lambda: Counter())

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            self.counter[row[self.group_by_column.name]] += 1
        # print(self.counter, job)
        return [
            {self.group_by_column.name: key, "count": count}
            for key, count in self.counter.items()
        ]

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert self.group_by_column.name in {col_name for col_name, _ in schema}
        return [
            (self.group_by_column.name, ColumnType.UNKNOWN),
            ("count", ColumnType.INTEGER),
        ]

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Count()")
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
                f"shuffle/{job.current_stage}_{job.worker_id}_{shuffle_bucket}.bin"
            )
            print("writing", shuffle_file)
            append_rows(rows, shuffle_file)
        return []

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} ShuffleToFile()")
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

    def create_jobs(
        self, full_task: "Task", worker_count: int, stage_count: int
    ) -> Iterable[Job]:
        return []
