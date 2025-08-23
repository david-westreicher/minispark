from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Iterable, Self
from multiprocessing import Pool
from typing import Protocol
from .io import (
    deserialize_block_starts,
    deserialize_schema,
    deserialize_block,
    Schema,
)
from .sql import Col, BinaryOperatorColumn

Row = dict[str, Any]


@dataclass
class Job:
    task: "Task"
    block_id: int

    def execute(self) -> Iterable[Row]:
        yield from self.task.execute(self.block_id)


class Task(Protocol):
    def execute(self, block_id: int) -> Iterable[Row]: ...
    @property
    def all_columns(self) -> Iterable[Col]: ...
    def validate_schema(self) -> Schema: ...
    def create_jobs(self, full_task: Self) -> Iterable[Job]: ...


@dataclass
class ProjectTask(Task):
    columns: list[Col]
    parent_task: Task

    def execute(self, block_id: int) -> Iterable[Row]:
        for row in self.parent_task.execute(block_id):
            yield {col.name: col.execute(row) for col in self.columns}

    @property
    def all_columns(self) -> Iterable[Col]:
        yield from self.columns

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

    def create_jobs(self, full_task: Task) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task)


@dataclass
class LoadTask(Task):
    file_path: Path
    parent_task: Task

    def execute(self, block_id: int) -> Iterable[Row]:
        block_start = self.block_starts[block_id]
        with self.file_path.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, self.schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, self.schema)}

    @property
    def all_columns(self) -> Iterable[Col]:
        yield from []

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

    def create_jobs(self, full_task: Task) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task)
        for block_id in range(len(self.block_starts)):
            yield Job(full_task, block_id)


@dataclass
class FilterTask(Task):
    column: Col
    parent_task: Task

    def __post_init__(self):
        assert type(self.column) is BinaryOperatorColumn

    def execute(self, block_id: int) -> Iterable[Row]:
        for row in self.parent_task.execute(block_id):
            if self.column.execute(row):
                yield row

    @property
    def all_columns(self) -> Iterable[Col]:
        yield self.column

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

    def create_jobs(self, full_task: Task) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task)


@dataclass
class VoidTask(Task):
    parent_task: Task | None = None

    def execute(self, block_id: int) -> Iterable[Row]:
        raise NotImplementedError

    @property
    def all_columns(self) -> Iterable[Col]:
        yield from []

    def validate_schema(self) -> Schema:
        return []

    def create_jobs(self, full_task: Task) -> Iterable[Job]:
        yield from []


def ex(job: Job):
    return list(job.execute())


class Executor:
    def __init__(self, task: Task):
        self.task = task
        self.worker_pool = Pool(processes=10)

    def execute(self) -> Iterable[Row]:
        Analyzer(self.task).analyze()
        jobs = list(self.task.create_jobs(self.task))
        print(jobs)
        for job_result in self.worker_pool.imap_unordered(ex, jobs):
            yield from job_result


class Analyzer:
    def __init__(self, task: Task):
        self.task = task

    def analyze(self) -> Schema:
        return self.task.validate_schema()


class DataFrame:
    def __init__(self):
        self.task = VoidTask()

    def table(self, file_path: str) -> Self:
        self.task = LoadTask(Path(file_path), self.task)
        return self

    def collect(self) -> list[Row]:
        return list(Executor(self.task).execute())

    def show(self, n=20) -> None:
        first = True
        for row in Executor(self.task).execute():
            if first:
                print("\t".join(row.keys()))
                first = False
            print("\t".join(str(v) for v in row.values()))
            n -= 1
            if n == 0:
                break

    @property
    def schema(self) -> Schema:
        return Analyzer(self.task).analyze()

    def select(self, *columns: Col) -> Self:
        if columns == ():
            columns = tuple(Col(col_name) for col_name, _ in self.schema)
        self.task = ProjectTask(list(columns), self.task)
        return self

    def filter(self, column: Col) -> Self:
        self.task = FilterTask(column, self.task)
        return self


if __name__ == "__main__":
    df = DataFrame().table("data.bin")
    df = (
        df.select(
            (Col("col_a") * -1).alias("col_a_minus"),
            (Col("col_b") * 1).alias("col_b_plus"),
            Col("col_c").alias("col_d"),
        )
        .filter(Col("col_a_minus") <= Col("col_b_plus"))
        .select(
            Col("col_a_minus").alias("final_a"),
            Col("col_b_plus").alias("final_b"),
            Col("col_d").alias("final_d"),
        )
        .filter(Col("final_a") == -1)
    )
    df.show(n=-1)
