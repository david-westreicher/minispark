from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Iterable, Self
from typing import Protocol
from .io import (
    deserialize_block_starts,
    deserialize_schema,
    deserialize_block,
    Schema,
)
from .sql import Col, BinaryOperatorColumn

Row = dict[str, Any]


class Task(Protocol):
    def execute(self, data: Iterable[Row]) -> Iterable[Row]: ...
    @property
    def all_columns(self) -> Iterable[Col]: ...
    def validate_schema(self, schema: Schema) -> Schema: ...


@dataclass
class ProjectTask(Task):
    columns: list[Col]

    def execute(self, data: Iterable[Row]) -> Iterable[Row]:
        for row in data:
            yield {col.name: col.execute(row) for col in self.columns}

    @property
    def all_columns(self) -> Iterable[Col]:
        yield from self.columns

    def validate_schema(self, schema: Schema) -> Schema:
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


@dataclass
class LoadTask(Task):
    file_path: Path

    def execute(self, data: Iterable[Row]) -> Iterable[Row]:
        with self.file_path.open(mode="rb") as f:
            block_starts = deserialize_block_starts(f)
            for block_start in block_starts:
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

    def validate_schema(self, schema: Schema) -> Schema:
        assert schema == []
        return self.schema


@dataclass
class FilterTask(Task):
    column: Col

    def __post_init__(self):
        assert type(self.column) is BinaryOperatorColumn

    def execute(self, data: Iterable[Row]) -> Iterable[Row]:
        for row in data:
            if self.column.execute(row):
                yield row

    @property
    def all_columns(self) -> Iterable[Col]:
        yield self.column

    def validate_schema(self, schema: Schema) -> Schema:
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


class Executor:
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks

    def execute(self) -> Iterable[Row]:
        Analyzer(self.tasks).analyze()
        result: Iterable[Row] = []
        for task in self.tasks:
            result = task.execute(result)
        yield from result


class Analyzer:
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks

    def analyze(self) -> Schema:
        curr_schema: Schema = []
        for task in self.tasks:
            curr_schema = task.validate_schema(curr_schema)
        return curr_schema


class DataFrame:
    def __init__(self):
        self.tasks = []

    def table(self, file_path: str) -> Self:
        self.tasks.append(LoadTask(Path(file_path)))
        return self

    def collect(self) -> list[Row]:
        return list(Executor(self.tasks).execute())

    def show(self, n=20) -> None:
        first = True
        for row in Executor(self.tasks).execute():
            if first:
                print("\t".join(row.keys()))
                first = False
            print("\t".join(str(v) for v in row.values()))
            n -= 1
            if n <= 0:
                break

    @property
    def schema(self) -> Schema:
        return Analyzer(self.tasks).analyze()

    def select(self, *columns: Col) -> Self:
        self.tasks.append(ProjectTask(list(columns)))
        return self

    def filter(self, column: Col) -> Self:
        self.tasks.append(FilterTask(column))
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
    )
    df.show()
    print(df.schema)
