from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Self

from .execution import ExecutionEngine, PythonExecutionEngine
from .tasks import (
    AggregateCountTask,
    FilterTask,
    JoinTask,
    JoinType,
    LoadTableBlockTask,
    ProjectTask,
    Task,
    VoidTask,
)

if TYPE_CHECKING:
    from .constants import Row, Schema
    from .sql import Col


class GroupedData:
    def __init__(self, df: DataFrame, column: Col) -> None:
        self.df = df
        self.group_column = column

    def count(self) -> DataFrame:
        self.df.task = AggregateCountTask(self.df.task, group_by_column=self.group_column)
        return self.df


class DataFrame:
    def __init__(self, engine: ExecutionEngine | None = None) -> None:
        self.engine = engine if engine is not None else PythonExecutionEngine()
        self.task: Task = VoidTask()

    @property
    def schema(self) -> Schema:
        return self.task.validate_schema()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableBlockTask(self.task, file_path=Path(file_path))
        return self

    def select(self, *columns: Col) -> Self:
        self.task = ProjectTask(self.task, columns=list(columns))
        return self

    def filter(self, column: Col) -> Self:
        self.task = FilterTask(self.task, condition=column)
        return self

    def group_by(self, column: Col) -> GroupedData:
        return GroupedData(self, column)

    def join(self, other_df: Self, on: Col, how: JoinType) -> DataFrame:
        self.task = JoinTask(self.task, right_side_task=other_df.task, join_condition=on, how=how)
        return self

    def collect(self) -> list[Row]:
        job_results = self.engine.execute_full_task(self.task)
        result = list(self.engine.collect_results(job_results))
        self.engine.cleanup()
        return result

    def show(self, n: int = 10) -> None:
        results = self.engine.execute_full_task(self.task)
        first = True
        for row in self.engine.collect_results(results, limit=n):
            if first:
                print("|" + ("|".join(f"{col:<10}" for col in row)) + "|")  # noqa: T201
                print("|" + ("+".join("-" * 10 for _ in row)) + "|")  # noqa: T201
                first = False
            print("|" + ("|".join(f"{v:<10}" for v in row.values())) + "|")  # noqa: T201
        self.engine.cleanup()
