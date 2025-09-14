from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Self

from tabulate import tabulate

from .execution import ExecutionEngine, PythonExecutionEngine
from .tasks import (
    AggregateTask,
    BroadcastHashJoinTask,
    FilterTask,
    JoinType,
    LoadTableBlockTask,
    ProjectTask,
    Task,
    VoidTask,
)

if TYPE_CHECKING:
    from .constants import Row, Schema
    from .sql import AggCol, Col


class GroupedData:
    def __init__(self, df: DataFrame, column: Col) -> None:
        self.df = df
        self.group_column = column

    def agg(self, *agg_columns: AggCol) -> DataFrame:
        self.df.task = AggregateTask(self.df.task, group_by_column=self.group_column, agg_columns=list(agg_columns))
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
        self.task = BroadcastHashJoinTask(self.task, right_side_task=other_df.task, join_condition=on, how=how)
        return self

    def collect(self) -> list[Row]:
        job_results = self.engine.execute_full_task(self.task)
        return list(self.engine.collect_results(job_results))

    def show(self, n: int = 10) -> int:
        results = self.engine.execute_full_task(self.task)
        rows = list(self.engine.collect_results(results, limit=n))
        print(tabulate(rows, tablefmt="rounded_outline", headers="keys"))  # noqa: T201
        return len(rows)
