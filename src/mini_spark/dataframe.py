from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Self

from .execution import ExecutionEngine, PythonExecutionEngine
from .sql import BinaryOperatorColumn, Col
from .tasks import (
    CountTask,
    FilterTask,
    JoinTask,
    JoinType,
    LoadShuffleFileTask,
    LoadTableTask,
    ProjectTask,
    ShuffleToFileTask,
    Task,
    VoidTask,
)

if TYPE_CHECKING:
    from .constants import Row, Schema


class GroupedData:
    def __init__(self, df: DataFrame, column: Col) -> None:
        self.df = df
        self.group_column = column

    def count(self) -> DataFrame:
        self.df.task = ShuffleToFileTask(self.df.task, key_column=self.group_column)
        self.df.task = LoadShuffleFileTask(self.df.task)
        self.df.task = CountTask(self.df.task, group_by_column=self.group_column)
        return self.df


class DataFrame:
    def __init__(self, engine: ExecutionEngine | None = None) -> None:
        self.engine = engine if engine is not None else PythonExecutionEngine()
        self.task: Task = VoidTask()

    @property
    def schema(self) -> Schema:
        return self.task.validate_schema()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableTask(self.task, file_path=Path(file_path))
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
        assert type(on) is BinaryOperatorColumn
        # TODO(david): extract left,right side correctly (maybe in analyze)
        self.task = ShuffleToFileTask(self.task)
        other_df.task = ShuffleToFileTask(other_df.task)
        self.task = JoinTask(
            self.task,
            right_side_task=other_df.task,
            join_condition=on,
            how=how,
        )
        return self

    def collect(self) -> list[Row]:
        job_results = self.engine.execute_full_task(self.task)
        return list(self.engine.collect_results(job_results))

    def show(self, n: int = 10) -> None:
        results = self.engine.execute_full_task(self.task)
        first = True
        for row in self.engine.collect_results(results, limit=n):
            if first:
                print("|" + ("|".join(f"{col:<10}" for col in row)) + "|")  # noqa: T201
                print("|" + ("+".join("-" * 10 for _ in row)) + "|")  # noqa: T201
                first = False
            print("|" + ("|".join(f"{v:<10}" for v in row.values())) + "|")  # noqa: T201
