from pathlib import Path
from typing import Self
from .sql import BinaryOperatorColumn, Col
from .tasks import (
    Task,
    VoidTask,
    LoadTableTask,
    ProjectTask,
    FilterTask,
    ShuffleToFileTask,
    LoadShuffleFileTask,
    JoinType,
    JoinTask,
    CountTask,
)
from .query import Executor
from .constants import Row, Schema


class GroupedData:
    def __init__(self, df: "DataFrame", column: Col):
        self.df = df
        self.group_column = column

    def count(self) -> "DataFrame":
        self.df.task = ShuffleToFileTask(self.df.task, key_column=self.group_column)
        self.df.task = LoadShuffleFileTask(self.df.task)
        self.df.task = CountTask(self.df.task, group_by_column=self.group_column)
        return self.df


class DataFrame:
    def __init__(self) -> None:
        self.task: Task = VoidTask()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableTask(self.task, file_path=Path(file_path))
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
        return self.task.validate_schema()

    def select(self, *columns: Col) -> Self:
        self.task = ProjectTask(self.task, columns=list(columns))
        return self

    def filter(self, column: Col) -> Self:
        self.task = FilterTask(self.task, condition=column)
        return self

    def group_by(self, column: Col) -> GroupedData:
        return GroupedData(self, column)

    def join(self, other_df: Self, on: Col, how: JoinType):
        assert type(on) is BinaryOperatorColumn
        # TODO: extract left,right side correctly (maybe in analyze)
        self.task = ShuffleToFileTask(self.task)
        other_df.task = ShuffleToFileTask(other_df.task)
        self.task = JoinTask(
            self.task,
            right_side_task=other_df.task,
            join_condition=on,
            how=how,
        )
        return self
