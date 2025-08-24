from pathlib import Path
from typing import Self
from .sql import Col, Lit
from .io import Schema
from .tasks import (
    Task,
    VoidTask,
    LoadTableTask,
    ProjectTask,
    FilterTask,
    GroupByTask,
    ShuffleToFileTask,
    LoadShuffleFileTask,
    CountTask,
)
from .query import Executor, Analyzer
from .constants import Row


class GroupedData:
    def __init__(self, df: "DataFrame", column: Col):
        self.df = df
        self.df.task = GroupByTask(self.df.task, column)
        self.group_column = column

    def count(self) -> "DataFrame":
        self.df.task = ShuffleToFileTask(self.df.task)
        self.df.task = LoadShuffleFileTask(self.df.task)
        self.df.task = CountTask(self.df.task, self.group_column)
        return self.df


class DataFrame:
    def __init__(self) -> None:
        self.task: Task = VoidTask()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableTask(self.task, Path(file_path))
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
        self.task = ProjectTask(self.task, list(columns))
        return self

    def filter(self, column: Col) -> Self:
        self.task = FilterTask(self.task, column)
        return self

    def group_by(self, column: Col) -> GroupedData:
        return GroupedData(self, column)


if __name__ == "__main__":
    df = DataFrame().table("data.bin")
    df.show()
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
        # .filter(Col("final_a") == -1)
        .group_by(Col("final_d"))
        .count()
    )
    df.show(n=-1)
