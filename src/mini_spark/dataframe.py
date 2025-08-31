from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Self

import rpyc
from rpyc import Connection, OneShotServer, Service

from .query import TaskExecutor
from .sql import BinaryOperatorColumn, Col
from .tasks import (
    CountTask,
    FilterTask,
    Job,
    JoinTask,
    JoinType,
    LoadShuffleFileTask,
    LoadTableTask,
    ProjectTask,
    ShuffleToFileTask,
    Task,
    VoidTask,
)
from .utils import chunk_list

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


@dataclass
class JobResults:
    pass


class Executor(Service):  # type:ignore[misc]
    def __init__(self, port: int) -> None:
        self.id = port
        server = OneShotServer(self, port=port)
        server.start()  # blocks until driver disconnected

    def on_connect(self, conn: Connection) -> None:
        print("executor: driver connected", conn)  # noqa: T201

    def on_disconnect(self, conn: Connection) -> None:
        print("executor: driver disconnected", conn)  # noqa: T201

    def exposed_get_file_content(self, file_name: str) -> bytes:
        executor_file = Path("executors") / str(self.id) / file_name
        with executor_file.open("rb") as f:
            return f.read()

    def exposed_execute_jobs(self, stage: Task, jobs: list[Job]) -> JobResults:
        print("executing job on executor", stage, jobs)  # noqa: T201
        # TODO(david): process all jobs paralelly -> thread pool, give sysargs with job info
        return JobResults()

    def execute_jobs(self, stage: Task, jobs: list[Job]) -> JobResults:
        raise NotImplementedError


class Driver:
    def __init__(self) -> None:
        self.executors: list[Executor] = []

    def add_executor(self, host: str, port: int) -> None:
        connection = rpyc.connect(host, port)
        self.executors.append(connection.root)

    def execute_jobs_on_nodes(self, stage: Task, jobs: list[Job]) -> list[JobResults]:
        # TODO(david): this should be execute_task(full_task)
        # fulltask analyze
        # fulltask optimize
        # compile into zig binary
        # send binary to executors
        # wait
        # split task into stages
        # for each stage:
        #   generate jobs
        #   distribute jobs to executors
        #   collect job results for next stages
        # collect results from last stage (via jobresults)
        # collect traces
        async_calls = []
        for executor, job_chunk in zip(self.executors, chunk_list(jobs, len(self.executors)), strict=True):
            remote_execute = rpyc.async_(executor.exposed_execute_jobs)
            async_calls.append(remote_execute(stage, job_chunk))
        return [async_call.value for async_call in async_calls]  # block until all nodes are finished


class MiniSpark:
    @staticmethod
    def create(executors: int = 1, workers_per_executor: int = 1) -> None:
        # create drivers and workers
        pass


class DataFrame:
    def __init__(self) -> None:
        self.task: Task = VoidTask()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableTask(self.task, file_path=Path(file_path))
        return self

    def collect(self) -> list[Row]:
        return list(TaskExecutor(self.task).execute())

    def show(self, n: int = 10) -> None:
        first = True
        for row in TaskExecutor(self.task).execute(limit=n):
            if first:
                print("|" + ("|".join(f"{col:<10}" for col in row)) + "|")  # noqa: T201
                print("|" + ("+".join("-" * 10 for _ in row)) + "|")  # noqa: T201
                first = False
            print("|" + ("|".join(f"{v:<10}" for v in row.values())) + "|")  # noqa: T201

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
