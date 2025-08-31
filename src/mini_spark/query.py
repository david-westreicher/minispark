from __future__ import annotations

import threading
from collections.abc import Iterable
from contextlib import AbstractContextManager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Self

import rpyc
from rpyc import Connection, OneShotServer, Service

from .constants import GLOBAL_TEMP_FOLDER
from .io import BlockFile
from .tasks import (
    Job,
    JoinTask,
    LoadShuffleFileTask,
    ShuffleToFileTask,
    Task,
    VoidTask,
    WriteToLocalFileTask,
)
from .utils import (
    TRACER,
    chunk_list,
    trace,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import TracebackType

    from .constants import Row


@dataclass
class JobResults:
    result_files: list[Path] = field(default_factory=list)


class ExecutionEngine(Protocol):
    def execute_full_task(self, full_task: Task) -> list[JobResults]: ...
    def collect_results(self, results: list[JobResults]) -> Iterable[Row]: ...


def split_into_stages(root_task: Task) -> Iterable[Task]:
    curr_task = root_task
    while curr_task.parent_task is not None:
        if type(curr_task) is JoinTask:
            old_left, curr_task.parent_task = curr_task.parent_task, VoidTask()
            old_right, curr_task.right_side_task = (
                curr_task.right_side_task,
                VoidTask(),
            )
            yield root_task
            yield from split_into_stages(old_right)
            yield from split_into_stages(old_left)
            return
        if type(curr_task.parent_task) in {ShuffleToFileTask}:
            tmp, curr_task.parent_task = curr_task.parent_task, VoidTask()
            yield deepcopy(root_task)
            curr_task.parent_task = tmp
            yield from split_into_stages(curr_task.parent_task)
            return
        curr_task = curr_task.parent_task
    yield root_task


class Executor(Service):  # type:ignore[misc]
    def __init__(self, port: int, *, block: bool = True) -> None:
        self.id = port
        server = OneShotServer(self, port=port)
        if block:
            server.start()
        else:
            self.thread = threading.Thread(target=server.start, daemon=True)
            self.thread.start()

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
        self.connections: list[Connection] = []

    def add_executor(self, host: str, port: int) -> None:
        connection = rpyc.connect(host, port)
        self.connections.append(connection)
        self.executors.append(connection.root)

    def stop(self) -> None:
        for conn in self.connections:
            conn.close()

    def execute_jobs_on_nodes(self, full_task: Task, jobs: list[Job]) -> list[JobResults]:
        async_calls = []
        for executor, job_chunk in zip(self.executors, chunk_list(jobs, len(self.executors)), strict=True):
            remote_execute = rpyc.async_(executor.exposed_execute_jobs)
            async_calls.append(remote_execute(full_task, job_chunk))
        return [async_call.value for async_call in async_calls]  # block until all nodes are finished


class DistributedExecutionEngine(AbstractContextManager["DistributedExecutionEngine"], ExecutionEngine):
    def __init__(self) -> None:
        self.driver = Driver()

    def __enter__(self) -> Self:
        self.executor = Executor(port=5000, block=False)
        self.driver.add_executor("localhost", port=5000)
        return self

    def execute_full_task(self, full_task: Task) -> list[JobResults]:
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
        return self.driver.execute_jobs_on_nodes(full_task, [Job(full_task) for _ in range(4)])

    def collect_results(self, results: list[JobResults]) -> Iterable[Row]:
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.driver.stop()


class PythonExecutionEngine(ExecutionEngine):
    def __init__(self) -> None:
        self.shuffle_files_to_delete: set[Path] = set()

    def execute_full_task(self, full_task: Task) -> list[JobResults]:
        final_result_file = GLOBAL_TEMP_FOLDER / "result.bin"
        full_task = WriteToLocalFileTask(full_task, file_path=final_result_file)
        Analyzer.analyze(full_task)
        stages = list(reversed(list(split_into_stages(full_task))))
        for stage_num, stage in enumerate(stages):
            Analyzer.optimize(stage, stage_num)
        TRACER.start("Execution")
        for i, stage in enumerate(stages):
            print("#" * 100)  # noqa: T201
            print("Stage", i)  # noqa: T201
            TRACER.start(f"Stage {i}")
            self.execute_stage(stage, i)
            TRACER.end()
        TRACER.end()
        return [JobResults(result_files=[final_result_file])]

    def execute_stage(self, stage: Task, stage_num: int) -> list[JobResults]:
        assert stage.inferred_schema is not None
        stage.explain()

        TRACER.start("Create jobs")
        jobs = list(stage.create_jobs(stage, 1))
        for job in jobs:
            job.current_stage = stage_num
            job.worker_id = 0
            self.shuffle_files_to_delete.update(job.files_to_delete)
        TRACER.end()

        for job in jobs:
            TRACER.start("job")
            job.execute()
            TRACER.end()
        return []

    def collect_results(self, results: list[JobResults]) -> Iterable[Row]:
        for job_result in results:
            for file in job_result.result_files:
                yield from BlockFile(file).read_data_rows()

    def cleanup(self) -> None:
        TRACER.start("remove files")
        for shuffle_file in self.shuffle_files_to_delete:
            shuffle_file.unlink(missing_ok=True)
        self.shuffle_files_to_delete.clear()
        TRACER.end()
        self.shuffle_files_to_delete.clear()


class Analyzer:
    @staticmethod
    @trace("analyze")
    def analyze(task: Task) -> None:
        if type(task) is VoidTask:
            return
        task.inferred_schema = task.validate_schema()
        if type(task) is JoinTask:
            Analyzer.analyze(task.right_side_task)
        Analyzer.analyze(task.parent_task)

    @staticmethod
    @trace("optimize")
    def optimize(task: Task, stage_num: int) -> None:
        if type(task) is LoadShuffleFileTask:
            task.stage_to_load = stage_num - 1
        if type(task) is JoinTask:
            task.left_shuffle_stage = stage_num - 2
            task.right_shuffle_stage = stage_num - 1
        if type(task) is VoidTask:
            return
        Analyzer.optimize(task.parent_task, stage_num)
