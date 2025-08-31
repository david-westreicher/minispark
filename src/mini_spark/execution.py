from __future__ import annotations

import math
import multiprocessing
import shutil
import subprocess
import threading
from collections.abc import Iterable
from contextlib import AbstractContextManager
from copy import deepcopy
from dataclasses import dataclass, field
from multiprocessing import Pool
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Self
from uuid import uuid4

import rpyc
from rpyc import Connection, OneShotServer, Service

from mini_spark.zig_bridge import compile_stages

from .constants import GLOBAL_TEMP_FOLDER
from .io import BlockFile
from .tasks import (
    Job,
    JoinTask,
    LoadShuffleFileTask,
    LoadTableBlockJob,
    ShuffleToFileTask,
    Task,
    VoidTask,
    WriteToLocalFileTask,
)
from .utils import (
    TRACER,
    chunk_list,
    trace,
    trace_yield,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import TracebackType

    from .constants import Row


@dataclass
class JobResults:
    worker_id: str = ""
    result_files: list[Path] = field(default_factory=list)


@dataclass
class DistributedJob:
    stage_id: int
    input_file: Path
    input_block_id: int
    output_file: Path = Path()
    executor_binary: Path = Path()
    id: str = ""

    def to_tuple(self) -> tuple[int, str, int, str]:
        return (self.stage_id, str(self.input_file), self.input_block_id, str(self.output_file))

    @staticmethod
    def from_tuple(t: DistributedJobTuple) -> DistributedJob:
        return DistributedJob(stage_id=t[0], input_file=Path(t[1]), input_block_id=t[2], output_file=Path(t[3]))


DistributedJobTuple = tuple[int, str, int, str]


class ExecutionEngine(Protocol):
    def execute_full_task(self, full_task: Task) -> list[JobResults]: ...
    def collect_results(self, results: list[JobResults], limit: int = -1) -> Iterable[Row]: ...
    def cleanup(self) -> None: ...


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


def execute_job(job: DistributedJob) -> JobResults:
    print("executor: executing job", job)  # noqa: T201
    worker_id = multiprocessing.current_process().name
    trace_file = (job.executor_binary.parent / f"trace-{job.id}").absolute()
    TRACER.add_trace_file(trace_file, worker_id)
    subprocess.call(  # noqa: S603
        [
            str(job.executor_binary),
            str(job.stage_id),
            str(job.input_file.absolute()),
            str(job.input_block_id),
            str(job.output_file.absolute()),
            str(trace_file),
        ]
    )
    return JobResults(result_files=[job.output_file])


class Executor(Service):  # type:ignore[misc]
    def __init__(self, port: int, *, block: bool = True) -> None:
        self.id = str(port)
        self.executor_path = Path("executors") / self.id
        self.executor_path.mkdir(parents=True, exist_ok=False)
        server = OneShotServer(self, port=port)
        if block:
            server.start()
        else:
            self.thread = threading.Thread(target=server.start, daemon=True)
            self.thread.start()
        self.worker_pool = Pool()

    def on_connect(self, conn: Connection) -> None:
        print("executor: driver connected", conn)  # noqa: T201

    def on_disconnect(self, conn: Connection) -> None:
        print("executor: driver disconnected", conn)  # noqa: T201
        self.worker_pool.close()
        shutil.rmtree(self.executor_path, ignore_errors=False)
        print("executor: files removed")  # noqa: T201
        self.thread.join()

    def exposed_get_file_content(self, file_name: str) -> bytes:
        executor_file = Path("executors") / str(self.id) / file_name
        with executor_file.open("rb") as f:
            return f.read()

    def exposed_execute_jobs(self, jobs: list[DistributedJobTuple]) -> tuple[str, list[str]]:
        parsed_jobs = [DistributedJob.from_tuple(job) for job in jobs]
        for job in parsed_jobs:
            job.output_file = self.executor_path / f"stage_{job.stage_id}_block_{job.input_block_id}.bin"
            job.executor_binary = self.executor_path / "executor_binary"
            job.id = str(uuid4())
        worker_results = self.worker_pool.map(execute_job, parsed_jobs)
        return (
            self.id,
            [str(result.absolute()) for results in worker_results for result in results.result_files],
        )

    def exposed_recieve_binary(self, binary: bytes) -> None:
        print("executor: received binary")  # noqa: T201
        binary_destination = self.executor_path / "executor_binary"
        with binary_destination.open("wb") as f:
            f.write(binary)
        binary_destination.chmod(0o755)

    def recieve_binary(self, binary: bytes) -> None:
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

    def execute_stage_on_nodes(self, jobs: list[Job]) -> list[JobResults]:
        async_calls = []
        distributed_jobs = []
        for job in jobs:
            assert type(job) is LoadTableBlockJob
            distributed_jobs.append(
                DistributedJob(
                    stage_id=job.current_stage,
                    input_file=job.table_file.absolute(),
                    input_block_id=job.block_id,
                ).to_tuple()
            )
        for executor, job_chunk in zip(self.executors, chunk_list(distributed_jobs, len(self.executors)), strict=True):
            remote_execute = rpyc.async_(executor.exposed_execute_jobs)
            async_calls.append(remote_execute(job_chunk))
        results_waited_for = [async_call.value for async_call in async_calls]
        return [
            JobResults(worker_id=worker_id, result_files=[Path(f) for f in result_files])
            for worker_id, result_files in results_waited_for
        ]

    @trace("distribute binary")
    def distribute_binary(self, binary_file: Path) -> None:
        with binary_file.open("rb") as f:
            for executor in self.executors:
                executor.recieve_binary(f.read())


class DistributedExecutionEngine(AbstractContextManager["DistributedExecutionEngine"], ExecutionEngine):
    def __init__(self) -> None:
        self.driver = Driver()

    def __enter__(self) -> Self:
        self.executor = Executor(port=5000, block=False)
        self.driver.add_executor("localhost", port=5000)
        return self

    def execute_full_task(self, full_task: Task) -> list[JobResults]:
        print("#" * 100)  # noqa: T201
        print("Logical Plan")  # noqa: T201
        full_task.explain()
        full_task = WriteToLocalFileTask(full_task, file_path=Path())
        Analyzer.analyze(full_task)
        stages = list(reversed(list(split_into_stages(full_task))))
        for stage_num, stage in enumerate(stages):
            Analyzer.optimize(stage, stage_num)
        binary_output_file = compile_stages(stages)
        self.driver.distribute_binary(binary_output_file)
        print(binary_output_file)  # noqa: T201
        TRACER.start("Execution")
        final_result = []
        for stage_num, stage in enumerate(stages):
            print("#" * 100)  # noqa: T201
            print("Stage", stage_num)  # noqa: T201
            TRACER.start(f"Stage {stage_num}")
            stage.explain()
            jobs = list(stage.create_jobs(stage, 1))
            for job in jobs:
                job.current_stage = stage_num
            final_result = self.driver.execute_stage_on_nodes(jobs)
            TRACER.end()
        TRACER.end()
        return final_result

    @trace_yield("collect results")
    def collect_results(self, results: list[JobResults], limit: int = -1) -> Iterable[Row]:
        for job_result in results:
            for file in job_result.result_files:
                for row in BlockFile(file).read_data_rows():
                    yield row
                    limit -= 1
                    if limit <= 0:
                        return

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        self.driver.stop()


class PythonExecutionEngine(ExecutionEngine):
    def __init__(self) -> None:
        self.shuffle_files_to_delete: set[Path] = set()

    @trace("execute full task")
    def execute_full_task(self, full_task: Task) -> list[JobResults]:
        print("#" * 100)  # noqa: T201
        print("Logical Plan")  # noqa: T201
        full_task.explain()
        final_result_file = GLOBAL_TEMP_FOLDER / "result.bin"
        final_result_file.unlink(missing_ok=True)
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
            stage.explain()
            self.execute_stage(stage, i)
            TRACER.end()
        TRACER.end()
        return [JobResults(result_files=[final_result_file])]

    def execute_stage(self, stage: Task, stage_num: int) -> list[JobResults]:
        assert stage.inferred_schema is not None

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

    @trace_yield("collect results")
    def collect_results(self, results: list[JobResults], limit: int = math.inf) -> Iterable[Row]:  # type:ignore[assignment]
        for job_result in results:
            for file in job_result.result_files:
                for row in BlockFile(file).read_data_rows():
                    yield row
                    limit -= 1
                    if limit <= 0:
                        return

    @trace("remove files")
    def cleanup(self) -> None:
        for shuffle_file in self.shuffle_files_to_delete:
            shuffle_file.unlink(missing_ok=True)
        final_result_file = GLOBAL_TEMP_FOLDER / "result.bin"
        final_result_file.unlink(missing_ok=True)
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
