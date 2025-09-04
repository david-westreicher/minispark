from __future__ import annotations

import math
import multiprocessing
import shutil
import subprocess
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import TYPE_CHECKING, Self

import rpyc
from rpyc import Connection, OneShotServer, Service

from .codegen import compile_plan
from .constants import WORKER_POOL_PROCESSES
from .io import BlockFile
from .jobs import Job, JobResult, JobResultTuple, OutputFile, ScanJob
from .plan import PhysicalPlan, Stage
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
    from .tasks import Task


class ExecutionEngine(ABC):
    @abstractmethod
    def execute_full_task(self, full_task: Task) -> list[JobResult]: ...

    @abstractmethod
    def cleanup(self) -> None: ...

    @trace_yield("collect results")
    def collect_results(self, results: list[JobResult], limit: int = math.inf) -> Iterable[Row]:  # type:ignore[assignment]
        output_files = {file for result in results for file in result.output_files}
        for file in output_files:
            for row in BlockFile(file.file_path).read_data_rows():
                yield row
                limit -= 1
                if limit <= 0:
                    return


class PythonExecutionEngine(ExecutionEngine):
    def __init__(self) -> None:
        self.shuffle_files_to_delete: set[Path] = set()

    @trace("execute full task")
    def execute_full_task(self, full_task: Task) -> list[JobResult]:
        print("###### Logical Plan")  # noqa: T201
        full_task.explain()
        print()  # noqa: T201
        physical_plan = PhysicalPlan.generate_physical_plan(full_task)
        print("##### Physical Plan")  # noqa: T201
        physical_plan.explain()
        print()  # noqa: T201

        TRACER.start("Execution")
        for i, stage in enumerate(physical_plan.stages):
            print("executing ", stage)  # noqa: T201
            TRACER.start(f"Stage {i}")
            for job in stage.create_jobs():
                stage.execute(job)
            TRACER.end()
        TRACER.end()
        last_stage = physical_plan.stages[-1]
        self.shuffle_files_to_delete.update(
            f.file_path for stage in physical_plan.stages for result in stage.job_results for f in result.output_files
        )
        return last_stage.job_results

    @trace_yield("collect results")
    def collect_results(self, results: list[JobResult], limit: int = math.inf) -> Iterable[Row]:  # type:ignore[assignment]
        output_files = {file for result in results for file in result.output_files}
        for file in output_files:
            for row in BlockFile(file.file_path).read_data_rows():
                yield row
                limit -= 1
                if limit <= 0:
                    return

    @trace("remove files")
    def cleanup(self) -> None:
        for shuffle_file in self.shuffle_files_to_delete:
            shuffle_file.unlink(missing_ok=True)
        self.shuffle_files_to_delete.clear()


class DistributedExecutionEngine(AbstractContextManager["DistributedExecutionEngine"], ExecutionEngine):
    def __init__(self) -> None:
        self.driver = Driver()

    def __enter__(self) -> Self:
        self.executor = Executor(port=5000, block=False)
        self.driver.add_executor("localhost", port=5000)
        return self

    def execute_full_task(self, full_task: Task) -> list[JobResult]:
        print("###### Logical Plan")  # noqa: T201
        full_task.explain()
        print()  # noqa: T201
        physical_plan = PhysicalPlan.generate_physical_plan(full_task)
        print("##### Physical Plan")  # noqa: T201
        physical_plan.explain()
        print()  # noqa: T201

        binary_output_file = compile_plan(physical_plan)
        self.driver.distribute_binary(binary_output_file)
        TRACER.start("Execution")
        for i, stage in enumerate(physical_plan.stages):
            TRACER.start(f"Stage {i}")
            job_results = self.driver.execute_stage_on_nodes(stage)
            stage.job_results.extend(job_results)
            TRACER.end()
        TRACER.end()
        last_stage = physical_plan.stages[-1]
        return last_stage.job_results

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.driver.stop()

    def cleanup(self) -> None:
        pass  # cleanup is done on executors, after driver stop


DistributedJobTuple = tuple[str, str, str, int, str]


@dataclass
class DistributedJob:
    id: str
    stage_id: str
    input_file: Path
    input_block_id: int
    output_file: Path = Path()
    executor_binary: Path = Path()

    def to_tuple(self) -> DistributedJobTuple:
        return (self.id, self.stage_id, str(self.input_file), self.input_block_id, str(self.output_file))

    @staticmethod
    def from_tuple(t: DistributedJobTuple) -> DistributedJob:
        return DistributedJob(
            id=t[0],
            stage_id=t[1],
            input_file=Path(t[2]),
            input_block_id=t[3],
            output_file=Path(t[4]),
        )

    @staticmethod
    def from_normal_job(job: Job, current_stage: str) -> DistributedJob:
        assert type(job) is ScanJob
        return DistributedJob(
            id=job.id,
            stage_id=current_stage,
            input_file=job.file_path.absolute(),
            input_block_id=job.block_id,
        )


def execute_job(job: DistributedJob) -> JobResult:
    print("executor: executing job", job)  # noqa: T201
    worker_id = multiprocessing.current_process().name
    trace_file = (job.executor_binary.parent / f"trace-{job.id}").absolute()
    TRACER.add_trace_file(trace_file, worker_id)
    ret_code = subprocess.call(  # noqa: S603
        [
            str(job.executor_binary),
            str(job.stage_id),
            str(job.input_file.absolute()),
            str(job.input_block_id),
            str(job.output_file.absolute()),
            str(trace_file),
        ],
    )
    assert ret_code == 0, f"executor: job {job.id} failed with code {ret_code}"
    # TODO(david): we should parse which files the executor actually created
    output_files = [OutputFile(executor_id="", file_path=job.output_file, partition=0)]
    return JobResult(job_id=job.id, executor_id="", output_files=output_files)


class Executor(Service):  # type:ignore[misc]
    def __init__(self, port: int, *, block: bool = True) -> None:
        self.id = str(port)
        self.executor_path = Path("executors") / self.id
        self.executor_path.mkdir(parents=True, exist_ok=False)
        server = OneShotServer(self, port=port)
        if block:
            server.start()
        else:
            self.thread = threading.Thread(target=server.start, daemon=False)
            self.thread.start()
        self.worker_pool = Pool(processes=WORKER_POOL_PROCESSES)

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

    def exposed_execute_jobs(self, jobs: list[DistributedJobTuple]) -> list[JobResultTuple]:
        parsed_jobs = [DistributedJob.from_tuple(job) for job in jobs]
        for job in parsed_jobs:
            job.output_file = self.executor_path / f"stage_{job.stage_id}_block_{job.input_block_id}.bin"
            job.executor_binary = self.executor_path / "executor_binary"
        job_results = self.worker_pool.map(execute_job, parsed_jobs)
        for result in job_results:
            result.executor_id = self.id
            result.output_files = [OutputFile(self.id, f.file_path, f.partition) for f in result.output_files]
        return [job_result.to_tuple() for job_result in job_results]

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

    def execute_stage_on_nodes(self, stage: Stage) -> list[JobResult]:
        jobs = [DistributedJob.from_normal_job(job, stage.stage_id).to_tuple() for job in stage.create_jobs()]
        async_calls = []
        for executor, job_chunk in zip(self.executors, chunk_list(jobs, len(self.executors)), strict=True):
            remote_execute = rpyc.async_(executor.exposed_execute_jobs)
            async_calls.append(remote_execute(job_chunk))
        results_waited_for: list[list[JobResultTuple]] = [async_call.value for async_call in async_calls]
        return [JobResult.from_tuple(result) for results in results_waited_for for result in results]

    @trace("distribute binary")
    def distribute_binary(self, binary_file: Path) -> None:
        with binary_file.open("rb") as f:
            for executor in self.executors:
                executor.recieve_binary(f.read())
