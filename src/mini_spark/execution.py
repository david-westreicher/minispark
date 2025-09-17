from __future__ import annotations

import math
import queue
import shutil
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING

from .codegen import compile_plan
from .constants import WORKER_POOL_PROCESSES
from .io import BlockFile
from .jobs import Job, JobResult, OutputFile
from .plan import PhysicalPlan
from .utils import (
    TRACER,
    trace,
    trace_yield,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import TracebackType

    from .constants import Row
    from .dataframe import DataFrame
    from .tasks import Task


class ExecutionError(Exception):
    def __init__(self, message: str = "Execution failed") -> None:
        super().__init__(message)


class ExecutionEngine(AbstractContextManager["ExecutionEngine"], ABC):
    @abstractmethod
    def execute_full_task(self, full_task: Task) -> list[JobResult]: ...

    def generate_physical_plan(self, full_task: Task) -> PhysicalPlan:
        return PhysicalPlan.generate_physical_plan(full_task)

    @trace_yield("collect results")
    def collect_results(self, results: list[JobResult], limit: int = math.inf) -> Iterable[Row]:  # type:ignore[assignment]
        output_files = {file for result in results for file in result.output_files}
        for file in output_files:
            for row in BlockFile(file.file_path).read_data_rows():
                yield row
                limit -= 1
                if limit <= 0:
                    return

    def sql(self, query: str) -> DataFrame:
        from .parser import parse_sql  # noqa: PLC0415

        df = parse_sql(query)
        df.engine = self
        return df


class PythonExecutionEngine(ExecutionEngine):
    def __init__(self) -> None:
        self.shuffle_files_to_delete: set[Path] = set()

    @trace("execute full task")
    def execute_full_task(self, full_task: Task) -> list[JobResult]:
        physical_plan = self.generate_physical_plan(full_task)
        TRACER.start("Execution")
        for i, stage in enumerate(physical_plan.stages):
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

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        for shuffle_file in self.shuffle_files_to_delete:
            shuffle_file.unlink(missing_ok=True)
        self.shuffle_files_to_delete.clear()


class ThreadEngine(ExecutionEngine):
    def __init__(self, work_folder: Path = Path("executors") / "local") -> None:
        self.work_folder = work_folder

    @trace("execute full task")
    def execute_full_task(self, full_task: Task) -> list[JobResult]:
        physical_plan = self.generate_physical_plan(full_task)
        binary_output_file = compile_plan(physical_plan)
        TRACER.start("Execution")
        self.worker_pool = ThreadWorkerPool(binary_output_file, WORKER_POOL_PROCESSES, self.work_folder)
        self.work_folder.mkdir(parents=True, exist_ok=False)
        for i, stage in enumerate(physical_plan.stages):
            TRACER.start(f"Stage {i}")
            job_results = self.worker_pool.execute_jobs_on_workers(int(stage.stage_id), list(stage.create_jobs()))
            stage.job_results.extend(job_results)
            TRACER.end()
        TRACER.end()
        last_stage = physical_plan.stages[-1]
        self.worker_pool.stop()
        return last_stage.job_results

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        shutil.rmtree(self.work_folder, ignore_errors=True)


class ThreadWorkerPool:
    def __init__(self, binary: Path, num_workers: int, work_folder: Path) -> None:
        self.job_queue: queue.Queue[tuple[int, Job]] = queue.Queue()
        self.result_queue: queue.Queue[JobResult] = queue.Queue()
        self.fail_event = threading.Event()
        self.workers = [
            ThreadWorker(
                str(worker_id),
                binary,
                self.job_queue,
                self.result_queue,
                self.fail_event,
                work_folder,
            )
            for worker_id in range(num_workers)
        ]

    @trace("execute jobs on workers")
    def execute_jobs_on_workers(self, stage_id: int, jobs: list[Job]) -> list[JobResult]:
        for job in jobs:
            self.job_queue.put((stage_id, job))
        while True:
            with self.job_queue.all_tasks_done:
                if self.job_queue.unfinished_tasks == 0:
                    break
                self.job_queue.all_tasks_done.wait(0.0001)
            if self.fail_event.is_set():
                raise ExecutionError
        results: list[JobResult] = []
        while len(results) < len(jobs):
            results.append(self.result_queue.get())
        return results

    @trace("stop workers")
    def stop(self) -> None:
        self.job_queue.shutdown()
        for worker in self.workers:
            worker.join()


class ThreadWorker(threading.Thread):
    def __init__(  # noqa: PLR0913
        self,
        worker_id: str,
        binary: Path,
        job_queue: queue.Queue[tuple[int, Job]],
        result_queue: queue.Queue[JobResult],
        fail_event: threading.Event,
        work_folder: Path,
    ) -> None:
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.fail_event = fail_event
        trace_file = work_folder / f"{worker_id}_trace.pftrace"
        self.process = subprocess.Popen(  # noqa: S603
            [
                str(binary),
                worker_id,
                str(work_folder / worker_id),
                str(trace_file),
            ],
            stdin=subprocess.PIPE,
            stderr=sys.stdout,
            stdout=subprocess.PIPE,
            text=False,
        )
        self.track_uuid = TRACER.new_track(self.worker_id)
        TRACER.add_trace_file(trace_file, worker_id, self.track_uuid)
        self.start()

    def run(self) -> None:
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        while True:
            try:
                stage_id, job = self.job_queue.get()
                TRACER.start("job", self.track_uuid)
                self.process.stdin.write(bytes([stage_id]))
                self.process.stdin.write(job.encode())
                self.process.stdin.flush()
                TRACER.start("await worker", self.track_uuid)
                job_result = JobResult(job_id=job.id, executor_id=self.worker_id, output_files=[])
                while True:
                    result = self.process.stdout.readline().decode().strip()
                    result_path, partition = result.split()
                    if result_path == "job_finished":
                        break
                    job_result.output_files.append(OutputFile(Path(result_path), int(partition)))
                TRACER.end(self.track_uuid)
                self.result_queue.put(job_result)
                self.job_queue.task_done()
                TRACER.end(self.track_uuid)
            except queue.ShutDown:
                self.stop_process()
                break
            except Exception as e:  # noqa: BLE001
                print(f"Worker {self.worker_id} failed: {e}", file=sys.stderr)  # noqa: T201
                self.fail_event.set()
                break

    def stop_process(self) -> None:
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.process.stdin.write(bytes([255]))
        self.process.stdin.flush()
        self.process.stdin.close()
        ret = self.process.wait()
        assert ret == 0, f"Worker {self.worker_id} exited with code {ret}"
