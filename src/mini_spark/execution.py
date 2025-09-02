from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from .io import BlockFile
from .jobs import Job, JobResult, JoinJob, LoadShuffleFilesJob, OutputFile, ScanJob
from .sql import BinaryOperatorColumn
from .tasks import (
    AggregateCountTask,
    ConsumerTask,
    JoinTask,
    LoadShuffleFilesTask,
    LoadTableBlockTask,
    ProducerTask,
    Task,
    VoidTask,
    WriterTask,
    WriteToLocalFileTask,
    WriteToShufflePartitions,
)
from .utils import (
    TRACER,
    trace,
    trace_yield,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .constants import Row


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
    def execute_full_task(self, full_task: Task) -> list[JobResult]: ...
    def collect_results(self, results: list[JobResult], limit: int = -1) -> Iterable[Row]: ...
    def cleanup(self) -> None: ...


"""
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
            assert type(job) is ScanJob
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
        PhysicalPlan.infer_schema_and_expand_tasks(full_task)
        stages = list(reversed(list(split_into_stages(full_task))))
        for stage_num, stage in enumerate(stages):
            PhysicalPlan.optimize(stage, stage_num)
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
"""


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
            TRACER.start(f"Stage {i}")
            for job in stage.create_jobs():
                stage.execute(job)
            TRACER.end()
        TRACER.end()
        last_stage = physical_plan.stages[-1]
        self.shuffle_files_to_delete.update(
            f.file_path for result in last_stage.job_results for f in result.output_files
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


class Stage:
    def __init__(self) -> None:
        self.full_task: Task = VoidTask()
        self.dependencies: list[Stage] = []

    def late_initialize(self, stage_id: str) -> None:
        self.stage_id = stage_id
        producer, *consumers, writer = list(self.full_task.task_chain)
        assert isinstance(producer, ProducerTask)
        assert all(isinstance(consumer, ConsumerTask) for consumer in consumers)
        assert isinstance(writer, WriterTask)
        self.producer = producer
        self.consumers = cast("list[ConsumerTask]", consumers)
        self.writer = writer
        self.job_results: list[JobResult] = []

    @trace("job")
    def execute(self, job: Job) -> JobResult:
        job_result = JobResult(job.id, "local", [])
        for input_block in self.producer.generate_blocks(job):
            last_output = input_block
            for consumer in self.consumers:
                last_output = consumer.execute(last_output)
            written_files = self.writer.write(last_output, self.stage_id)
            job_result.output_files += written_files
        self.job_results.append(job_result)
        return job_result

    def create_jobs(self) -> Iterable[Job]:
        if type(self.producer) is LoadTableBlockTask:
            block_starts = BlockFile(self.producer.file_path).block_starts
            for block_id in range(len(block_starts)):
                yield ScanJob(file_path=self.producer.file_path, block_id=block_id)
        elif type(self.producer) is LoadShuffleFilesTask:
            assert len(self.dependencies) == 1
            shuffle_files = self.get_shuffle_files_of(self.dependencies[0])
            for shuffle_files_for_partition in shuffle_files.values():
                yield LoadShuffleFilesJob(shuffle_files=list(shuffle_files_for_partition))
        elif type(self.producer) is JoinTask:
            assert len(self.dependencies) == 2  # noqa: PLR2004
            left_stage, right_stage = self.dependencies
            left_shuffle_files = self.get_shuffle_files_of(left_stage)
            right_shuffle_files = self.get_shuffle_files_of(right_stage)
            partitions = set(left_shuffle_files.keys()) | set(right_shuffle_files.keys())
            for partition in partitions:
                yield JoinJob(
                    left_shuffle_files=list(left_shuffle_files[partition]),
                    right_shuffle_files=list(right_shuffle_files[partition]),
                )
        else:
            raise NotImplementedError(f"Job creation not implemented for {type(self.producer)}")

    def get_shuffle_files_of(self, stage: Stage) -> dict[int, set[OutputFile]]:
        shuffle_files: dict[int, set[OutputFile]] = defaultdict(set)
        for job_result in stage.job_results:
            for shuffle_file in job_result.output_files:
                shuffle_files[shuffle_file.partition].add(shuffle_file)
        return shuffle_files

    def __str__(self) -> str:
        consumers = f"[{','.join(type(c).__name__ for c in self.consumers)}] -> " if self.consumers else ""
        return f"Stage {self.stage_id}: {type(self.producer).__name__} -> {consumers}{type(self.writer).__name__}"

    def __repr__(self) -> str:
        return self.__str__()

    def explain(self) -> None:
        self.writer.explain()


def split_into_stages(root_task: Task, current_stage: Stage | None = None) -> Iterable[Stage]:
    if current_stage is None:
        current_stage = Stage()
    curr_task = root_task
    while curr_task.parent_task is not None:
        if type(curr_task) is JoinTask:
            old_left, curr_task.parent_task = curr_task.parent_task, VoidTask()
            old_right, curr_task.right_side_task = (
                curr_task.right_side_task,
                VoidTask(),
            )
            current_stage.full_task = root_task
            left_stage = Stage()
            right_stage = Stage()
            current_stage.dependencies = [left_stage, right_stage]
            yield current_stage
            yield from split_into_stages(old_right, right_stage)
            yield from split_into_stages(old_left, left_stage)
            return
        if type(curr_task.parent_task) in {WriteToShufflePartitions}:
            tmp, curr_task.parent_task = curr_task.parent_task, VoidTask()
            current_stage.full_task = deepcopy(root_task)
            curr_task.parent_task = tmp
            next_stage = Stage()
            current_stage.dependencies = [next_stage]
            yield current_stage
            yield from split_into_stages(curr_task.parent_task, next_stage)
            return
        curr_task = curr_task.parent_task
    current_stage.full_task = root_task
    yield current_stage


class PhysicalPlan:
    def __init__(self, stages: list[Stage]) -> None:
        self.stages = stages

    @staticmethod
    def infer_schema(task: Task) -> None:
        if type(task) is VoidTask:
            return
        task.inferred_schema = task.validate_schema()
        if type(task) is JoinTask:
            PhysicalPlan.infer_schema(task.right_side_task)
        PhysicalPlan.infer_schema(task.parent_task)

    @staticmethod
    def expand_tasks(task: Task) -> None:
        if type(task) is VoidTask:
            return
        if type(task) is JoinTask:
            # TODO(david): decompose join_condition: distribute the right keys to right shuffle task
            assert type(task.join_condition) is BinaryOperatorColumn
            task.left_key = task.join_condition.left_side
            task.right_key = task.join_condition.right_side
            task.parent_task = WriteToShufflePartitions(task.parent_task, key_column=task.left_key)
            task.right_side_task = WriteToShufflePartitions(task.right_side_task, key_column=task.right_key)
            PhysicalPlan.expand_tasks(task.right_side_task)
        if type(task) is AggregateCountTask:
            task.parent_task = WriteToShufflePartitions(task.parent_task, key_column=task.group_by_column)
            task.parent_task = LoadShuffleFilesTask(task.parent_task)
        PhysicalPlan.expand_tasks(task.parent_task)

    @staticmethod
    @trace("Physical Plan Generation")
    def generate_physical_plan(full_task: Task) -> PhysicalPlan:
        full_task = WriteToLocalFileTask(full_task)
        PhysicalPlan.expand_tasks(full_task)
        PhysicalPlan.infer_schema(full_task)
        stages = list(reversed(list(split_into_stages(full_task))))
        for stage_id, stage in enumerate(stages):
            stage.late_initialize(stage_id=str(stage_id))
        return PhysicalPlan(stages)

    def explain(self) -> None:
        for stage_num, stage in enumerate(self.stages):
            print("Stage", stage_num)  # noqa: T201
            stage.explain()
            print("-" * 10)  # noqa: T201
