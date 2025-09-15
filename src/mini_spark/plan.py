from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from typing import TYPE_CHECKING, cast

from tabulate import tabulate

from mini_spark.constants import DEBUG_EXECUTION

from .io import BlockFile
from .jobs import Job, JobResult, JoinJob, LoadShuffleFilesJob, OutputFile, ScanJob
from .sql import BinaryOperatorColumn, Col
from .tasks import (
    AggregateTask,
    BroadcastHashJoinTask,
    ConsumerTask,
    LoadShuffleFilesTask,
    LoadTableBlockTask,
    ProducerTask,
    ProjectTask,
    Task,
    VoidTask,
    WriterTask,
    WriteToLocalFileTask,
    WriteToShufflePartitions,
)
from .utils import (
    convert_columns_to_rows,
    trace,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .constants import Columns


def debug_task_output(task: Task, chunk: Columns | None) -> None:
    if not DEBUG_EXECUTION or chunk is None:
        return
    print(task, "OUTPUT")  # noqa: T201
    assert task.inferred_schema is not None
    rows = list(convert_columns_to_rows(chunk, task.inferred_schema))
    print(tabulate(rows, tablefmt="rounded_outline", headers="keys"))  # noqa: T201


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
        job_result = deepcopy(self)._execute(job)  # We deepcopy to reset the state of all tasks # noqa: SLF001
        self.job_results.append(job_result)
        return job_result

    def _execute(self, job: Job) -> JobResult:
        job_result = JobResult(job.id, "local", [])
        block_generator = iter(self.producer.generate_chunks(job))
        while True:
            try:
                chunk, is_last = next(block_generator)
            except StopIteration:
                chunk = None
                is_last = True
            debug_task_output(self.producer, chunk)
            for consumer in self.consumers:
                chunk, is_last = consumer.execute(chunk, is_last=is_last)
                debug_task_output(consumer, chunk)
            written_files = self.writer.write(chunk, self.stage_id)
            job_result.output_files += written_files
            if is_last:
                break
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
        elif type(self.producer) is BroadcastHashJoinTask:
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
        dependencies = ",".join(str(dep.stage_id) for dep in self.dependencies)
        return (
            f"[Stage {self.stage_id}: {type(self.producer).__name__} -> {consumers}{type(self.writer).__name__}, "
            f"deps: ({dependencies})]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def explain(self) -> None:
        self.full_task.explain()


def split_into_stages(root_task: Task, current_stage: Stage | None = None) -> Iterable[Stage]:
    if current_stage is None:
        current_stage = Stage()
    curr_task = root_task
    while curr_task.parent_task is not None:
        if type(curr_task) is BroadcastHashJoinTask:
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
        if type(task) is BroadcastHashJoinTask:
            PhysicalPlan.infer_schema(task.right_side_task)
        PhysicalPlan.infer_schema(task.parent_task)

    @staticmethod
    def expand_tasks(task: Task) -> None:
        if type(task) is VoidTask:
            return
        if type(task) is BroadcastHashJoinTask:
            # TODO(david): decompose join_condition: distribute the right keys to right shuffle task
            assert type(task.join_condition) is BinaryOperatorColumn
            task.left_key = task.join_condition.left_side
            task.right_key = task.join_condition.right_side
            task.parent_task = WriteToShufflePartitions(task.parent_task, key_column=task.left_key)
            task.right_side_task = WriteToShufflePartitions(task.right_side_task, key_column=task.right_key)
            PhysicalPlan.expand_tasks(task.right_side_task)
        if type(task) is AggregateTask:
            original_parent = task.parent_task
            task.parent_task = AggregateTask(
                task.parent_task, group_by_column=task.group_by_column, agg_columns=task.agg_columns
            )
            task.parent_task = WriteToShufflePartitions(task.parent_task, key_column=task.group_by_column)
            task.parent_task = LoadShuffleFilesTask(task.parent_task)
            task.before_shuffle = False
            PhysicalPlan.expand_tasks(original_parent)
            return
        PhysicalPlan.expand_tasks(task.parent_task)

    @staticmethod
    def cleanup_output_column_names(task: Task) -> None:
        output_schema = task.inferred_schema
        assert output_schema is not None
        if not any("." in col_name for (col_name, _) in output_schema):
            return
        clean_columns = [Col(col_name).alias(col_name.split(".")[-1]) for (col_name, _) in output_schema]
        task.parent_task = ProjectTask(task.parent_task, columns=clean_columns)
        new_output_schema = list(
            zip(
                [col.name for col in clean_columns],
                [col_type for (_, col_type) in output_schema],
                strict=True,
            )
        )
        task.parent_task.inferred_schema = new_output_schema
        task.inferred_schema = new_output_schema

    @staticmethod
    @trace("Physical Plan Generation")
    def generate_physical_plan(full_task: Task) -> PhysicalPlan:
        full_task = WriteToLocalFileTask(full_task)
        PhysicalPlan.expand_tasks(full_task)
        PhysicalPlan.infer_schema(full_task)
        PhysicalPlan.cleanup_output_column_names(full_task)
        stages = list(reversed(list(split_into_stages(full_task))))
        for stage_id, stage in enumerate(stages):
            stage.late_initialize(stage_id=str(stage_id))
        return PhysicalPlan(stages)

    def explain(self) -> None:
        for stage_num, stage in enumerate(self.stages):
            print("Stage", stage_num)  # noqa: T201
            stage.explain()
            print("-" * 10)  # noqa: T201
