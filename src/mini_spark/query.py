from typing import Iterable
from copy import deepcopy
from pathlib import Path
from .tasks import (
    Job,
    LoadShuffleFileTask,
    Task,
    JoinTask,
    VoidTask,
    ShuffleToFileTask,
)
from .constants import Columns, Row
from .utils import (
    convert_columns_to_rows,
    TRACER,
    trace,
)
from . import utils


class Executor:
    def __init__(self, task: Task, worker_count: int = 10):
        self.task = task
        self.worker_count = worker_count
        self.shuffle_files_to_delete: set[Path] = set()

    def execute(self) -> Iterable[Row]:
        Analyzer.analyze(self.task)
        stages = list(reversed(list(self.split_into_stages(self.task))))
        for stage_num, stage in enumerate(stages):
            Analyzer.optimize(stage, stage_num)
        TRACER.start("Execution")
        for i, stage in enumerate(stages):
            print("#" * 100)
            print("Stage", i)
            TRACER.start(f"Stage {i}")
            yield from self.execute_stage(stage, i)
            TRACER.end()
        TRACER.end()

        TRACER.start("remove files")
        for shuffle_file in self.shuffle_files_to_delete:
            shuffle_file.unlink(missing_ok=True)
        self.shuffle_files_to_delete.clear()
        TRACER.end()

    def execute_stage(self, stage, stage_num: int) -> Iterable[Row]:
        assert stage.inferred_schema is not None
        stage.explain()
        TRACER.start("Create jobs")
        jobs = list(stage.create_jobs(stage, self.worker_count))
        for job in jobs:
            job.current_stage = stage_num
            self.shuffle_files_to_delete.update(job.files_to_delete)
        TRACER.end()
        print("Jobs:", len(jobs))
        print("Physical plan created")
        for job_i, job_result in enumerate(map(self.execute_job_group_on_worker, jobs)):
            TRACER.start(f"Process job {job_i}")
            yield from convert_columns_to_rows(job_result, stage.inferred_schema)
            TRACER.end()

    def execute_job_group_on_worker(self, job: Job) -> Columns:
        job.worker_id = 0
        assert job.worker_id <= self.worker_count
        utils.TRACER.start("job")
        result = job.execute()
        utils.TRACER.end()
        return result

    def split_into_stages(self, root_task: Task) -> Iterable[Task]:
        curr_task = root_task
        while curr_task.parent_task is not None:
            if type(curr_task) is JoinTask:
                old_left, curr_task.parent_task = curr_task.parent_task, VoidTask()
                old_right, curr_task.right_side_task = (
                    curr_task.right_side_task,
                    VoidTask(),
                )
                yield root_task
                yield from self.split_into_stages(old_right)
                yield from self.split_into_stages(old_left)
                return
            if type(curr_task.parent_task) in {ShuffleToFileTask}:
                tmp, curr_task.parent_task = curr_task.parent_task, VoidTask()
                yield deepcopy(root_task)
                curr_task.parent_task = tmp
                yield from self.split_into_stages(curr_task.parent_task)
                return
            curr_task = curr_task.parent_task
        yield root_task


class Analyzer:
    @staticmethod
    @trace("analyze")
    def analyze(task: Task):
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
