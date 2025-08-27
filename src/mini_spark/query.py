from typing import Iterable
from copy import deepcopy
from pathlib import Path
from multiprocessing import Pool, Queue
from .tasks import (
    Job,
    LoadShuffleFileTask,
    Task,
    JoinTask,
    VoidTask,
    ShuffleToFileTask,
)
from .constants import Columns, Row, USE_WORKERS
from .utils import convert_columns_to_rows

worker_id = 0
id_queue: "Queue[int]" = Queue()


def init_worker():
    global worker_id
    worker_id = id_queue.get()


class Executor:
    def __init__(self, task: Task, worker_count: int = 10):
        self.task = task
        self.worker_count = worker_count

    def execute(self) -> Iterable[Row]:
        for worker_id in range(self.worker_count):
            id_queue.put(worker_id)
        Analyzer.analyze(self.task)
        stages = list(reversed(list(self.split_into_stages(self.task))))
        for stage_num, stage in enumerate(stages):
            Analyzer.optimize(stage, stage_num)
        shuffle_files_to_delete: set[Path] = set()
        with Pool(processes=self.worker_count, initializer=init_worker) as worker_pool:
            for i, stage in enumerate(stages):
                print("#" * 100)
                print("Stage", i)
                assert stage.inferred_schema is not None
                stage.explain()
                jobs = list(stage.create_jobs(stage, self.worker_count))
                for job in jobs:
                    job.current_stage = i
                    shuffle_files_to_delete.update(job.files_to_delete)
                print("Jobs:", len(jobs))
                print("Physical plan created")
                if USE_WORKERS:
                    for job_result in worker_pool.imap_unordered(
                        self.execute_job_group_on_worker, jobs
                    ):
                        yield from convert_columns_to_rows(
                            job_result, stage.inferred_schema
                        )
                else:
                    for job_result in map(self.execute_job_group_on_worker, jobs):
                        yield from convert_columns_to_rows(
                            job_result, stage.inferred_schema
                        )
            for shuffle_file in shuffle_files_to_delete:
                shuffle_file.unlink(missing_ok=True)

    def execute_job_group_on_worker(self, job: Job) -> Columns:
        job.worker_id = worker_id
        assert job.worker_id <= self.worker_count
        return job.execute()

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
    def analyze(task: Task):
        if type(task) is VoidTask:
            return
        task.inferred_schema = task.validate_schema()
        if type(task) is JoinTask:
            Analyzer.analyze(task.right_side_task)
        Analyzer.analyze(task.parent_task)

    @staticmethod
    def optimize(task: Task, stage_num: int) -> None:
        if type(task) is LoadShuffleFileTask:
            task.stage_to_load = stage_num - 1
        if type(task) is JoinTask:
            task.left_shuffle_stage = stage_num - 2
            task.right_shuffle_stage = stage_num - 1
        if type(task) is VoidTask:
            return
        Analyzer.optimize(task.parent_task, stage_num)
