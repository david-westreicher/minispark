from typing import Iterable
from copy import deepcopy
from pathlib import Path
from multiprocessing import Pool, Queue
from collections import defaultdict
from .io import Schema
from .tasks import (
    Job,
    Task,
    VoidTask,
    ShuffleToFileTask,
)
from .constants import Row

USE_WORKERS = True
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
        with Pool(processes=self.worker_count, initializer=init_worker) as worker_pool:
            Analyzer(self.task).analyze()
            stages = list(reversed(list(self.split_into_stages(self.task))))
            shuffle_files_to_delete: set[Path] = set()
            for i, stage in enumerate(stages):
                print("#" * 100)
                print("Stage", i)
                stage.explain()
                jobs = list(stage.create_jobs(stage, self.worker_count, i))
                print("Jobs:", len(jobs))
                for job in jobs:
                    job.current_stage = i
                    if job.shuffle_file is not None:
                        shuffle_files_to_delete.add(job.shuffle_file)
                grouped_jobs = self.group_jobs_by_worker(jobs)
                print("Physical plan created")
                if USE_WORKERS:
                    for job_result in worker_pool.imap_unordered(self.ex, grouped_jobs):
                        yield from job_result
                else:
                    for job_result in map(self.ex, grouped_jobs):
                        yield from job_result
            for shuffle_file in shuffle_files_to_delete:
                shuffle_file.unlink(missing_ok=True)

    def group_jobs_by_worker(self, jobs: list[Job]) -> list[list[Job]]:
        assert all(job.task == jobs[0].task for job in jobs)
        worker_list = defaultdict(list)
        for job in jobs:
            worker_list[job.worker_id].append(job)
        # the driver needs different copies of the task because it keeps a context
        for jobs in worker_list.values():
            same_task = deepcopy(jobs[0].task)
            for job in jobs:
                job.task = same_task
        single_worker_jobs = worker_list[-1]
        del worker_list[-1]
        return [list(job_list) for job_list in worker_list.values()] + [
            [job] for job in single_worker_jobs
        ]

    def ex(self, jobs: list[Job]) -> list[Row]:
        final_result = []
        for job in jobs:
            if USE_WORKERS:
                if job.worker_id == -1:
                    job.worker_id = worker_id
                job.worker_count = self.worker_count
            else:
                if job.worker_id == -1:
                    job.worker_id = 0
                job.worker_count = 2
            assert job.worker_id <= self.worker_count
            print("Job started", job.task.__class__.__name__, job.worker_id)
            final_result = list(job.execute())
            print("Job finished", job.task.__class__.__name__, job.worker_id)
        return final_result

    def split_into_stages(self, root_task: Task) -> Iterable[Task]:
        curr_task = root_task
        while curr_task.parent_task is not None:
            if type(curr_task.parent_task) in {ShuffleToFileTask}:
                tmp, curr_task.parent_task = curr_task.parent_task, VoidTask()
                yield deepcopy(root_task)
                curr_task.parent_task = tmp
                yield from self.split_into_stages(curr_task.parent_task)
                return
            curr_task = curr_task.parent_task
        yield root_task


class Analyzer:
    def __init__(self, task: Task):
        self.task = task

    def analyze(self) -> Schema:
        return self.task.validate_schema()
