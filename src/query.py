from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Iterable, Self
from collections import Counter
from copy import deepcopy
from multiprocessing import Pool, Queue
from typing import Protocol
from collections import defaultdict
from .io import (
    ColumnType,
    deserialize_block_starts,
    deserialize_schema,
    deserialize_block,
    Schema,
    serialize_rows,
)
from .sql import Col, BinaryOperatorColumn

USE_WORKERS = True

Row = dict[str, Any]


@dataclass
class Job:
    task: "Task"
    block_id: int
    worker_count: int = -1
    worker_id: int = -1
    shuffle_file: Path | None = None

    def execute(self) -> Iterable[Row]:
        yield from self.task.execute(self)


class Task(Protocol):
    def execute(self, job: Job) -> Iterable[Row]: ...
    def validate_schema(self) -> Schema: ...
    def create_jobs(self, full_task: Self, worker_count: int) -> Iterable[Job]: ...
    def explain(self, lvl: int = 0) -> None: ...


@dataclass
class ProjectTask(Task):
    columns: list[Col]
    parent_task: Task

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            yield {col.name: col.execute(row) for col in self.columns}

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = {
            col.name
            for column in self.columns
            for col in column.all_nested_columns
            if type(col) is Col
        }
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in projection: {unknown_cols}")
        return [(col.name, col.type) for col in self.columns]

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Project({', '.join(str(col) for col in self.columns)})")
        self.parent_task.explain(lvl + 1)


@dataclass
class LoadTableTask(Task):
    file_path: Path
    parent_task: Task

    def execute(self, job: Job) -> Iterable[Row]:
        block_start = self.block_starts[job.block_id]
        with self.file_path.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, self.schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, self.schema)}

    @cached_property
    def schema(self) -> Schema:
        with self.file_path.open(mode="rb") as f:
            return list(deserialize_schema(f))

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file_path.open(mode="rb") as f:
            return deserialize_block_starts(f)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        return self.schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)
        for block_id in range(len(self.block_starts)):
            yield Job(full_task, block_id)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} LoadTable({self.file_path})")
        self.parent_task.explain(lvl + 1)


@dataclass
class LoadShuffleFileTask(Task):
    parent_task: Task

    def execute(self, job: Job) -> Iterable[Row]:
        assert job.shuffle_file is not None
        block_start = self.block_starts(job.shuffle_file)[job.block_id]
        schema = self.load_schema(job.shuffle_file)
        with job.shuffle_file.open(mode="rb") as f:
            f.seek(block_start)
            block_data = deserialize_block(f, schema)
            for row in zip(*block_data):
                yield {name: val for val, (name, _) in zip(row, schema)}
        job.shuffle_file.unlink(missing_ok=True)

    def load_schema(self, shuffle_file: Path) -> Schema:
        with shuffle_file.open(mode="rb") as f:
            return list(deserialize_schema(f))

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        for worker_from in range(worker_count):
            for worker_to in range(worker_count):
                shuffle_file = Path(f"shuffle/{worker_from}_{worker_to}.bin")
                if not shuffle_file.exists():
                    continue
                for block_id in range(len(self.block_starts(shuffle_file))):
                    yield Job(
                        full_task,
                        block_id,
                        worker_id=worker_to,
                        shuffle_file=shuffle_file,
                    )
        yield from []

    def block_starts(self, shuffle_file: Path) -> list[int]:
        with shuffle_file.open(mode="rb") as f:
            return deserialize_block_starts(f)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} LoadShuffleFile()")
        self.parent_task.explain(lvl + 1)


@dataclass
class FilterTask(Task):
    column: Col
    parent_task: Task

    def __post_init__(self):
        assert type(self.column) is BinaryOperatorColumn

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            if self.column.execute(row):
                yield row

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = [
            col.name for col in self.column.all_nested_columns if type(col) is Col
        ]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in filter: {unknown_cols}")
        return schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Filter({self.column})")
        self.parent_task.explain(lvl + 1)


@dataclass
class GroupByTask(Task):
    column: Col
    parent_task: Task

    def execute(self, job: Job) -> Iterable[Row]:
        result: dict[str, list[Row]] = defaultdict(list)
        for row in self.parent_task.execute(job):
            result[self.column.execute(row)].append(row)
        for key, rows in result.items():
            yield Row(key=key, key_col_name=self.column.name, rows=rows)

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = [
            col.name for col in self.column.all_nested_columns if type(col) is Col
        ]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [
            col for col in referenced_column_names if col not in schema_cols
        ]
        if unknown_cols:
            raise Exception(f"Unknown columns in GroupBy: {unknown_cols}")
        return schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} GroupBy({self.column})")
        self.parent_task.explain(lvl + 1)


@dataclass
class CountTask(Task):
    group_by_column: Col
    parent_task: Task
    counter: dict[Any, int] = field(default_factory=lambda: Counter())

    def execute(self, job: Job) -> Iterable[Row]:
        for row in self.parent_task.execute(job):
            self.counter[row[self.group_by_column.name]] += 1
        # print(self.counter, job)
        return [
            {self.group_by_column.name: key, "count": count}
            for key, count in self.counter.items()
        ]

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert self.group_by_column.name in {col_name for col_name, _ in schema}
        return [
            (self.group_by_column.name, ColumnType.UNKNOWN),
            ("count", ColumnType.INTEGER),
        ]

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Count()")
        self.parent_task.explain(lvl + 1)


@dataclass
class ShuffleToFileTask(Task):
    parent_task: Task

    def execute(self, job: Job) -> Iterable[Row]:
        shuffle_buckets: dict[int, list[Row]] = defaultdict(list)
        for row in self.parent_task.execute(job):
            destination_shuffle = hash(row["key"]) % job.worker_count
            shuffle_buckets[destination_shuffle].extend(row["rows"])
        for shuffle_bucket, rows in shuffle_buckets.items():
            shuffle_file = Path(f"shuffle/{job.worker_id}_{shuffle_bucket}.bin")
            print("writing", shuffle_file)
            # TODO SHOULD BE APPEND NOT OVERWRITE
            serialize_rows(rows, shuffle_file)
        return []

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    def explain(self, lvl: int = 0):
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} ShuffleToFile()")
        self.parent_task.explain(lvl + 1)


@dataclass
class VoidTask(Task):
    parent_task: Task | None = None

    def execute(self, job: Job) -> Iterable[Row]:
        raise NotImplementedError

    def validate_schema(self) -> Schema:
        return []

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from []

    def explain(self, lvl: int = 0):
        pass


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
            for i, stage in enumerate(stages):
                print("#" * 100)
                print("Stage", i)
                stage.explain()
                jobs = list(stage.create_jobs(stage, self.worker_count))
                grouped_jobs = self.group_jobs_by_worker(jobs)
                if USE_WORKERS:
                    for job_result in worker_pool.imap_unordered(self.ex, grouped_jobs):
                        yield from job_result
                else:
                    for job_result in map(self.ex, grouped_jobs):
                        yield from job_result

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
            final_result = list(job.execute())
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


class GroupedData:
    def __init__(self, df: "DataFrame", column: Col):
        self.df = df
        self.df.task = GroupByTask(column, self.df.task)
        self.group_column = column

    def count(self) -> "DataFrame":
        self.df.task = ShuffleToFileTask(self.df.task)
        self.df.task = LoadShuffleFileTask(self.df.task)
        self.df.task = CountTask(self.group_column, self.df.task)
        return self.df


class DataFrame:
    def __init__(self) -> None:
        self.task: Task = VoidTask()

    def table(self, file_path: str) -> Self:
        self.task = LoadTableTask(Path(file_path), self.task)
        return self

    def collect(self) -> list[Row]:
        return list(Executor(self.task).execute())

    def show(self, n=20) -> None:
        first = True
        for row in Executor(self.task).execute():
            if first:
                print("\t".join(row.keys()))
                first = False
            print("\t".join(str(v) for v in row.values()))
            n -= 1
            if n == 0:
                break

    @property
    def schema(self) -> Schema:
        return Analyzer(self.task).analyze()

    def select(self, *columns: Col) -> Self:
        if columns == ():
            columns = tuple(Col(col_name) for col_name, _ in self.schema)
        self.task = ProjectTask(list(columns), self.task)
        return self

    def filter(self, column: Col) -> Self:
        self.task = FilterTask(column, self.task)
        return self

    def group_by(self, column: Col) -> GroupedData:
        return GroupedData(self, column)


if __name__ == "__main__":
    df = DataFrame().table("data.bin")
    df.show()
    df = (
        df.select(
            (Col("col_a") * -1).alias("col_a_minus"),
            (Col("col_b") * 1).alias("col_b_plus"),
            Col("col_c").alias("col_d"),
        )
        .filter(Col("col_a_minus") <= Col("col_b_plus"))
        .select(
            Col("col_a_minus").alias("final_a"),
            Col("col_b_plus").alias("final_b"),
            Col("col_d").alias("final_d"),
        )
        # .filter(Col("final_a") == -1)
        .group_by(Col("final_d"))
        .count()
    )
    df.show(n=-1)
