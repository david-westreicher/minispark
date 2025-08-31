from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from .algorithms import SORT_BLOCK_SIZE, external_merge_join, external_sort
from .constants import (
    SHUFFLE_FOLDER,
    SHUFFLE_PARTITIONS,
    Columns,
    ColumnType,
    Row,
    Schema,
)
from .io import BlockFile
from .sql import BinaryOperatorColumn, Col
from .utils import convert_rows_to_columns, create_temp_file, nice_schema, trace

if TYPE_CHECKING:
    from collections.abc import Iterable

JoinType = Literal["inner", "left", "right", "outer"]


@dataclass
class Job:
    task: Task
    worker_id: int = -1
    current_stage: int = -1

    @property
    def files_to_delete(self) -> Iterable[Path]:
        yield from []

    def execute(self) -> Columns:
        out: Columns = ()
        for task in self.task.task_chain:
            out = task.execute(out, self)
        return out


@dataclass(kw_only=True)
class LoadTableBlockJob(Job):
    table_file: Path
    block_id: int


@dataclass(kw_only=True)
class WriteToLocalFileJob(Job):
    local_file: Path


@dataclass(kw_only=True)
class LoadShuffleFilesJob(Job):
    shuffle_files: list[Path]
    shuffle_block_to_load: tuple[Path, int] | None = None

    def execute(self) -> Columns:
        last_execution: Columns = ()
        for shuffle_file in self.shuffle_files:
            if not shuffle_file.exists():
                continue
            for block_id in range(len(BlockFile(shuffle_file).block_starts)):
                self.shuffle_block_to_load = (shuffle_file, block_id)
                last_execution = super().execute()
        return last_execution

    @property
    def files_to_delete(self) -> Iterable[Path]:
        yield from self.shuffle_files


@dataclass(kw_only=True)
class LoadJoinFilesJob(Job):
    left_shuffle_files: list[Path]
    right_shuffle_files: list[Path]

    @property
    def files_to_delete(self) -> Iterable[Path]:
        yield from self.left_shuffle_files
        yield from self.right_shuffle_files


@trace("project_col")
def project_column(col: Col, input_columns: Columns, schema: Schema) -> list[Any]:
    col = col.schema_executor(schema)
    return [col.execute_row(row) for row in zip(*input_columns, strict=True)]


@dataclass
class Task(ABC):
    parent_task: Task
    inferred_schema: Schema | None = None

    @abstractmethod
    def execute(self, input_columns: Columns, job: Job) -> Columns: ...

    @abstractmethod
    def explain(self, lvl: int = 0) -> None: ...

    @abstractmethod
    def generate_zig_code(self, function_name: str) -> str: ...

    def validate_schema(self) -> Schema:
        return self.parent_task.validate_schema()

    @trace("create_jobs")
    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)

    @property
    def task_chain(self) -> Iterable[Task]:
        if type(self) is VoidTask:
            return
        yield from self.parent_task.task_chain
        yield self


@dataclass(kw_only=True)
class ProjectTask(Task):
    columns: list[Col]

    @trace("ProjectTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert self.parent_task.inferred_schema is not None
        return tuple(project_column(col, input_columns, self.parent_task.inferred_schema) for col in self.columns)

    def generate_zig_code(self, function_name: str) -> str:
        def generate_projection_functions() -> str:
            assert self.parent_task.inferred_schema is not None
            projections_code = [
                col.generate_zig_projection_function(
                    f"{function_name}_project_{col_num:0>2}",
                    self.parent_task.inferred_schema,
                )
                for col_num, col in enumerate(self.columns)
            ]
            return "\n".join(projections_code)

        def generate_projection_calls() -> str:
            assert self.parent_task.inferred_schema is not None
            code = [
                f"const slice: []ColumnData = try allocator.alloc(ColumnData, {len(self.columns)});",
                *[
                    f"""
                        const col_{col_num:0>2} = try allocator.alloc({
                        col.infer_type(self.parent_task.inferred_schema).native_zig_type
                    }, rows);
                        slice[{col_num}] = try {function_name}_project_{col_num:0>2}(
                            allocator, input, col_{col_num:0>2});
                    """
                    for col_num, col in enumerate(self.columns)
                ],
            ]
            return "\n".join(code)

        return f"""
        {generate_projection_functions()}
        pub fn {function_name}(
            allocator: std.mem.Allocator,
            input: []const ColumnData,
            job: Job,
            schema: []const ColumnSchema) ![]const ColumnData {{
            try Executor.GLOBAL_TRACER.startEvent("{function_name}");
            _ = job;
            _ = schema;
            const rows = input[0].len();
            {generate_projection_calls()}
            try Executor.GLOBAL_TRACER.endEvent("{function_name}");
            return slice;
        }}
        """

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        # expand * to all columns from previous schema
        self.columns = [
            sub
            for col in self.columns
            for sub in ([Col(name) for name, _ in schema] if type(col) is Col and col.name == "*" else [col])
        ]

        referenced_column_names = {
            col.name for column in self.columns for col in column.all_nested_columns if type(col) is Col
        }
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in projection: {unknown_cols}")
        return [(col.name, col.infer_type(schema)) for col in self.columns]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} Project({', '.join(str(col) for col in self.columns)}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadTableTask(Task):
    file_path: Path

    @trace("LoadTableTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert type(job) is LoadTableBlockJob
        return BlockFile(job.table_file).read_block_data_columns_by_id(job.block_id)

    def generate_zig_code(self, function_name: str) -> str:
        return f"""
            pub fn {function_name}(
                allocator: std.mem.Allocator,
                input: []const ColumnData,
                job: Job,
                schema: []const ColumnSchema) ![]const ColumnData {{
                try Executor.GLOBAL_TRACER.startEvent("{function_name}");
                _ = input;
                _ = schema;
                const block_file = try Executor.BlockFile.initFromFile(allocator, job.input_file);
                const data = try block_file.readBlock(job.input_block_id);
                try Executor.GLOBAL_TRACER.endEvent("{function_name}");
                return data;
            }}

        """

    @cached_property
    def file_schema(self) -> Schema:
        return BlockFile(self.file_path).file_schema

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert schema == []
        return self.file_schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        yield from self.parent_task.create_jobs(full_task, worker_count)
        for block_id in range(len(BlockFile(self.file_path).block_starts)):
            yield LoadTableBlockJob(
                full_task,
                table_file=self.file_path,
                block_id=block_id,
            )

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} LoadTable({self.file_path}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class LoadShuffleFileTask(Task):
    stage_to_load: int = -1

    @trace("LoadShuffleFileTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert type(job) is LoadShuffleFilesJob
        assert job.shuffle_block_to_load is not None
        table_file, block_id = job.shuffle_block_to_load
        return BlockFile(table_file).read_block_data_columns_by_id(block_id)

    def generate_zig_code(self, function_name: str) -> str:
        raise NotImplementedError

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        for partition in range(SHUFFLE_PARTITIONS):
            yield LoadShuffleFilesJob(
                full_task,
                shuffle_files=[
                    (SHUFFLE_FOLDER / f"{self.stage_to_load}_{worker_from}_{partition}.bin")
                    for worker_from in range(worker_count)
                ],
            )

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} LoadShuffleFile(stage_to_load={self.stage_to_load}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class FilterTask(Task):
    condition: Col

    def __post_init__(self) -> None:
        assert type(self.condition) is BinaryOperatorColumn, type(self.condition)

    @trace("FilterTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert self.parent_task.inferred_schema is not None
        condition_col = project_column(
            self.condition,
            input_columns,
            self.parent_task.inferred_schema,
        )
        return tuple([val for val, cond in zip(col, condition_col, strict=True) if cond] for col in input_columns)

    def generate_zig_code(self, function_name: str) -> str:
        condition_function_name = f"{function_name}_condition"

        def generate_condition_function() -> str:
            assert self.parent_task.inferred_schema is not None
            return self.condition.generate_zig_condition_function(
                condition_function_name,
                self.parent_task.inferred_schema,
            )

        return f"""
            {generate_condition_function()}
            pub fn {function_name}(
                allocator: std.mem.Allocator,
                input: []const ColumnData,
                job: Job,
                schema: []const ColumnSchema) ![]const ColumnData {{
                try Executor.GLOBAL_TRACER.startEvent("{function_name}");
                _ = job;
                _ = schema;
                const rows = input[0].len();
                const condition_col = try allocator.alloc(bool, rows);
                {condition_function_name}(allocator, input, condition_col);
                const slice: []ColumnData = try allocator.alloc(ColumnData, input.len);
                for (input, 0..) |col, col_idx| {{
                    const filtered_column = try Executor.filter_column(col, condition_col, allocator);
                    slice[col_idx] = filtered_column;
                }}
                try Executor.GLOBAL_TRACER.endEvent("{function_name}");
                return slice;
            }}
        """

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        referenced_column_names = [col.name for col in self.condition.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Filter: {unknown_cols}")
        return schema

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Filter({self.condition}):{nice_schema(self.inferred_schema)}")  # noqa: T201
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class JoinTask(Task):
    right_side_task: Task
    join_condition: Col
    how: JoinType = "inner"
    left_shuffle_stage: int = -1
    right_shuffle_stage: int = -1
    left_key: Col | None = None
    right_key: Col | None = None

    @trace("JoinTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert self.left_key
        assert self.right_key
        assert type(job) is LoadJoinFilesJob
        left_sorted_file = self.sort_shuffle_files(
            job.left_shuffle_files,
            self.left_key,
        )
        right_sorted_file = self.sort_shuffle_files(
            job.right_shuffle_files,
            self.right_key,
        )
        joined_rows: list[Row] = []
        if left_sorted_file and right_sorted_file:
            # TODO(david): does not hold for all join types
            joined_rows = list(
                external_merge_join(
                    left_sorted_file,
                    right_sorted_file,
                    self.left_key.execute,
                    self.right_key.execute,
                    self.how,
                ),
            )
        if left_sorted_file is not None:
            left_sorted_file.unlink(missing_ok=True)
        if right_sorted_file is not None:
            right_sorted_file.unlink(missing_ok=True)
        assert self.inferred_schema
        return convert_rows_to_columns(joined_rows, self.inferred_schema)

    @trace("Sort shuffle file")
    def sort_shuffle_files(self, shuffle_files: list[Path], key: Col) -> Path | None:
        if not shuffle_files:
            return None
        shuffle_file = BlockFile(create_temp_file(), SORT_BLOCK_SIZE).merge_files(shuffle_files).file
        output_file = create_temp_file()
        tmp_file = create_temp_file()
        external_sort(shuffle_file, key.execute, output_file, tmp_file)
        shuffle_file.unlink(missing_ok=True)
        tmp_file.unlink(missing_ok=True)
        return output_file

    def generate_zig_code(self, function_name: str) -> str:
        raise NotImplementedError

    def validate_schema(self) -> Schema:
        left_schema = self.parent_task.validate_schema()
        right_schema = self.right_side_task.validate_schema()
        referenced_column_names = [col.name for col in self.join_condition.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in left_schema + right_schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in Join: {unknown_cols}")
        assert type(self.join_condition) is BinaryOperatorColumn
        # TODO(david): decompose join_condition: distribute the right keys to right shuffle task
        self.left_key = self.join_condition.left_side
        self.right_key = self.join_condition.right_side
        left_parent = self.parent_task
        assert type(left_parent) is ShuffleToFileTask
        left_parent.key_column = self.left_key
        right_parent = self.right_side_task
        assert type(right_parent) is ShuffleToFileTask
        right_parent.key_column = self.right_key
        return left_schema + right_schema

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:
        assert not list(self.parent_task.create_jobs(full_task, worker_count))
        for partition in range(SHUFFLE_PARTITIONS):
            yield LoadJoinFilesJob(
                full_task,
                left_shuffle_files=list(
                    self.collect_shuffle_files(
                        self.left_shuffle_stage,
                        worker_count,
                        partition,
                    ),
                ),
                right_shuffle_files=list(
                    self.collect_shuffle_files(
                        self.right_shuffle_stage,
                        worker_count,
                        partition,
                    ),
                ),
            )

    def collect_shuffle_files(
        self,
        stage: int,
        worker_count: int,
        partition: int,
    ) -> Iterable[Path]:
        for worker_from in range(worker_count):
            shuffle_file = SHUFFLE_FOLDER / f"{stage}_{worker_from}_{partition}.bin"
            if not shuffle_file.exists():
                continue
            yield shuffle_file

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f'{indent} Join({self.join_condition}, "{self.how}", left_shuffle: {self.left_shuffle_stage}'
            f", right_shuffle: {self.right_shuffle_stage}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)
        self.right_side_task.explain(lvl + 1)


@dataclass(kw_only=True)
class CountTask(Task):
    group_by_column: Col
    counter: dict[Any, int] = field(default_factory=lambda: Counter())

    @trace("CountTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert self.parent_task.inferred_schema is not None
        group_column = project_column(
            self.group_by_column,
            input_columns,
            self.parent_task.inferred_schema,
        )
        counts = Counter(group_column)
        return (list(counts.keys()), list(counts.values()))

    def generate_zig_code(self, function_name: str) -> str:
        raise NotImplementedError

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        assert self.group_by_column.name in {col_name for col_name, _ in schema}
        return [
            (self.group_by_column.name, self.group_by_column.infer_type(schema)),
            ("count", ColumnType.INTEGER),
        ]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(f"{indent} Count():{nice_schema(self.inferred_schema)}")  # noqa: T201
        self.parent_task.explain(lvl + 1)


@dataclass
class ShuffleToFileTask(Task):
    key_column: Col | None = None

    @trace("ShuffleToFileTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:
        assert self.key_column is not None
        assert self.parent_task.inferred_schema is not None
        key_column = project_column(
            self.key_column,
            input_columns,
            self.parent_task.inferred_schema,
        )
        final_output: tuple[list[list[Any]], ...] = tuple([] for _ in range(SHUFFLE_PARTITIONS))
        for col in input_columns:
            col_buckets: tuple[list[Any], ...] = tuple([] for _ in range(SHUFFLE_PARTITIONS))
            for val, key in zip(col, key_column, strict=True):
                destination = hash(key) % SHUFFLE_PARTITIONS
                col_buckets[destination].append(val)
            for shuffle, col_bucket in zip(final_output, col_buckets, strict=True):
                shuffle.append(col_bucket)
        for partition, full_data in enumerate(final_output):
            if len(full_data[0]) == 0:
                continue
            shuffle_file = Path(
                SHUFFLE_FOLDER / f"{job.current_stage}_{job.worker_id}_{partition}.bin",
            )
            data_in_rows = list(zip(*full_data, strict=True))
            BlockFile(shuffle_file).append_tuples(
                data_in_rows,
                self.parent_task.inferred_schema,
            )
        return ()

    def generate_zig_code(self, function_name: str) -> str:
        raise NotImplementedError

    def validate_schema(self) -> Schema:
        schema = self.parent_task.validate_schema()
        if self.key_column is None:
            return schema
        referenced_column_names = [col.name for col in self.key_column.all_nested_columns if type(col) is Col]
        schema_cols = {col_name for col_name, _ in schema}
        unknown_cols = [col for col in referenced_column_names if col not in schema_cols]
        if unknown_cols:
            raise ValueError(f"Unknown columns in GroupBy: {unknown_cols}")
        if self.key_column.name in schema_cols:
            return schema
        return [(self.key_column.name, self.key_column.infer_type(schema)), *schema]

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} ShuffleToFile({self.key_column}):{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass(kw_only=True)
class WriteToLocalFileTask(Task):
    file_path: Path

    @trace("WriteToLocalFileTask")
    def execute(self, input_columns: Columns, job: Job) -> Columns:  # noqa: ARG002
        assert self.parent_task.inferred_schema is not None
        if len(input_columns) == 0 or len(input_columns[0]) == 0:
            return ()
        BlockFile(self.file_path, schema=self.parent_task.inferred_schema).append_data(input_columns)
        return ()

    def generate_zig_code(self, function_name: str) -> str:
        # TODO(david): should be append, not write
        return f"""
            pub fn {function_name}(
                allocator: std.mem.Allocator,
                input: []const ColumnData,
                job: Job,
                schema: []const ColumnSchema) ![]const ColumnData {{
                try Executor.GLOBAL_TRACER.startEvent("{function_name}");
                var block_file = try Executor.BlockFile.init(allocator, schema);
                const block = Executor.Block{{ .cols = input }};
                try block_file.writeData(job.output_file, block);
                try Executor.GLOBAL_TRACER.endEvent("{function_name}");
                return (&[_]ColumnData{{}})[0..];
            }}
        """

    def explain(self, lvl: int = 0) -> None:
        indent = "  " * lvl + ("+- " if lvl > 0 else "")
        print(  # noqa: T201
            f"{indent} WriteToLocalFileTask():{nice_schema(self.inferred_schema)}",
        )
        self.parent_task.explain(lvl + 1)


@dataclass
class VoidTask(Task):
    parent_task: Task | None = None  # type:ignore[assignment]

    def execute(self, input_columns: Columns, job: Job) -> Columns:
        raise NotImplementedError

    def generate_zig_code(self, function_name: str) -> str:
        raise NotImplementedError

    def validate_schema(self) -> Schema:
        return []

    def create_jobs(self, full_task: Task, worker_count: int) -> Iterable[Job]:  # noqa: ARG002
        return []

    def explain(self, lvl: int = 0) -> None:
        pass
