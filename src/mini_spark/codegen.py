import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from jinja2 import Environment

from .constants import Schema
from .plan import PhysicalPlan, Stage
from .sql import Col
from .tasks import (
    ConsumerTask,
    FilterTask,
    LoadTableBlockTask,
    ProducerTask,
    ProjectTask,
    WriterTask,
    WriteToLocalFileTask,
)
from .utils import trace

STAGES_FILE = Path("zig-src/src/stage.zig")
STAGES_BINARY_OUTPUT = Path("zig-src/zig-out/bin/executor")


class CompileError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


@dataclass
class JinjaColumnReference:
    name: str
    pos: int
    type: str
    struct_type: str


class JinjaColumn:
    def __init__(self, column: Col, function_name: str, input_schema: Schema, *, is_condition: bool = False) -> None:
        self.function_name = function_name
        if not is_condition:
            output_type = column.infer_type(input_schema)
            self.zig_type = output_type.native_zig_type
            self.struct_type = output_type.zig_type
        referenced_columns = {col.name for col in column.all_nested_columns}
        self.references = [
            JinjaColumnReference(col_name, col_pos, col_type.native_zig_type, col_type.zig_type)
            for col_pos, (col_name, col_type) in enumerate(input_schema)
            if col_name in referenced_columns
        ]
        self.column_names = ",".join(f"col_{ref.name}" for ref in self.references)
        self.names = ",".join(ref.name for ref in self.references)
        self.zig_code = column.zig_code_representation(input_schema)


class JinjaProducer:
    def __init__(self, producer: ProducerTask, function_name: str) -> None:
        self.function_name = function_name
        self.is_load_table_block = type(producer) is LoadTableBlockTask

    def get_projection_columns(self) -> Iterable[JinjaColumn]:
        return []


class JinjaConsumer:
    def __init__(self, consumer: ConsumerTask, function_name: str) -> None:
        self.function_name = function_name
        self.is_select = type(consumer) is ProjectTask
        self.is_filter = type(consumer) is FilterTask
        self.projection_columns = []
        self.condition_columns = []
        assert consumer.parent_task.inferred_schema is not None
        if self.is_select:
            assert type(consumer) is ProjectTask
            self.input_columns = len(consumer.columns)
            self.projection_columns = [
                JinjaColumn(
                    column,
                    f"{function_name}_project_{i}",
                    consumer.parent_task.inferred_schema,
                )
                for i, column in enumerate(consumer.columns)
            ]
            self.columns = self.projection_columns
        if self.is_filter:
            assert type(consumer) is FilterTask
            self.condition = JinjaColumn(
                consumer.condition,
                f"{function_name}_condition",
                consumer.parent_task.inferred_schema,
                is_condition=True,
            )
            self.condition_columns = [self.condition]

    def get_projection_columns(self) -> Iterable[JinjaColumn]:
        yield from self.projection_columns

    def get_condition_columns(self) -> Iterable[JinjaColumn]:
        yield from self.condition_columns


class JinjaWriter:
    def __init__(self, writer: WriterTask, function_name: str) -> None:
        self.function_name = function_name
        self.is_write_local_file = type(writer) is WriteToLocalFileTask
        assert writer.inferred_schema is not None
        self.output_schema = writer.inferred_schema

    def get_projection_columns(self) -> Iterable[JinjaColumn]:
        return []


class JinjaStage:
    def __init__(self, stage: Stage) -> None:
        nice_stage_id = f"{int(stage.stage_id):>02}"
        self.function_name = f"run_stage_{nice_stage_id}"
        self.name = f"stage_{nice_stage_id}"
        self.id = stage.stage_id
        producer_function_name = f"stage_{nice_stage_id}_{stage.producer.__class__.__name__}"
        self.producer = JinjaProducer(stage.producer, producer_function_name)
        self.consumers = [
            JinjaConsumer(consumer, f"stage_{nice_stage_id}_{i}_{consumer.__class__.__name__}")
            for i, consumer in enumerate(stage.consumers)
        ]
        writer_function_name = f"stage_{nice_stage_id}_{stage.writer.__class__.__name__}"
        self.writer = JinjaWriter(stage.writer, writer_function_name)

    def get_projection_columns(self) -> Iterable[JinjaColumn]:
        yield from self.producer.get_projection_columns()
        for consumer in self.consumers:
            yield from consumer.get_projection_columns()
        yield from self.writer.get_projection_columns()

    def get_condition_columns(self) -> Iterable[JinjaColumn]:
        for consumer in self.consumers:
            yield from consumer.get_condition_columns()


class JinjaPlan:
    def __init__(self, plan: PhysicalPlan) -> None:
        self.stages = [JinjaStage(stage) for stage in plan.stages]
        self.projection_columns = [projection for stage in self.stages for projection in stage.get_projection_columns()]
        self.condition_columns = [condition for stage in self.stages for condition in stage.get_condition_columns()]


@trace("compile stages")
def compile_plan(physical_plan: PhysicalPlan) -> Path:
    template_str = resources.read_text("mini_spark.templates", "plan.zig")
    template = Environment().from_string(template_str)  # noqa: S701
    final_code = template.render(plan=JinjaPlan(physical_plan))
    with STAGES_FILE.open("w", encoding="utf-8") as f:
        f.write(final_code)
    result = subprocess.run(
        ["zig", "build", "-Doptimize=ReleaseFast"],  # noqa: S607
        cwd="zig-src",
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stderr:
        raise CompileError(result.stderr)
    return STAGES_BINARY_OUTPUT
