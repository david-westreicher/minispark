import subprocess
from pathlib import Path

from .constants import ColumnType, Schema
from .tasks import Task

ZIG_TYPE = {
    ColumnType.INTEGER: "I32",
    ColumnType.STRING: "STR",
}

STAGES_FILE = Path("zig-src/src/stage.zig")


class CompileError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


def compile_stages(stages: list[Task]) -> None:
    code_buffer = [
        'const std = @import("std");',
        'const Executor = @import("executor");',
        'const ColumnData = @import("executor").ColumnData;',
        'const ColumnSchema = @import("executor").ColumnSchema;',
        'const Job = @import("executor").Job;',
    ]
    for stage_num, stage in enumerate(stages):
        code_buffer.extend(compile_stage(list(stage.task_chain), stage_num))

    def create_stages_array() -> list[str]:
        return [
            "pub const STAGES: []const *const fn (std.mem.Allocator, Job) anyerror!void = &.{",
            *[f"run_stage_{stage_num:0>2}," for stage_num in range(len(stages))],
            "};",
        ]

    code_buffer.extend(create_stages_array())
    final_code = "\n".join(code_buffer)
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


def compile_stage(task_chain: list[Task], stage_num: int) -> list[str]:
    generated_code = []
    stage_function_names = []
    schema_codes = []

    def generate_schema_code(schema: Schema) -> str:
        schema_code = [
            "(&[_]ColumnSchema{",
            *[
                f'.{{ .typ = Executor.TYPE_{ZIG_TYPE[col_type]}, .name = "{col_name}" }},'
                for col_name, col_type in schema
            ],
            "})[0..];",
        ]
        return "\n".join(schema_code)

    for task_num, task in enumerate(task_chain):
        function_name = f"stage_{stage_num}_task_{task_num}_{task.__class__.__name__}"
        function_code = task.generate_zig_code(function_name)
        generated_code.append(function_code)
        stage_function_names.append(function_name)
        assert task.inferred_schema is not None
        schema_codes.append(generate_schema_code(task.inferred_schema))

    def generate_task_calls() -> str:
        generated_code = []
        func_num = 0
        for func_num, func_name in enumerate(stage_function_names):
            generated_code.extend(
                [
                    f"const schema_{func_num:0>2} = {schema_codes[func_num]}",
                    f"const last_input_{func_num + 1:0>2} = try {func_name}("
                    f"allocator, last_input_{func_num:0>2}, job, schema_{func_num:0>2});",
                ]
            )

        generated_code.append(f"_ = last_input_{func_num + 1:0>2};")
        return "\n".join(generated_code)

    run_stage_code = f"""
        pub fn run_stage_{stage_num:0>2}(allocator: std.mem.Allocator, job: Job) !void {{
            const last_input_00 = &[_]ColumnData{{}};
            {generate_task_calls()}
        }}
    """
    generated_code.append(run_stage_code)
    return generated_code
