const std = @import("std");
const Executor = @import("executor");
const ColumnData = @import("executor").ColumnData;
const ColumnSchema = @import("executor").ColumnSchema;
const Job = @import("executor").Job;
const Tracer = @import("executor").GLOBAL_TRACER;


{% for col in plan.condition_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData, output: []bool) void {
    _ = allocator;
    {%- for ref in col.references %}
    const col_{{ref.name}} = input[{{ref.pos}}].{{ref.struct_type}};
    {%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        output[_idx] = {{col.zig_code}};
    }
}
{%- endfor %}

{% for col in plan.projection_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData, output: []{{col.zig_type}}) !ColumnData {
    _ = allocator;
    {%- for ref in col.references %}
    const col_{{ref.name}} = input[{{ref.pos}}].{{ref.struct_type}};
    {%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        output[_idx] = {{col.zig_code}};
    }
    const const_out = output;
    return ColumnData{ .{{col.struct_type}} = const_out };
}
{%- endfor %}

{% for stage in plan.stages %}

    {%- if stage.producer.is_load_table_block -%}
pub fn {{stage.producer.function_name}}(allocator: std.mem.Allocator, job: Job) ![]const ColumnData {
    try Executor.GLOBAL_TRACER.startEvent("{{stage.producer.function_name}}");
    const block_file = try Executor.BlockFile.initFromFile(allocator, job.input_file);
    const data = try block_file.readBlock(job.input_block_id);
    try Executor.GLOBAL_TRACER.endEvent("{{stage.producer.function_name}}");
    return data;
}
    {%- endif -%}

    {%- for consumer in stage.consumers -%}

        {%- if consumer.is_select %}
pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData) ![]const ColumnData {
    try Executor.GLOBAL_TRACER.startEvent("{{consumer.function_name}}");
    const rows = input[0].len();
    const slice: []ColumnData = try allocator.alloc(ColumnData, {{consumer.input_columns}});

        {%- for col in consumer.columns %}
    const col_{{ loop.index0 }} = try allocator.alloc({{col.zig_type}}, rows);
    slice[{{ loop.index0 }}] = try {{col.function_name}}(allocator, input, col_{{ loop.index0}});
        {%- endfor %}

    try Executor.GLOBAL_TRACER.endEvent("{{consumer.function_name}}");
    return slice;
}

        {%- endif %}

        {%- if consumer.is_filter %}
pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData) ![]const ColumnData {
    try Executor.GLOBAL_TRACER.startEvent("{{consumer.condition.function_name}}");
    const rows = input[0].len();
    const condition_col = try allocator.alloc(bool, rows);
    {{consumer.condition.function_name}}(allocator, input, condition_col);
    var output_rows: usize = 0;
    for (condition_col) |c| {
        if (c) output_rows += 1;
    }
    const slice: []ColumnData = try allocator.alloc(ColumnData, input.len);
    for (input, 0..) |col, col_idx| {
        const filtered_column = try Executor.filter_column(col, condition_col, output_rows, allocator);
        slice[col_idx] = filtered_column;
    }
    try Executor.GLOBAL_TRACER.endEvent("{{consumer.condition.function_name}}");
    return slice;
}
        {%- endif %}

    {%- endfor %}

    {%- if stage.writer.is_write_local_file %}
pub fn {{stage.writer.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData, job: Job, schema: []const ColumnSchema) !void {
    try Executor.GLOBAL_TRACER.startEvent("{{stage.writer.function_name}}");
    var block_file = try Executor.BlockFile.init(allocator, schema);
    const block = Executor.Block{ .cols = input };
    try block_file.writeData(job.output_file, block);
    try Executor.GLOBAL_TRACER.endEvent("{{stage.writer.function_name}}");
}
    {%- endif -%}
{%- endfor %}

{% for stage in plan.stages %}
pub fn {{ stage.function_name }}(allocator: std.mem.Allocator, job: Job) !void {
    try Executor.GLOBAL_TRACER.startEvent("stage_{{stage.id}}");

    const last_input_0 = try {{stage.producer.function_name}}(allocator, job);
    {%- for consumer in stage.consumers %}
    const last_input_{{ loop.index0+1 }} = try {{consumer.function_name}}(allocator, last_input_{{ loop.index0 }});
    {%- endfor %}
    const output_schema = (&[_]ColumnSchema{
        {%- for name, column in stage.writer.output_schema -%}
        .{ .typ = Executor.TYPE_{{column.zig_type.upper()}}, .name = "{{name}}" },
        {%- endfor -%}
    })[0..];
    const last_input_{{ (stage.consumers | length)+1 }} = try {{stage.writer.function_name}}(allocator, last_input_{{ (stage.consumers | length) }}, job, output_schema);
    _ = last_input_{{ (stage.consumers | length)+1 }};
    try Executor.GLOBAL_TRACER.endEvent("stage_{{stage.id}}");
}
{%- endfor %}

pub const STAGES: []const *const fn (std.mem.Allocator, Job) anyerror!void = &.{
    {%- for stage in plan.stages %}
    {{ stage.function_name }},
    {%- endfor %}
};
