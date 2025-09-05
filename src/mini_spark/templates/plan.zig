const std = @import("std");
const Executor = @import("executor");
const ColumnData = @import("executor").ColumnData;
const ColumnSchema = @import("executor").ColumnSchema;
const Job = @import("executor").Job;
const Tracer = @import("executor").GLOBAL_TRACER;
const OutputFile = @import("executor").OutputFile;
const PARTITIONS = Executor.PARTITIONS;

// ###### Hash columns
//{% for col in plan.hash_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData) ![]u8 {
    const rows = input[0].len();
    const partition_per_row = try allocator.alloc(u8, rows);
    //{%- for ref in col.references %}
    const col_{{ref.name}} = input[{{ref.pos}}].{{ref.struct_type}};
    //{%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        //{%- if col.struct_type == 'Str' %}
        const value_to_hash: []const u8 = {{col.zig_code}};
        //{%- else %}
        const value_to_hash: []const u8 = @ptrCast(&({{col.zig_code}})[0..@sizeOf({{col.zig_type}})]);
        //{%- endif %}
        partition_per_row[_idx] = @intCast(std.hash.Murmur3_32.hash(value_to_hash) & 0xF);
    }
    return partition_per_row;
}
//{% endfor %}

// ###### Condition columns
//{% for col in plan.condition_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData, output: []bool) void {
    _ = allocator;
    //{%- for ref in col.references %}
    const col_{{ref.name}} = input[{{ref.pos}}].{{ref.struct_type}};
    //{%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        output[_idx] = {{col.zig_code}};
    }
}
//{%- endfor %}

// ###### Projection columns
//{% for col in plan.projection_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, input: []const ColumnData, output: []{{col.zig_type}}) !ColumnData {
    _ = allocator;
    //{%- for ref in col.references %}
    const col_{{ref.name}} = input[{{ref.pos}}].{{ref.struct_type}};
    //{%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        output[_idx] = {{col.zig_code}};
    }
    const const_out = output;
    return ColumnData{ .{{col.struct_type}} = const_out };
}
//{%- endfor %}

//{% for stage in plan.stages %}
// ###### Stage {{stage.id}} task functions

    // ###### Consumers
    //{%- for consumer in stage.consumers -%}

        //{%- if consumer.is_select %}
        pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
            const column_data = input.chunk orelse return .{ .is_last = input.is_last };
            try Executor.GLOBAL_TRACER.startEvent("{{consumer.function_name}}");
            const rows = column_data[0].len();
            const slice: []ColumnData = try allocator.alloc(ColumnData, {{consumer.input_columns}});

            //{%- for col in consumer.columns %}
            const col_{{ loop.index0 }} = try allocator.alloc({{col.zig_type}}, rows);
            slice[{{ loop.index0 }}] = try {{col.function_name}}(allocator, column_data, col_{{ loop.index0}});
            //{%- endfor %}

            try Executor.GLOBAL_TRACER.endEvent("{{consumer.function_name}}");
            return .{ .chunk = slice, .is_last = input.is_last };
        }

        //{%- endif %}

        //{%- if consumer.is_filter %}
        pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
            const column_data = input.chunk orelse return .{ .is_last = input.is_last };
            try Executor.GLOBAL_TRACER.startEvent("{{consumer.condition.function_name}}");
            const rows = column_data[0].len();
            const condition_col = try allocator.alloc(bool, rows);
            {{consumer.condition.function_name}}(allocator, column_data, condition_col);
            var output_rows: usize = 0;
            for (condition_col) |c| {
                if (c) output_rows += 1;
            }
            const slice: []ColumnData = try allocator.alloc(ColumnData, column_data.len);
            for (column_data, 0..) |col, col_idx| {
                const filtered_column = try Executor.filter_column(col, condition_col, output_rows, allocator);
                slice[col_idx] = filtered_column;
            }
            try Executor.GLOBAL_TRACER.endEvent("{{consumer.condition.function_name}}");
            return .{ .chunk = slice, .is_last = input.is_last };
        }
        //{%- endif %}


        //{%- if consumer.is_count_aggregate %}
        pub const {{consumer.class_name}} = struct {
            allocator: std.mem.Allocator,
            counts: std.StringHashMap(u32),

            pub fn init(allocator: std.mem.Allocator) !{{consumer.class_name}} {
                return {{consumer.class_name}}{
                    .allocator = allocator,
                    .counts = std.StringHashMap(u32).init(allocator),
                };
            }

            pub fn next(self: *{{consumer.class_name}}, allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
                if (input.is_last and input.chunk == null) {
                    try Executor.GLOBAL_TRACER.startEvent("{{consumer.class_name}}-emit");
                    const column_counts = try allocator.alloc(i32, self.counts.count());
                    const column_key = try allocator.alloc({{consumer.group_column.zig_type}}, self.counts.count());
                    var it = self.counts.iterator();
                    var idx: usize = 0;
                    while (it.next()) |entry| {
                        column_key[idx] = entry.key_ptr.*;
                        column_counts[idx] = @intCast(entry.value_ptr.*);
                        idx += 1;
                    }
                    const chunk: []ColumnData = try allocator.alloc(ColumnData, 2);
                    chunk[0] = ColumnData{ .{{consumer.group_column.struct_type}} = column_key };
                    chunk[1] = ColumnData{ .I32 = column_counts };
                    try Executor.GLOBAL_TRACER.endEvent("{{consumer.class_name}}-emit");
                    return .{ .chunk = chunk, .is_last = true };
                }
                try Executor.GLOBAL_TRACER.startEvent("{{consumer.class_name}}-agg");
                const column_data = input.chunk orelse return .{ .is_last = input.is_last };
                //{%- for ref in consumer.group_column.references %}
                const col_{{ref.name}} = column_data[{{ref.pos}}].{{ref.struct_type}};
                //{%- endfor %}
                //{%- if consumer.in_sum_mode %}
                const count_column = column_data[1].I32;
                for ({{consumer.group_column.column_names}}, count_column) |{{consumer.group_column.names}}, prev_count| {
                    const key = {{consumer.group_column.zig_code}};
                    const pre:u32 = @intCast(prev_count);
                    const existing = try self.counts.getOrPut(key);
                    if (existing.found_existing) {
                        existing.value_ptr.* += pre;
                    } else {
                        existing.value_ptr.* = pre;
                    }
                }
                //{%- else %}
                for ({{consumer.group_column.column_names}}) |{{consumer.group_column.names}}| {
                    const key = {{consumer.group_column.zig_code}};
                    const existing = try self.counts.getOrPut(key);
                    if (existing.found_existing) {
                        existing.value_ptr.* += 1;
                    } else {
                        existing.value_ptr.* = 1;
                    }
                }
                //{%- endif %}
                try Executor.GLOBAL_TRACER.endEvent("{{consumer.class_name}}-agg");
                return .{ .chunk = null, .is_last = input.is_last };
            }
        };
        //{%- endif %}

    //{%- endfor %}

    // ###### Writers
    //{%- if stage.writer.is_write_local_file %}
    pub fn {{stage.writer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult, job: Job, schema: []const ColumnSchema, output_files: *std.ArrayList(OutputFile)) !void {
        const column_data = input.chunk orelse return;
        try Executor.GLOBAL_TRACER.startEvent("{{stage.writer.function_name}}");
        var block_file = try Executor.BlockFile.init(allocator, schema);
        const block = Executor.Block{ .cols = column_data };
        try block_file.writeData(job.output_file, block);
        try Executor.GLOBAL_TRACER.endEvent("{{stage.writer.function_name}}");
        const output_file_obj: OutputFile = .{ .file_path = job.output_file, .partition_id = 0 };
        try output_files.append(allocator, output_file_obj);
    }
    //{%- endif %}

    //{%- if stage.writer.is_write_shuffle %}
    pub fn {{stage.writer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult, job: Job, schema: []const ColumnSchema, output_files: *std.ArrayList(OutputFile)) !void {
        const column_data = input.chunk orelse return;
        try Executor.GLOBAL_TRACER.startEvent("{{stage.writer.function_name}}");
        try Executor.GLOBAL_TRACER.startEvent("parition_per_row");
        const partition_per_row = try {{stage.writer.hash_column.function_name}}(allocator, column_data);
        var bucket_sizes: [PARTITIONS]u32 = .{0} ** PARTITIONS;
        for (partition_per_row) |partition| {
            bucket_sizes[partition] += 1;
        }
        try Executor.GLOBAL_TRACER.endEvent("parition_per_row");
        //{% for name, column in stage.writer.output_schema %}
        try Executor.GLOBAL_TRACER.startEvent("fill_bucket {{name}}");
        const col_{{loop.index0}}_buckets = try Executor.fill_buckets(
            {{column.native_zig_type}},
            allocator,
            column_data[{{loop.index0}}].{{column.zig_type}},
            bucket_sizes,
            partition_per_row,);
        try Executor.GLOBAL_TRACER.endEvent("fill_bucket {{name}}");
        //{%- endfor %}

        for (0..PARTITIONS) |i| {
            if (col_0_buckets[i].items.len == 0)
                continue;
            var block_file = try Executor.BlockFile.init(allocator, schema);
            const block = Executor.Block{
                .cols = (&[_]ColumnData{
                    //{% for name, column in stage.writer.output_schema %}
                    .{ .{{column.zig_type}} = col_{{loop.index0}}_buckets[i].items },
                    //{%- endfor %}
                }),
            };
            var buffer: [128]u8 = undefined;
            try Executor.GLOBAL_TRACER.startEvent("write partition");
            const output_file = try std.fmt.bufPrint(&buffer, "{s}_{d}", .{ job.output_file, i });
            try block_file.writeData(output_file, block);
            const output_file_obj: OutputFile = .{ .file_path = try allocator.dupe(u8, output_file), .partition_id = @intCast(i) };
            try output_files.append(allocator, output_file_obj);
            try Executor.GLOBAL_TRACER.endEvent("write partition");
        }
        try Executor.GLOBAL_TRACER.endEvent("{{stage.writer.function_name}}");
    }
    //{%- endif -%}

//{%- endfor %}

// ###### Stage functions
//{% for stage in plan.stages %}
pub fn {{ stage.function_name }}(allocator: std.mem.Allocator, job: Job) !void {
    try Executor.GLOBAL_TRACER.startEvent("stage_{{stage.id}}");

    //{%- if stage.producer.is_load_table_block %}
    var producer = try Executor.LoadTableBlockProducer.init(allocator, job.input_file, job.input_block_id);
    //{%- elif stage.producer.is_load_shuffles %}
    var producer = try Executor.LoadShuffleFilesProducer.init(allocator, job.shuffle_files);
    //{%- endif %}
    //{%- for consumer in stage.consumers %}
        //{%- if consumer.is_count_aggregate %}
    var {{consumer.object_name}} = try {{consumer.class_name}}.init(allocator);
        //{%- endif %}
    //{%- endfor %}
    const output_schema = (&[_]ColumnSchema{
        //{% for name, column in stage.writer.output_schema -%}
        .{ .typ = Executor.TYPE_{{column.zig_type.upper()}}, .name = "{{name}}" },
        //{% endfor -%}
    })[0..];

    var last_output: Executor.TaskResult = .{.chunk= undefined, .is_last=false};
    var output_files = try std.ArrayList(OutputFile).initCapacity(allocator, PARTITIONS);
    while (true){
        const last_input_0 = try producer.next();
        //{%- for consumer in stage.consumers %}
        const last_input_{{ loop.index0+1 }} = try {{consumer.function_name}}(allocator, last_input_{{ loop.index0 }});
        //{%- endfor %}
        last_output = last_input_{{ (stage.consumers | length) }};
        try {{stage.writer.function_name}}(allocator, last_output, job, output_schema, &output_files);
        if (last_output.is_last) break;
    }
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    for (output_files.items) |file| {
        try stdout.print("{s} {d}\n", .{file.file_path, file.partition_id});
    }
    try stdout.flush();
    try Executor.GLOBAL_TRACER.endEvent("stage_{{stage.id}}");
}
//{% endfor %}

pub const STAGES: []const *const fn (std.mem.Allocator, Job) anyerror!void = &.{
    //{%- for stage in plan.stages %}
    {{ stage.function_name }},
    //{%- endfor %}
};
