const std = @import("std");
const Regex = @import("regex").Regex;
const Executor = @import("executor");
const ColumnData = @import("executor").ColumnData;
const ColumnSchema = @import("executor").ColumnSchema;
const Block = @import("executor").Block;
const Schema = @import("executor").Schema;
const StringColumn = @import("executor").StringColumn;
const concatStrings = @import("executor").concatStrings;
const Job = @import("executor").Job;
const Tracer = @import("executor").GLOBAL_TRACER;
const OutputFile = @import("executor").OutputFile;
const PARTITIONS = Executor.PARTITIONS;

// ###### Map Entries
//{%- for stage in plan.stages %}
    //{%- for consumer in stage.consumers -%}
        //{%- if consumer.is_aggregate %}
            pub const {{consumer.entry_class_name}} = struct {
                key_0: {{consumer.group_column.zig_type}},
                //{%- for ref in consumer.agg_columns %}
                agg_{{loop.index0}}: {{ref.zig_type}},
                //{%- endfor %}
            };
        //{%- endif %}
    //{%- endfor %}
//{%- endfor %}

// ###### Hash columns
//{% for col in plan.hash_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, block: Block) ![]u8 {
    const rows = block.rows();
    const partition_per_row = try allocator.alloc(u8, rows);
    //{%- for ref in col.references %}
    const col_{{ref.name}} = block.cols[{{ref.pos}}].{{ref.struct_type}};
    //{%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        //{%- if col.struct_type == 'Str' %}
        const value_to_hash: []const u8 = {{col.zig_code}};
        //{%- else %}
        const value = {{col.zig_code}};
        const value_to_hash: []const u8 = std.mem.asBytes(&value);
        //{%- endif %}
        partition_per_row[_idx] = @intCast(std.hash.Murmur3_32.hash(value_to_hash) & 0xF);
    }
    return partition_per_row;
}
//{% endfor %}

// ###### Condition columns
//{% for col in plan.condition_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, block: Block, output: []bool) !void {
    //{%- if not col.like_columns %}
    _ = allocator;
    //{%- endif %}
    //{%- for col in col.like_columns %}
    var re = try Regex.compile(allocator, "{{col.regex}}");
    //{%- endfor %}
    //{%- for ref in col.references %}
    const col_{{ref.name}} = block.cols[{{ref.pos}}].{{ref.struct_type}};
    //{%- endfor %}
    for ({{col.column_names}}, 0..) |{{col.names}}, _idx| {
        output[_idx] = {{col.zig_code}};
    }
}
//{%- endfor %}

// ###### Projection columns
//{% for col in plan.projection_columns %}
pub fn {{col.function_name}}(allocator: std.mem.Allocator, block: Block, rows: usize) !ColumnData {
    //{%- if col.is_direct_column %}
        _ = allocator;
        _ = rows;
        return ColumnData{ .{{col.struct_type}} = block.cols[{{col.direct_column_pos}}].{{col.struct_type}} };
    //{%- else %}
        const output_buffer = try allocator.alloc({{col.zig_type}}, rows);
        //{%- if not col.column_names %}
        _ = block;
        //{%- endif %}
        //{%- for ref in col.references %}
        const col_{{ref.name}} = block.cols[{{ref.pos}}].{{ref.struct_type}};
        //{%- endfor %}
        for ({{col.column_names + ("," if col.column_names else "")}} 0..rows) |{{col.names + ("," if col.column_names else "")}} _idx| {
            output_buffer[_idx] = {{col.zig_code}};
        }
        const const_out = output_buffer;
        //{%- if col.struct_type == 'Str' %}
        return ColumnData{ .{{col.struct_type}} = try StringColumn.init(allocator, const_out) };
        //{%- else %}
        return ColumnData{ .{{col.struct_type}} = const_out };
        //{%- endif %}
    //{%- endif %}
}
//{%- endfor %}

//{% for stage in plan.stages %}
// ###### Stage {{stage.id}} task functions

    // ###### Consumers
    //{%- for consumer in stage.consumers -%}

        //{%- if consumer.is_select %}
        pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
            const block = input.chunk orelse return .{ .is_last = input.is_last };
            try Executor.GLOBAL_TRACER.startEvent("{{consumer.function_name}}");
            const rows = block.rows();
            const slice: []ColumnData = try allocator.alloc(ColumnData, {{consumer.input_columns}});

            //{%- for col in consumer.columns %}
            slice[{{ loop.index0 }}] = try {{col.function_name}}(allocator, block, rows);
            //{%- endfor %}

            try Executor.GLOBAL_TRACER.endEvent("{{consumer.function_name}}");
            return .{ .chunk = Block {.cols=slice}, .is_last = input.is_last };
        }

        //{%- endif %}

        //{%- if consumer.is_filter %}
        pub fn {{consumer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
            const block = input.chunk orelse return .{ .is_last = input.is_last };
            try Executor.GLOBAL_TRACER.startEvent("{{consumer.condition.function_name}}");
            const rows = block.rows();
            const condition_col = try allocator.alloc(bool, rows);
            _ = try {{consumer.condition.function_name}}(allocator, block, condition_col);
            var output_rows: usize = 0;
            for (condition_col) |c| {
                if (c) output_rows += 1;
            }
            const slice: []ColumnData = try allocator.alloc(ColumnData, block.cols.len);
            for (block.cols, 0..) |col, col_idx| {
                const filtered_column = try Executor.filter_column(col, condition_col, output_rows, allocator);
                slice[col_idx] = filtered_column;
            }
            try Executor.GLOBAL_TRACER.endEvent("{{consumer.condition.function_name}}");
            return .{ .chunk = Block{.cols=slice}, .is_last = input.is_last };
        }
        //{%- endif %}

        //{%- if consumer.is_aggregate %}
        pub const {{consumer.class_name}} = struct {
            allocator: std.mem.Allocator,
            //{%- if consumer.group_column.struct_type == 'Str' %}
            aggregator: std.StringHashMap({{consumer.entry_class_name}}),
            //{%- else %}
            aggregator: std.AutoHashMap({{consumer.group_column.zig_type}}, {{consumer.entry_class_name}}),
            //{%- endif %}

            pub fn init(allocator: std.mem.Allocator) !{{consumer.class_name}} {
                return {{consumer.class_name}}{
                    .allocator = allocator,
                    //{%- if consumer.group_column.struct_type == 'Str' %}
                    .aggregator = std.StringHashMap({{consumer.entry_class_name}}).init(allocator),
                    //{%- else %}
                    .aggregator = std.AutoHashMap({{consumer.group_column.zig_type}}, {{consumer.entry_class_name}}).init(allocator),
                    //{%- endif %}
                };
            }

            pub fn next(self: *{{consumer.class_name}}, allocator: std.mem.Allocator, input: Executor.TaskResult) !Executor.TaskResult {
                if (input.is_last and input.chunk == null) {
                    const row_count = self.aggregator.count();
                    try Executor.GLOBAL_TRACER.startEvent("{{consumer.class_name}}-emit");
                    const column_key = try allocator.alloc({{consumer.group_column.zig_type}}, row_count);
                    //{%- for ref in consumer.agg_columns %}
                    const column_{{loop.index0}} = try allocator.alloc({{ref.zig_type}}, row_count);
                    //{%- endfor %}
                    var it = self.aggregator.iterator();
                    var idx: usize = 0;
                    while (it.next()) |entry| {
                        const agg_entry = entry.value_ptr.*;
                        column_key[idx] = agg_entry.key_0;
                        //{%- for ref in consumer.agg_columns %}
                        column_{{loop.index0}}[idx] = agg_entry.agg_{{loop.index0}};
                        //{%- endfor %}
                        idx += 1;
                    }
                    const chunk: []ColumnData = try allocator.alloc(ColumnData, {{(consumer.agg_columns | length) + 1}});
                    //{%- if consumer.group_column.struct_type == 'Str' %}
                    chunk[0] = ColumnData{ .Str = try StringColumn.init(allocator, column_key) };
                    //{%- else %}
                    chunk[0] = ColumnData{ .{{consumer.group_column.struct_type}} = column_key };
                    //{%- endif %}
                    //{%- for ref in consumer.agg_columns %}
                    chunk[{{loop.index0 + 1}}] = ColumnData{ .{{ref.struct_type}} = column_{{loop.index0}} };
                    //{%- endfor %}
                    try Executor.GLOBAL_TRACER.endEvent("{{consumer.class_name}}-emit");
                    return .{ .chunk = Block{ .cols = chunk }, .is_last = true };
                }
                try Executor.GLOBAL_TRACER.startEvent("{{consumer.class_name}}-agg");
                const block = input.chunk orelse return .{ .is_last = input.is_last };
                //{%- if consumer.before_shuffle %}
                const rows = block.rows();
                    //{%- if consumer.group_column.struct_type == 'Str' %}
                const col_key = (try {{consumer.group_column.function_name}}(allocator, block, rows)).{{consumer.group_column.struct_type}}.slices;
                    //{%- else %}
                const col_key = (try {{consumer.group_column.function_name}}(allocator, block, rows)).{{consumer.group_column.struct_type}};
                    //{%- endif %}
                //{%- else %}
                    //{%- if consumer.group_column.struct_type == 'Str' %}
                const col_key = block.cols[0].{{consumer.group_column.struct_type}}.slices;
                    //{%- else %}
                const col_key = block.cols[0].{{consumer.group_column.struct_type}};
                    //{%- endif %}
                //{%- endif %}
                //{%- for ref in consumer.agg_columns %}
                    //{%- if consumer.before_shuffle %}
                try Executor.GLOBAL_TRACER.startEvent("project {{ref.real_column.name}}");
                const col_{{ loop.index0 }} = (try {{ref.function_name}}(allocator, block, rows)).{{ref.struct_type}};
                try Executor.GLOBAL_TRACER.endEvent("project {{ref.real_column.name}}");
                    //{%- else %}
                const col_{{ loop.index0 }} = block.cols[{{ loop.index0 + 1}}].{{ref.struct_type}};
                    //{%- endif %}
                //{%- endfor %}
                for (col_key, {{ consumer.agg_column_names }}) |key, {{ consumer.agg_column_var_names }}| {
                    const existing = try self.aggregator.getOrPut(key);
                    if (existing.found_existing) {
                        var entry = existing.value_ptr;
                        //{%- for col in consumer.agg_columns %}
                            //{%- if col.real_column.type == 'sum' %}
                        entry.agg_{{loop.index0}} += c{{loop.index0}};
                            //{%- endif %}
                            //{%- if col.real_column.type == 'min' %}
                        entry.agg_{{loop.index0}} = @min(c{{loop.index0}}, entry.agg_{{loop.index0}});
                            //{%- endif %}
                            //{%- if col.real_column.type == 'max' %}
                        entry.agg_{{loop.index0}} = @max(c{{loop.index0}}, entry.agg_{{loop.index0}});
                            //{%- endif %}
                        //{%- endfor %}
                    } else {
                        existing.value_ptr.* = .{
                            .key_0 = key,
                            //{%- for col in consumer.agg_columns %}
                            .agg_{{loop.index0}} = c{{loop.index0}},
                            //{%- endfor %}
                        };
                    }
                }
                try Executor.GLOBAL_TRACER.endEvent("{{consumer.class_name}}-agg");
                return .{ .chunk = null, .is_last = input.is_last };
            }
        };
        //{%- endif %}

    //{%- endfor %}

    // ###### Writers
    //{%- if stage.writer.is_write_local_file %}
    pub fn {{stage.writer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult, job: Job, schema: Schema, output_files: *std.ArrayList(OutputFile)) !void {
        const block = input.chunk orelse return;
        try Executor.GLOBAL_TRACER.startEvent("{{stage.writer.function_name}}");
        var block_file = try Executor.BlockFile.init(allocator, schema,job.output_file);
        try block_file.appendData(allocator, block);
        try Executor.GLOBAL_TRACER.endEvent("{{stage.writer.function_name}}");
        const output_file_obj: OutputFile = .{ .file_path = job.output_file, .partition_id = 0 };
        try output_files.append(allocator, output_file_obj);
    }
    //{%- endif %}

    //{%- if stage.writer.is_write_shuffle %}
    pub fn {{stage.writer.function_name}}(allocator: std.mem.Allocator, input: Executor.TaskResult, job: Job, schema: Schema, output_files: *std.ArrayList(OutputFile)) !void {
        const block = input.chunk orelse return;
        try Executor.GLOBAL_TRACER.startEvent("{{stage.writer.function_name}}");
        try Executor.GLOBAL_TRACER.startEvent("parition_per_row");
        const partition_per_row = try {{stage.writer.hash_column.function_name}}(allocator, block);
        var bucket_sizes: [PARTITIONS]u32 = .{0} ** PARTITIONS;
        for (partition_per_row) |partition| {
            bucket_sizes[partition] += 1;
        }
        try Executor.GLOBAL_TRACER.endEvent("parition_per_row");
        //{% for name, column in stage.writer.output_schema %}
        try Executor.GLOBAL_TRACER.startEvent("fill_bucket {{name}}");
        const col_{{loop.index0}}_buckets = try Executor.fill_buckets_{{column.zig_type}}(
            allocator,
            block.cols[{{loop.index0}}],
            bucket_sizes,
            partition_per_row,);
        try Executor.GLOBAL_TRACER.endEvent("fill_bucket {{name}}");
        //{%- endfor %}

        for (0..PARTITIONS) |i| {
            if (col_0_buckets[i].items.len == 0)
                continue;
            var column_data = ([_]ColumnData{
                //{% for name, column in stage.writer.output_schema %}
                //{%- if column.zig_type == 'Str' %}
                .{ .Str = try StringColumn.init(allocator, col_{{loop.index0}}_buckets[i].items) },
                //{%- else %}
                .{ .{{column.zig_type}} = col_{{loop.index0}}_buckets[i].items },
                //{%- endif %}
                //{%- endfor %}
            });
            const output_block = Executor.Block{.cols = column_data[0..]};
            var buffer: [128]u8 = undefined;
            try Executor.GLOBAL_TRACER.startEvent("write partition");
            const output_file = try std.fmt.bufPrint(&buffer, "{s}_{d}", .{ job.output_file, i });
            var block_file = try Executor.BlockFile.init(allocator, schema, output_file);
            try block_file.appendData(allocator, output_block);
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
    var producer = try Executor.LoadTableBlockProducer.init(allocator, job.input_file orelse @panic("input file not set"), job.input_block_id);
    //{%- elif stage.producer.is_load_shuffles %}
    var producer = try Executor.LoadShuffleFilesProducer.init(allocator, job.shuffle_files orelse @panic("shuffle files not set"));
    //{%- elif stage.producer.is_join %}
    const left_schema = Schema{ .columns = (&[_]ColumnSchema{
        //{%- for col_name, col_type in stage.producer.left_schema %}
        .{ .typ = Executor.TYPE_{{col_type.zig_type.upper()}}, .name = "{{col_name}}" },
        //{%- endfor %}
    })[0..] };
    var producer = try Executor.JoinProducer({{stage.producer.left_key.zig_type}}).init(
        allocator,
        job.shuffle_files orelse @panic("input file not set"),
        {{stage.producer.left_key.function_name}},
        left_schema,
        job.right_shuffle_files orelse @panic("right input file not set"),
        {{stage.producer.right_key.function_name}},
    );
    //{%- endif %}
    //{%- for consumer in stage.consumers %}
        //{%- if consumer.is_aggregate %}
    var {{consumer.object_name}} = try {{consumer.class_name}}.init(allocator);
        //{%- endif %}
    //{%- endfor %}
    const output_schema = Schema{.columns=(&[_]ColumnSchema{
        //{% for name, column in stage.writer.output_schema -%}
        .{ .typ = Executor.TYPE_{{column.zig_type.upper()}}, .name = "{{name}}" },
        //{% endfor -%}
    })[0..]};

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
