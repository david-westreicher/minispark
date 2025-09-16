const std = @import("std");

pub const TYPE_STR = @import("block_file.zig").TYPE_STR;
pub const TYPE_I32 = @import("block_file.zig").TYPE_I32;
pub const TYPE_F32 = @import("block_file.zig").TYPE_F32;
pub const ROWS_PER_BLOCK = @import("block_file.zig").ROWS_PER_BLOCK;
pub const BlockFile = @import("block_file.zig").BlockFile;
pub const Block = @import("block_file.zig").Block;
pub const ColumnData = @import("block_file.zig").ColumnData;
pub const StringColumn = @import("block_file.zig").StringColumn;
pub const ColumnSchema = @import("block_file.zig").ColumnSchema;
pub const Schema = @import("block_file.zig").Schema;

pub const PARTITIONS = 16;

pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
    UnexepctedState,
};

pub const TraceEvent = struct {
    name: []const u8,
    time: u64,
    is_start: bool,
};

pub const Tracer = struct {
    allocator: std.mem.Allocator,
    timer: std.time.Timer,
    events: std.ArrayList(TraceEvent),

    pub fn init(allocator: std.mem.Allocator) !Tracer {
        return Tracer{
            .allocator = allocator,
            .timer = try std.time.Timer.start(),
            .events = try std.ArrayList(TraceEvent).initCapacity(allocator, 100),
        };
    }

    pub fn deinit(self: *Tracer, allocator: std.mem.Allocator) void {
        self.events.deinit(allocator);
    }

    pub fn startEvent(self: *Tracer, name: []const u8) !void {
        std.debug.print("start - {s}\n", .{name});
        try self.events.append(self.allocator, TraceEvent{
            .name = name,
            .time = self.timer.read(),
            .is_start = true,
        });
    }

    pub fn endEvent(self: *Tracer, name: []const u8) !void {
        std.debug.print("end - {s}\n", .{name});
        try self.events.append(self.allocator, TraceEvent{
            .name = name,
            .time = self.timer.read(),
            .is_start = false,
        });
    }

    pub fn save(self: *Tracer, file_path: []const u8) !void {
        const fs = std.fs.cwd();
        var file = try fs.createFile(file_path, .{ .truncate = true });
        defer file.close();

        for (self.events.items) |event| {
            const event_type: u8 = if (event.is_start) 0 else 1;
            try file.writeAll(&[_]u8{event_type});
            try file.writeAll(std.mem.asBytes(&event.time));
            const name_len: u8 = @intCast(event.name.len);
            try file.writeAll(&[_]u8{name_len});
            try file.writeAll(event.name);
        }
        return;
    }
};

pub var GLOBAL_TRACER: Tracer = undefined;

pub const Job = struct {
    stage_id: u32 = 0,
    output_file: []const u8,
    input_file: ?[]const u8 = null,
    input_block_id: u32 = 0,
    shuffle_files: ?[][]const u8 = null,
    right_shuffle_files: ?[][]const u8 = null,

    pub fn fromFile(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, output_path: []const u8) !?Job {
        const stage: u8 = try reader.interface.takeInt(u8, .little);
        if (stage == 255) {
            return null;
        }
        const job_type: u8 = try reader.interface.takeInt(u8, .little);
        var job = Job{
            .stage_id = stage,
            .output_file = try std.fmt.allocPrint(allocator, "{s}_result_{d}.bin", .{ output_path, stage }),
        };
        if (job_type == 0) {
            job.input_file = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            job.input_block_id = try reader.interface.takeInt(u32, .little);
            return job;
        }
        if (job_type == 1) {
            const shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const shuffle_files = try allocator.alloc([]const u8, shuffle_files_num);
            for (0..shuffle_files_num) |i| {
                shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.shuffle_files = shuffle_files;
            return job;
        }
        if (job_type == 2) {
            const shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const shuffle_files = try allocator.alloc([]const u8, shuffle_files_num);
            for (0..shuffle_files_num) |i| {
                shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.shuffle_files = shuffle_files;
            const right_shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const right_shuffle_files = try allocator.alloc([]const u8, right_shuffle_files_num);
            for (0..right_shuffle_files_num) |i| {
                right_shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.right_shuffle_files = right_shuffle_files;
            return job;
        }
        return null;
    }
};

pub fn filter_column(col: ColumnData, condition_col: []const bool, output_rows: usize, allocator: std.mem.Allocator) !ColumnData {
    switch (col) {
        .I32 => |vals| {
            const filtered_vals = try allocator.alloc(i32, output_rows);
            var filter_index: u32 = 0;
            for (vals, condition_col) |input_val, c| {
                if (c) {
                    filtered_vals[filter_index] = input_val;
                    filter_index += 1;
                }
            }
            return ColumnData{ .I32 = filtered_vals };
        },
        .F32 => |vals| {
            const filtered_vals = try allocator.alloc(f32, output_rows);
            var filter_index: u32 = 0;
            for (vals, condition_col) |input_val, c| {
                if (c) {
                    filtered_vals[filter_index] = input_val;
                    filter_index += 1;
                }
            }
            return ColumnData{ .F32 = filtered_vals };
        },
        .Str => |vals| {
            var total_output_len: usize = 0;
            for (vals.slices, condition_col) |string, c| {
                if (c) {
                    total_output_len += string.len;
                }
            }
            var new_total = try allocator.alloc(u8, total_output_len);
            var offset: usize = 0;
            const filtered_vals = try allocator.alloc([]const u8, output_rows);
            var filter_index: u32 = 0;
            for (vals.slices, condition_col) |string, c| {
                if (c) {
                    @memcpy(new_total[offset .. offset + string.len], string);
                    filtered_vals[filter_index] = new_total[offset .. offset + string.len];
                    offset += string.len;
                    filter_index += 1;
                }
            }
            return ColumnData{ .Str = StringColumn{
                .total_buffer = new_total,
                .slices = filtered_vals,
            } };
        },
    }
}

pub fn fill_buckets_F32(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(f32) {
    switch (column) {
        .F32 => |vals| {
            var buckets: [PARTITIONS]std.ArrayList(f32) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList(f32).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
        .I32 => |_| {
            @panic("unexpected type");
        },
        .Str => |_| {
            @panic("unexpected type");
        },
    }
}
pub fn fill_buckets_I32(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(i32) {
    switch (column) {
        .I32 => |vals| {
            var buckets: [PARTITIONS]std.ArrayList(i32) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList(i32).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
        .F32 => |_| {
            @panic("unexpected type");
        },
        .Str => |_| {
            @panic("unexpected type");
        },
    }
}
pub fn fill_buckets_Str(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList([]const u8) {
    switch (column) {
        .I32 => |_| {
            @panic("unexpected type");
        },
        .F32 => |_| {
            @panic("unexpected type");
        },
        .Str => |vals| {
            var buckets: [PARTITIONS]std.ArrayList([]const u8) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList([]const u8).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals.slices) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
    }
}

pub const TaskResult = struct {
    chunk: ?Block = null,
    is_last: bool = false,
};

pub const OutputFile = struct {
    file_path: []const u8,
    partition_id: u8,
};

const AnyList = union(enum) {
    IntList: std.ArrayList(i32),
    FloatList: std.ArrayList(f32),
    StrList: std.ArrayList([]const u8),

    pub fn deinit(self: *AnyList, allocator: std.mem.Allocator) void {
        switch (self.*) {
            inline else => |*list| list.deinit(allocator),
        }
    }
};

pub inline fn concatStrings(allocator: std.mem.Allocator, strings: []const []const u8) ![]u8 {
    var total_len: usize = 0;
    for (strings) |s| {
        total_len += s.len;
    }

    var result = try allocator.alloc(u8, total_len);
    var offset: usize = 0;
    for (strings) |s| {
        @memcpy(result[offset .. offset + s.len], s);
        offset += s.len;
    }
    return result;
}

pub fn JoinProducer(comptime K: type) type {
    return struct {
        const MapType = if (K == []const u8)
            std.StringHashMap(std.ArrayList(u32))
        else
            std.AutoHashMap(K, std.ArrayList(u32));

        allocator: std.mem.Allocator,
        left_shuffle_files: [][]const u8,
        left_projection: *const fn (std.mem.Allocator, Block, usize) error{OutOfMemory}!ColumnData,
        left_schema: Schema,
        right_block_loader: LoadShuffleFilesProducer,
        right_projection: *const fn (std.mem.Allocator, Block, usize) error{OutOfMemory}!ColumnData,
        left_row_map: MapType,
        left_columns: ?[]AnyList = null,
        finished: bool = false,

        pub fn init(
            allocator: std.mem.Allocator,
            left_shuffle_files: [][]const u8,
            left_projection: *const fn (std.mem.Allocator, Block, usize) error{OutOfMemory}!ColumnData,
            left_schema: Schema,
            right_shuffle_files: [][]const u8,
            right_projection: *const fn (std.mem.Allocator, Block, usize) error{OutOfMemory}!ColumnData,
        ) !@This() {
            return .{
                .allocator = allocator,
                .left_shuffle_files = left_shuffle_files,
                .left_projection = left_projection,
                .left_schema = left_schema,
                .right_block_loader = try LoadShuffleFilesProducer.init(allocator, right_shuffle_files),
                .right_projection = right_projection,
                .left_row_map = MapType.init(allocator),
            };
        }

        pub fn deinit(self: *@This()) void {
            const left_columns = self.left_columns orelse @panic("left_columns should be initialized before deinit");
            for (left_columns) |*col| {
                col.deinit(self.allocator);
            }
            self.allocator.free(left_columns);
            var it = self.left_row_map.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.left_row_map.deinit();
        }

        pub fn next(self: *@This()) !TaskResult {
            if (self.finished) {
                return .{ .chunk = null, .is_last = true };
            }

            // build hashmap from left side and collect all rows (left_columns)
            if (self.left_columns == null) {
                const left_columns = try self.allocator.alloc(AnyList, self.left_schema.columns.len);
                var load_left = try LoadShuffleFilesProducer.init(self.allocator, self.left_shuffle_files);
                for (left_columns, self.left_schema.columns) |*col, schema_col| {
                    if (schema_col.typ == TYPE_I32) {
                        col.* = AnyList{ .IntList = try std.ArrayList(i32).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else if (schema_col.typ == TYPE_F32) {
                        col.* = AnyList{ .FloatList = try std.ArrayList(f32).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else if (schema_col.typ == TYPE_STR) {
                        col.* = AnyList{ .StrList = try std.ArrayList([]const u8).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else {
                        return Error.UnknownType;
                    }
                }
                while (true) {
                    const last_output = try load_left.next();
                    if (last_output.is_last) {
                        break;
                    }
                    var chunk = last_output.chunk orelse break;
                    const key_column: []const K = if (K == []const u8)
                        (try self.left_projection(self.allocator, chunk, chunk.rows())).Str.slices
                    else
                        (try self.left_projection(self.allocator, chunk, chunk.rows())).I32;
                    for (key_column, 0..) |key, row_idx| {
                        const entry = try self.left_row_map.getOrPut(key);
                        if (!entry.found_existing) {
                            entry.value_ptr.* = try std.ArrayList(u32).initCapacity(self.allocator, 1);
                        }
                        try entry.value_ptr.append(self.allocator, @intCast(row_idx));
                    }
                    for (chunk.cols, 0..) |col_data, col_idx| {
                        switch (left_columns[col_idx]) {
                            .IntList => |*list| {
                                switch (col_data) {
                                    .I32 => |vals| {
                                        try list.appendSlice(self.allocator, vals);
                                    },
                                    else => return Error.UnknownType,
                                }
                            },
                            .FloatList => |*list| {
                                switch (col_data) {
                                    .F32 => |vals| {
                                        try list.appendSlice(self.allocator, vals);
                                    },
                                    else => return Error.UnknownType,
                                }
                            },
                            .StrList => |*list| {
                                switch (col_data) {
                                    .Str => |vals| {
                                        try list.appendSlice(self.allocator, vals.slices);
                                    },
                                    else => return Error.UnknownType,
                                }
                            },
                        }
                    }
                }
                self.left_columns = left_columns;
            }

            // generate joined chunks
            const left_columns = self.left_columns orelse return Error.UnexepctedState;
            const last_output = try self.right_block_loader.next();
            var chunk = last_output.chunk orelse return .{ .chunk = null, .is_last = true };
            defer chunk.deinit(self.allocator);
            if (last_output.is_last) {
                self.finished = true;
                return .{ .chunk = null, .is_last = true };
            }
            const key_column: []const K = if (K == []const u8)
                (try self.right_projection(self.allocator, chunk, chunk.rows())).Str.slices
            else
                (try self.right_projection(self.allocator, chunk, chunk.rows())).I32;

            // create outputbuffer
            const output_columns = try self.allocator.alloc(AnyList, self.left_schema.columns.len + chunk.cols.len);
            defer self.allocator.free(output_columns);
            for (self.left_schema.columns, 0..) |schema_col, idx| {
                if (schema_col.typ == TYPE_I32) {
                    output_columns[idx] = AnyList{ .IntList = try std.ArrayList(i32).initCapacity(self.allocator, chunk.rows()) };
                } else if (schema_col.typ == TYPE_STR) {
                    output_columns[idx] = AnyList{ .StrList = try std.ArrayList([]const u8).initCapacity(self.allocator, chunk.rows()) };
                } else {
                    return Error.UnknownType;
                }
            }
            for (chunk.cols, self.left_schema.columns.len..) |chunk_col, idx| {
                switch (chunk_col) {
                    .I32 => |vals| {
                        output_columns[idx] = AnyList{ .IntList = try std.ArrayList(i32).initCapacity(self.allocator, vals.len) };
                    },
                    .F32 => |vals| {
                        output_columns[idx] = AnyList{ .FloatList = try std.ArrayList(f32).initCapacity(self.allocator, vals.len) };
                    },
                    .Str => |vals| {
                        output_columns[idx] = AnyList{ .StrList = try std.ArrayList([]const u8).initCapacity(self.allocator, vals.slices.len) };
                    },
                }
            }

            for (key_column, 0..) |key, right_row_idx| {
                const left_rows_idx = self.left_row_map.get(key) orelse continue;
                for (left_columns, 0..) |left_col, output_col_idx| {
                    switch (left_col) {
                        .IntList => |left_rows| {
                            for (left_rows_idx.items) |left_row_idx| {
                                var output_col = &output_columns[output_col_idx].IntList;
                                try output_col.append(self.allocator, left_rows.items[left_row_idx]);
                            }
                        },
                        .FloatList => |left_rows| {
                            for (left_rows_idx.items) |left_row_idx| {
                                var output_col = &output_columns[output_col_idx].FloatList;
                                try output_col.append(self.allocator, left_rows.items[left_row_idx]);
                            }
                        },
                        .StrList => |left_rows| {
                            for (left_rows_idx.items) |left_row_idx| {
                                var output_col = &output_columns[output_col_idx].StrList;
                                try output_col.append(self.allocator, left_rows.items[left_row_idx]);
                            }
                        },
                    }
                }
                for (chunk.cols, self.left_schema.columns.len..) |right_col, output_col_idx| {
                    switch (right_col) {
                        .I32 => |vals| {
                            var output_col = &output_columns[output_col_idx].IntList;
                            const right_value = vals[right_row_idx];
                            try output_col.appendNTimes(self.allocator, right_value, left_rows_idx.items.len);
                        },
                        .F32 => |vals| {
                            var output_col = &output_columns[output_col_idx].FloatList;
                            const right_value = vals[right_row_idx];
                            try output_col.appendNTimes(self.allocator, right_value, left_rows_idx.items.len);
                        },
                        .Str => |vals| {
                            var output_col = &output_columns[output_col_idx].StrList;
                            const right_value = vals.slices[right_row_idx];
                            try output_col.appendNTimes(self.allocator, right_value, left_rows_idx.items.len);
                        },
                    }
                }
            }
            const joined_columns = try self.allocator.alloc(ColumnData, self.left_schema.columns.len + chunk.cols.len);
            for (joined_columns, output_columns) |*col, output_col| {
                switch (output_col) {
                    .IntList => |list| {
                        var mutable_list = list;
                        col.* = ColumnData{ .I32 = try mutable_list.toOwnedSlice(self.allocator) };
                    },
                    .FloatList => |list| {
                        var mutable_list = list;
                        col.* = ColumnData{ .F32 = try mutable_list.toOwnedSlice(self.allocator) };
                    },
                    .StrList => |list| {
                        col.* = ColumnData{ .Str = try StringColumn.init(self.allocator, list.items) };
                        var mutable_list = list;
                        mutable_list.deinit(self.allocator);
                    },
                }
            }
            return .{ .chunk = Block{ .cols = joined_columns }, .is_last = false };
        }
    };
}

pub const LoadTableBlockProducer = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    block_id: u32,
    finished: bool = false,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8, block_id: u32) !LoadTableBlockProducer {
        return LoadTableBlockProducer{
            .allocator = allocator,
            .file_path = file_path,
            .block_id = block_id,
        };
    }

    pub fn next(self: *LoadTableBlockProducer) !TaskResult {
        if (self.finished) {
            return .{ .chunk = null, .is_last = true };
        }
        try GLOBAL_TRACER.startEvent("load table block");
        const block_file = try BlockFile.initFromFile(self.allocator, self.file_path);
        const block_data = try block_file.readBlock(self.block_id);
        self.finished = true;
        try GLOBAL_TRACER.endEvent("load table block");
        return .{ .chunk = block_data, .is_last = false };
    }
};

pub const LoadShuffleFilesProducer = struct {
    allocator: std.mem.Allocator,
    file_paths: [][]const u8,
    current_file_index: usize = 0,
    current_block_index: u32 = 0,

    pub fn init(allocator: std.mem.Allocator, file_paths: [][]const u8) !LoadShuffleFilesProducer {
        return LoadShuffleFilesProducer{
            .allocator = allocator,
            .file_paths = file_paths,
        };
    }

    pub fn next(self: *LoadShuffleFilesProducer) !TaskResult {
        if (self.current_file_index >= self.file_paths.len) {
            return .{ .chunk = null, .is_last = true };
        }
        try GLOBAL_TRACER.startEvent("load shuffle block");
        const file_path = self.file_paths[self.current_file_index];
        const block_file = try BlockFile.initFromFile(self.allocator, file_path);
        if (self.current_block_index >= block_file.block_starts.items.len) {
            try GLOBAL_TRACER.endEvent("load shuffle block");
            self.current_file_index += 1;
            self.current_block_index = 0;
            return self.next();
        }
        const block_data = try block_file.readBlock(self.current_block_index);
        self.current_block_index += 1;
        try GLOBAL_TRACER.endEvent("load shuffle block");
        return .{ .chunk = block_data, .is_last = false };
    }
};

test "write -> append -> read block" {
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.testing.allocator;
    GLOBAL_TRACER = try Tracer.init(allocator);
    defer GLOBAL_TRACER.deinit(allocator);

    const schema = Schema{ .columns = (&[_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
    })[0..] };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 1, 2, 3 } };
    var col_str = try StringColumn.init(allocator, &[_][]const u8{ "hello", "hello", "zig", "!" });
    const col2 = ColumnData{ .Str = col_str };
    defer col_str.deinit(allocator);
    var block_data = [_]ColumnData{ col1, col2 };
    const block = Block{ .cols = block_data[0..] };

    var write_block_file = try BlockFile.init(allocator, schema, "tmp/test3.bin");
    defer write_block_file.deinit();
    try write_block_file.writeData(block);

    const project_key = struct {
        fn inner(allocato: std.mem.Allocator, blo: Block, rows: usize) !ColumnData {
            _ = allocato;
            _ = rows;
            return ColumnData{ .Str = blo.cols[1].Str };
        }
    }.inner;

    var left_shuffle_files = [_][]const u8{"tmp/test3.bin"};
    var join_producer = try JoinProducer.init(
        allocator,
        left_shuffle_files[0..],
        project_key,
        schema,
        left_shuffle_files[0..],
        project_key,
    );
    defer join_producer.deinit();

    // act
    const result = try join_producer.next();
    const result2 = try join_producer.next();

    // assert
    var chunk = result.chunk orelse return error.UnexepctedState;
    defer chunk.deinit(allocator);
    var expected_col_str = try StringColumn.init(allocator, &[_][]const u8{
        "hello",
        "hello",
        "hello",
        "hello",
        "zig",
        "!",
    });
    defer expected_col_str.deinit(allocator);
    var expected_columns = [_]ColumnData{
        ColumnData{ .I32 = &[_]i32{ -1, 1, -1, 1, 2, 3 } },
        ColumnData{ .Str = expected_col_str },
        ColumnData{ .I32 = &[_]i32{ -1, -1, 1, 1, 2, 3 } },
        ColumnData{ .Str = expected_col_str },
    };
    const expected_block = Block{ .cols = expected_columns[0..] };
    try std.testing.expectEqualDeep(chunk, expected_block);
    try std.testing.expectEqualDeep(result2, TaskResult{ .chunk = null, .is_last = true });
}
