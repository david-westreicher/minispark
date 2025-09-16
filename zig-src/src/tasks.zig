const std = @import("std");

const Block = @import("block_file.zig").Block;
const BlockFile = @import("block_file.zig").BlockFile;
const ColumnData = @import("block_file.zig").ColumnData;
const ColumnType = @import("block_file.zig").ColumnType;
const Schema = @import("block_file.zig").Schema;
const StringColumn = @import("block_file.zig").StringColumn;

const AnyList = @import("utils.zig").AnyList;
const Executor = @import("root.zig");
const Error = Executor.Error;

const ROWS_PER_BLOCK = @import("block_file.zig").ROWS_PER_BLOCK;

pub const TaskResult = struct {
    chunk: ?Block = null,
    is_last: bool = false,
};

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
                    if (schema_col.typ == ColumnType.I32) {
                        col.* = AnyList{ .IntList = try std.ArrayList(i32).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else if (schema_col.typ == ColumnType.I64) {
                        col.* = AnyList{ .Int64List = try std.ArrayList(i64).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else if (schema_col.typ == ColumnType.F32) {
                        col.* = AnyList{ .FloatList = try std.ArrayList(f32).initCapacity(self.allocator, ROWS_PER_BLOCK) };
                    } else if (schema_col.typ == ColumnType.STR) {
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
                            .Int64List => |*list| {
                                switch (col_data) {
                                    .I64 => |vals| {
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
            const left_columns = self.left_columns orelse return Error.UnexpectedState;
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
                if (schema_col.typ == ColumnType.I32) {
                    output_columns[idx] = AnyList{ .IntList = try std.ArrayList(i32).initCapacity(self.allocator, chunk.rows()) };
                } else if (schema_col.typ == ColumnType.I64) {
                    output_columns[idx] = AnyList{ .Int64List = try std.ArrayList(i64).initCapacity(self.allocator, chunk.rows()) };
                } else if (schema_col.typ == ColumnType.F32) {
                    output_columns[idx] = AnyList{ .FloatList = try std.ArrayList(f32).initCapacity(self.allocator, chunk.rows()) };
                } else if (schema_col.typ == ColumnType.STR) {
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
                    .I64 => |vals| {
                        output_columns[idx] = AnyList{ .Int64List = try std.ArrayList(i64).initCapacity(self.allocator, vals.len) };
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
                        .Int64List => |left_rows| {
                            for (left_rows_idx.items) |left_row_idx| {
                                var output_col = &output_columns[output_col_idx].Int64List;
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
                        .I64 => |vals| {
                            var output_col = &output_columns[output_col_idx].Int64List;
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
                    .Int64List => |list| {
                        var mutable_list = list;
                        col.* = ColumnData{ .I64 = try mutable_list.toOwnedSlice(self.allocator) };
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
        try Executor.GLOBAL_TRACER.startEvent("load table block");
        const block_file = try BlockFile.initFromFile(self.allocator, self.file_path);
        const block_data = try block_file.readBlock(self.block_id);
        self.finished = true;
        try Executor.GLOBAL_TRACER.endEvent("load table block");
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
        try Executor.GLOBAL_TRACER.startEvent("load shuffle block");
        const file_path = self.file_paths[self.current_file_index];
        const block_file = try BlockFile.initFromFile(self.allocator, file_path);
        if (self.current_block_index >= block_file.block_starts.items.len) {
            try Executor.GLOBAL_TRACER.endEvent("load shuffle block");
            self.current_file_index += 1;
            self.current_block_index = 0;
            return self.next();
        }
        const block_data = try block_file.readBlock(self.current_block_index);
        self.current_block_index += 1;
        try Executor.GLOBAL_TRACER.endEvent("load shuffle block");
        return .{ .chunk = block_data, .is_last = false };
    }
};

test "write -> append -> read block" {
    const Tracer = @import("utils.zig").Tracer;
    const ColumnSchema = @import("block_file.zig").ColumnSchema;
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.testing.allocator;
    Executor.GLOBAL_TRACER = try Tracer.init(allocator);
    defer Executor.GLOBAL_TRACER.deinit(allocator);

    const schema = Schema{ .columns = (&[_]ColumnSchema{
        .{ .typ = ColumnType.I32, .name = "delta" },
        .{ .typ = ColumnType.STR, .name = "msg" },
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
    var join_producer = try JoinProducer([]const u8).init(
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
    var chunk = result.chunk orelse return error.UnexpectedState;
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
