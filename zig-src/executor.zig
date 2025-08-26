const std = @import("std");

const TYPE_I32: u8 = 0;
const TYPE_STR: u8 = 1;

pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
};

pub const Job = struct {
    input_file: []const u8,
    input_block_id: u32,
    output_file: []const u8,
};

pub const ColumnSchema = struct {
    typ: u8,
    name: []const u8,

    pub fn serializeSchema(file: anytype, columns: []const ColumnSchema) !void {
        if (columns.len > 255) return Error.SchemaTooLarge;

        try file.writeAll(&[_]u8{@intCast(columns.len)});

        for (columns) |column| {
            if (column.name.len > 255) return Error.NameTooLong;

            try file.writeAll(&[_]u8{column.typ});
            try file.writeAll(&[_]u8{@intCast(column.name.len)});
            try file.writeAll(column.name);
        }
    }

    pub fn readSchema(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) ![]ColumnSchema {
        const column_nums = try reader.interface.takeInt(u8, .little);
        var columns = try std.ArrayList(ColumnSchema).initCapacity(allocator, @intCast(column_nums));
        for (0..column_nums) |_| {
            const col_type = try reader.interface.takeInt(u8, .little);
            const name_len = try reader.interface.takeInt(u8, .little);
            const name_buf = try allocator.alloc(u8, name_len);
            try reader.interface.readSliceAll(name_buf);
            try columns.append(allocator, ColumnSchema{ .typ = col_type, .name = name_buf });
        }
        return columns.items;
    }

    pub fn format(
        self: ColumnSchema,
        writer: anytype,
    ) !void {
        try writer.print("Person(name=\"{s}\", type={d})", .{ self.name, self.typ });
    }
};

pub const ColumnData = union(enum) {
    I32: []const i32,
    Str: []const []const u8,

    pub fn serialize(self: ColumnData, file: anytype) !void {
        var col_len: u32 = 0;
        switch (self) {
            .I32 => |vals| {
                col_len = @intCast(vals.len * 4);
            },
            .Str => |vals| {
                for (vals) |s| {
                    if (s.len > 255) {
                        return Error.StringTooLong;
                    }
                    col_len += @intCast(1 + s.len);
                }
            },
        }
        try file.writeAll(std.mem.asBytes(&col_len));

        switch (self) {
            .I32 => |vals| {
                try file.writeAll(std.mem.sliceAsBytes(vals));
            },
            .Str => |vals| {
                for (vals) |s| {
                    const str_len: u8 = @intCast(s.len);
                    try file.writeAll(std.mem.asBytes(&str_len));
                    try file.writeAll(s);
                }
            },
        }
    }

    pub fn len(self: ColumnData) usize {
        return switch (self) {
            .I32 => |vals| vals.len,
            .Str => |vals| vals.len,
        };
    }

    pub fn readColumn(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, typ: u8, row_count: u32) !ColumnData {
        _ = try reader.interface.takeInt(u32, .little);
        switch (typ) {
            TYPE_I32 => {
                const vals = try allocator.alloc(i32, @intCast(row_count));
                try reader.interface.readSliceAll(std.mem.sliceAsBytes(vals));
                return ColumnData{ .I32 = vals };
            },
            TYPE_STR => {
                var vals = try std.ArrayList([]const u8).initCapacity(allocator, @intCast(row_count));
                for (0..row_count) |_| {
                    const str_len = try reader.interface.takeInt(u8, .little);
                    const str_buf = try allocator.alloc(u8, str_len);
                    try reader.interface.readSliceAll(str_buf);
                    try vals.append(allocator, str_buf);
                }
                return ColumnData{ .Str = vals.items };
            },
            else => {
                return Error.UnknownType;
            },
        }
    }

    pub fn format(
        self: ColumnData,
        writer: anytype,
    ) !void {
        switch (self) {
            .I32 => |vals| {
                try writer.print("I32 Column {any}", .{vals});
            },
            .Str => |vals| {
                try writer.print("Str Column {{ ", .{});
                for (vals) |val| {
                    try writer.print("\"{s}\", ", .{val});
                }
                try writer.print("}}\n", .{});
            },
        }
    }
};

pub const Block = struct {
    cols: []const ColumnData,

    pub fn serialize(self: Block, file: anytype) !void {
        const row_count: u32 = @intCast(self.cols[0].len());
        try file.writeAll(std.mem.asBytes(&row_count));
        for (self.cols) |col| {
            try col.serialize(file);
        }
        return;
    }

    pub fn readBlock(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, block_start: u32, schema: []const ColumnSchema) ![]ColumnData {
        try reader.seekTo(@intCast(block_start));
        const row_count = try reader.interface.takeInt(u32, .little);
        var columns = try std.ArrayList(ColumnData).initCapacity(allocator, schema.len);
        for (schema) |column| {
            const column_data = try ColumnData.readColumn(allocator, reader, column.typ, row_count);
            try columns.append(allocator, column_data);
        }
        return columns.items;
    }
};

pub const BlockFile = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    schema: []const ColumnSchema,
    block_starts: std.ArrayList(u32),

    pub fn init(allocator: std.mem.Allocator, schema: []const ColumnSchema) !BlockFile {
        return BlockFile{
            .allocator = allocator,
            .schema = schema,
            .block_starts = try std.ArrayList(u32).initCapacity(allocator, 100),
            .file_path = undefined,
        };
    }

    pub fn initFromFile(allocator: std.mem.Allocator, file_path: []const u8) !BlockFile {
        var file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_only });
        defer file.close();
        var file_buffer: [4096]u8 = undefined;
        var file_reader = file.reader(&file_buffer);
        const schema = try ColumnSchema.readSchema(allocator, &file_reader);
        const block_starts = try BlockFile.readBlockStarts(allocator, &file_reader);

        return BlockFile{
            .allocator = allocator,
            .file_path = file_path,
            .schema = schema,
            .block_starts = block_starts,
        };
    }

    pub fn writeData(
        self: *BlockFile,
        file_path: []const u8,
        data: Block,
    ) !void {
        const fs = std.fs.cwd();
        var file = try fs.createFile(file_path, .{ .truncate = true });
        defer file.close();
        try ColumnSchema.serializeSchema(file, self.schema);
        const start: u32 = @intCast(try file.getPos());
        try self.block_starts.append(self.allocator, start);
        std.debug.print("Writing block at offset {any}\n", .{data});
        try data.serialize(file);
        try self.writeFooter(file);
        return;
    }

    pub fn writeFooter(self: *BlockFile, file: anytype) !void {
        for (self.block_starts.items) |off| {
            try file.writeAll(std.mem.asBytes(&off));
        }

        const block_count: u32 = @intCast(self.block_starts.items.len);
        try file.writeAll(std.mem.asBytes(&block_count));
        return;
    }

    fn readBlockStarts(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) !std.ArrayList(u32) {
        const file_size = try reader.getSize();
        try reader.seekTo(file_size - 4);
        const block_count = try reader.interface.takeInt(u32, .little);
        const footer_size = 4 + (block_count * 4);
        try reader.seekTo(file_size - footer_size);

        var starts = try std.ArrayList(u32).initCapacity(allocator, @intCast(block_count));
        for (0..block_count) |_| {
            const off = try reader.interface.takeInt(u32, .little);
            try starts.append(allocator, off);
        }
        return starts;
    }

    pub fn readBlock(self: *const BlockFile, block_id: u32) ![]ColumnData {
        var file = try std.fs.cwd().openFile(self.file_path, .{ .mode = .read_only });
        defer file.close();
        var file_buffer: [4096]u8 = undefined;
        var file_reader = file.reader(&file_buffer);

        std.debug.assert(block_id < self.block_starts.items.len);
        const block_start = self.block_starts.items[block_id];
        return Block.readBlock(self.allocator, &file_reader, block_start, self.schema);
    }

    pub fn printContent(self: *const BlockFile) !void {
        for (self.schema) |column| {
            std.debug.print("{s}\t\t", .{column.name});
        }
        std.debug.print("\n", .{});
        for (self.block_starts.items, 0..) |_, i| {
            const columns = try self.readBlock(@intCast(i));
            const rows = columns[0].len();
            for (0..rows) |row_idx| {
                for (columns) |col| {
                    switch (col) {
                        .I32 => |vals| {
                            std.debug.print("{d}\t\t", .{vals[row_idx]});
                        },
                        .Str => |vals| {
                            std.debug.print("{s}\t\t", .{vals[row_idx]});
                        },
                    }
                }
                std.debug.print("\n", .{});
            }
        }
    }
};

pub fn test_write_file(allocator: std.mem.Allocator) !void {
    const schema_arr = [_]ColumnSchema{
        .{ .typ = 0, .name = "delta" },
        .{ .typ = 1, .name = "msg" },
    };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3 } };
    const col2 = ColumnData{ .Str = &[_][]const u8{ "hello", "zig", "!" } };
    const block_data = [_]ColumnData{ col1, col2 };
    const block = Block{ .cols = &block_data };

    const fs = std.fs.cwd();
    var file = try fs.createFile("example.bf", .{ .truncate = true });
    defer file.close();

    var bf = try BlockFile.init(allocator, schema_arr);
    try bf.writeData(file, block);
}

pub fn task_0(allocator: std.mem.Allocator, input: []ColumnData, job: Job, schema: []const ColumnSchema) ![]const ColumnData {
    _ = input;
    _ = schema;
    const block_file = try BlockFile.initFromFile(allocator, job.input_file);
    return try block_file.readBlock(job.input_block_id);
}

pub fn project_01(allocator: std.mem.Allocator, input: []const ColumnData, output: []i32) !ColumnData {
    _ = allocator;
    const col0 = input[0].I32;
    const col1 = input[0].I32;
    for (col0, col1, 0..) |v1, v2, idx| {
        output[idx] = v1 + v2;
    }
    const const_out = output;
    return ColumnData{ .I32 = const_out };
}

pub fn project_02(allocator: std.mem.Allocator, input: []const ColumnData, output: [][]u8) !ColumnData {
    const col0 = input[1].Str;
    const col1 = input[1].Str;
    for (col0, col1, 0..) |v1, v2, idx| {
        const result = try std.mem.concat(allocator, u8, &[_][]const u8{ v1, v2 });
        output[idx] = result;
    }
    return ColumnData{ .Str = output };
}

pub fn project_03(allocator: std.mem.Allocator, input: []const ColumnData, output: []bool) void {
    _ = allocator;
    const col0 = input[1].Str;
    for (col0, 0..) |v1, idx| {
        output[idx] = v1.len > 3;
    }
}

pub fn task_1(allocator: std.mem.Allocator, input: []const ColumnData, job: Job, schema: []const ColumnSchema) ![]const ColumnData {
    _ = job;
    _ = schema;
    const rows = input[0].len();

    const col1 = try allocator.alloc(i32, rows);
    const out1 = try project_01(allocator, input, col1);
    const col2 = try allocator.alloc([]u8, rows);
    const out2 = try project_02(allocator, input, col2);
    const slice: []ColumnData = try allocator.alloc(ColumnData, 2);
    slice[0] = out1;
    slice[1] = out2;

    std.debug.print("Task 1 produced {any}\n", .{slice});
    return slice;
}

pub fn filter_column(col: ColumnData, condition_col: []const bool, allocator: std.mem.Allocator) !ColumnData {
    var output_rows: usize = 0;
    for (condition_col) |c| {
        if (c) output_rows += 1;
    }
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
        .Str => |vals| {
            const filtered_vals = try allocator.alloc([]const u8, output_rows);
            var filter_index: u32 = 0;
            for (vals, condition_col) |input_val, c| {
                if (c) {
                    filtered_vals[filter_index] = input_val;
                    filter_index += 1;
                }
            }
            return ColumnData{ .Str = filtered_vals };
        },
    }
}

pub fn task_2(allocator: std.mem.Allocator, input: []const ColumnData, job: Job, schema: []const ColumnSchema) ![]const ColumnData {
    _ = job;
    _ = schema;
    const rows = input[0].len();
    const condition_col = try allocator.alloc(bool, rows);
    _ = project_03(allocator, input, condition_col);
    const slice: []ColumnData = try allocator.alloc(ColumnData, input.len);
    for (input, 0..) |col, col_idx| {
        const filtered_column = try filter_column(col, condition_col, allocator);
        slice[col_idx] = filtered_column;
    }
    return slice;
}

pub fn task_3(allocator: std.mem.Allocator, input: []const ColumnData, job: Job, schema: []const ColumnSchema) ![]const ColumnData {
    var block_file = try BlockFile.init(allocator, schema);
    const block = Block{ .cols = input };
    try block_file.writeData(job.output_file, block);
    return (&[_]ColumnData{})[0..];
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    const schema_arr = (&[_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
    })[0..];
    const job = Job{
        .input_file = "example.bf",
        .input_block_id = 0,
        .output_file = "output.bf",
    };

    const first_inpu = &[_]ColumnData{};

    const task_0_out = try task_0(allocator, first_inpu, job, schema_arr);
    const task_1_out = try task_1(allocator, task_0_out, job, schema_arr);
    const task_2_out = try task_2(allocator, task_1_out, job, schema_arr);
    const task_3_out = try task_3(allocator, task_2_out, job, schema_arr);
    _ = task_3_out;
}
