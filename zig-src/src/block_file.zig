const std = @import("std");

const SIZE_32KB = 32 * 1024;
const ROWS_PER_BLOCK = 2 * 1024 * 1024;

pub const TYPE_I32: u8 = 0;
pub const TYPE_STR: u8 = 1;
pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
};

pub const ColumnSchema = struct {
    typ: u8,
    name: []const u8,

    pub fn serializeSchema(writer: *std.fs.File.Writer, columns: []const ColumnSchema) !void {
        if (columns.len > 255) return Error.SchemaTooLarge;

        try writer.interface.writeAll(&[_]u8{@intCast(columns.len)});

        for (columns) |column| {
            if (column.name.len > 255) return Error.NameTooLong;

            try writer.interface.writeAll(&[_]u8{column.typ});
            try writer.interface.writeAll(&[_]u8{@intCast(column.name.len)});
            try writer.interface.writeAll(column.name);
        }
        try writer.end();
    }

    pub fn readSchema(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) ![]ColumnSchema {
        const column_nums = try reader.interface.takeInt(u8, .little);
        var columns = try std.ArrayList(ColumnSchema).initCapacity(allocator, column_nums);
        for (0..column_nums) |_| {
            const col_type = try reader.interface.takeInt(u8, .little);
            const name_len = try reader.interface.takeInt(u8, .little);
            const name_buf = try allocator.alloc(u8, name_len);
            try reader.interface.readSliceAll(name_buf);
            try columns.append(allocator, ColumnSchema{ .typ = col_type, .name = name_buf });
        }
        return columns.items;
    }
};

pub const ColumnData = union(enum) {
    I32: []const i32,
    Str: []const []const u8,

    pub fn serialize(self: ColumnData, allocator: std.mem.Allocator, writer: *std.fs.File.Writer, offset: ?usize, end: ?usize) !void {
        const start = offset orelse 0;
        const fini = end orelse self.len();
        switch (self) {
            .I32 => |vals| {
                const values = vals[start..fini];
                const col_len: u32 = @intCast(values.len * 4);
                try writer.interface.writeAll(std.mem.asBytes(&col_len));
                try writer.interface.writeAll(std.mem.sliceAsBytes(values));
            },
            .Str => |vals| {
                const values = vals[start..fini];
                var col_len: u32 = 0;
                for (values) |s| {
                    if (s.len > 255) {
                        return Error.StringTooLong;
                    }
                    col_len += @intCast(1 + s.len);
                }
                try writer.interface.writeAll(std.mem.asBytes(&col_len));
                var total_size: usize = 0;
                for (values) |s| {
                    total_size += 1;
                    total_size += s.len;
                }

                var buf = try allocator.alloc(u8, total_size);
                defer allocator.free(buf);
                var i: usize = 0;
                for (values) |s| {
                    buf[i] = @intCast(s.len);
                    i += 1;
                }
                for (values) |s| {
                    @memcpy(buf[i..][0..s.len], s);
                    i += s.len;
                }
                try writer.interface.writeAll(buf);
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
                const vals = try allocator.alloc(i32, row_count);
                try reader.interface.readSliceAll(std.mem.sliceAsBytes(vals));
                return ColumnData{ .I32 = vals };
            },
            TYPE_STR => {
                const lengths = try allocator.alloc(u8, row_count);
                defer allocator.free(lengths);

                try reader.interface.readSliceAll(std.mem.sliceAsBytes(lengths));
                var total_length: usize = 0;
                for (lengths) |l| {
                    total_length += l;
                }

                const string_buffer = try allocator.alloc(u8, total_length);
                try reader.interface.readSliceAll(string_buffer);

                const string_columns = try allocator.alloc([]const u8, row_count);
                var offset: u32 = 0;
                for (lengths, 0..) |length, i| {
                    string_columns[i] = string_buffer[offset .. offset + length];
                    offset += length;
                }
                return ColumnData{ .Str = string_columns };
            },
            else => {
                return Error.UnknownType;
            },
        }
    }
};

pub const Block = struct {
    cols: []const ColumnData,

    pub fn serialize(self: Block, allocator: std.mem.Allocator, writer: *std.fs.File.Writer, block_starts: *std.ArrayList(u32)) !void {
        var start: u32 = @intCast(writer.pos);
        const row_count: u32 = @intCast(self.cols[0].len());
        try writer.interface.writeAll(std.mem.asBytes(&row_count));
        var offset: usize = 0;
        while (offset < self.cols[0].len()) : (offset += ROWS_PER_BLOCK) {
            try block_starts.append(allocator, start);
            for (self.cols) |col| {
                const end = @min(offset + ROWS_PER_BLOCK, col.len());
                try col.serialize(allocator, writer, offset, end);
            }
            start = @intCast(writer.pos);
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

    pub fn merge(allocator: std.mem.Allocator, first_data: []const ColumnData, second_data: []const ColumnData) ![]const ColumnData {
        std.debug.assert(first_data.len == second_data.len);
        var merged = try std.ArrayList(ColumnData).initCapacity(allocator, first_data.len);
        for (first_data, 0..) |col, i| {
            const other_col = second_data[i];
            switch (col) {
                .I32 => |vals| {
                    const other_vals = other_col.I32;
                    const new_vals = try allocator.alloc(i32, vals.len + other_vals.len);
                    @memcpy(new_vals[0..vals.len], vals);
                    @memcpy(new_vals[vals.len..], other_vals);
                    try merged.append(allocator, ColumnData{ .I32 = new_vals });
                },
                .Str => |vals| {
                    const other_vals = other_col.Str;
                    const new_vals = try allocator.alloc([]const u8, vals.len + other_vals.len);
                    @memcpy(new_vals[0..vals.len], vals);
                    @memcpy(new_vals[vals.len..], other_vals);
                    try merged.append(allocator, ColumnData{ .Str = new_vals });
                },
            }
        }
        return merged.items;
    }
};

pub const BlockFile = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    schema: []const ColumnSchema,
    block_starts: std.ArrayList(u32),

    pub fn init(allocator: std.mem.Allocator, schema: []const ColumnSchema, file_path: []const u8) !BlockFile {
        return BlockFile{
            .allocator = allocator,
            .schema = schema,
            .block_starts = try std.ArrayList(u32).initCapacity(allocator, 100),
            .file_path = file_path,
        };
    }

    pub fn initFromFile(allocator: std.mem.Allocator, file_path: []const u8) !BlockFile {
        var file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_only });
        defer file.close();
        var file_buffer: [SIZE_32KB]u8 = undefined;
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

    pub fn writeData(self: *BlockFile, data: Block) !void {
        var file = try std.fs.cwd().createFile(self.file_path, .{ .truncate = true });
        defer file.close();
        var write_buffer: [SIZE_32KB]u8 = undefined;
        var writer = file.writer(&write_buffer);
        try ColumnSchema.serializeSchema(&writer, self.schema);
        try data.serialize(self.allocator, &writer, &self.block_starts);
        try self.writeFooter(&writer);
        try writer.end();
        return;
    }

    pub fn appendData(self: *BlockFile, allocator: std.mem.Allocator, block: Block) !void {
        var file = std.fs.cwd().openFile(self.file_path, .{ .mode = .write_only }) catch |err| {
            if (err == std.fs.File.OpenError.FileNotFound) {
                try self.writeData(block);
                return;
            } else {
                return err;
            }
        };
        defer file.close();
        var write_buffer: [SIZE_32KB]u8 = undefined;
        var writer = file.writer(&write_buffer);

        const read_block = try BlockFile.initFromFile(self.allocator, self.file_path);
        const last_block_id: u32 = @intCast(read_block.block_starts.items.len - 1);
        const last_block = try read_block.readBlock(last_block_id);
        self.block_starts.clearAndFree(allocator);
        try self.block_starts.appendSlice(allocator, read_block.block_starts.items);
        var block_to_append = block;

        if (last_block[0].len() < ROWS_PER_BLOCK) {
            block_to_append = Block{
                .cols = try Block.merge(allocator, last_block, block.cols),
            };
            const seek_pos: u64 = @intCast(self.block_starts.pop() orelse @panic("no block to pop"));
            try writer.seekTo(seek_pos);
        } else {
            const seek_pos: u64 = try file.getEndPos() - 4 * (self.block_starts.items.len + 1);
            try writer.seekTo(seek_pos); // right after last block
        }
        try block_to_append.serialize(allocator, &writer, &self.block_starts);
        try self.writeFooter(&writer);
        try writer.end();
        return;
    }

    pub fn writeFooter(self: *BlockFile, writer: *std.fs.File.Writer) !void {
        for (self.block_starts.items) |off| {
            try writer.interface.writeAll(std.mem.asBytes(&off));
        }

        const block_count: u32 = @intCast(self.block_starts.items.len);
        try writer.interface.writeAll(std.mem.asBytes(&block_count));
        return;
    }

    fn readBlockStarts(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) !std.ArrayList(u32) {
        const file_size = try reader.getSize();
        try reader.seekTo(file_size - 4);
        const block_count = try reader.interface.takeInt(u32, .little);
        const footer_size = 4 + (block_count * 4);
        try reader.seekTo(file_size - footer_size);

        var starts = try std.ArrayList(u32).initCapacity(allocator, block_count);
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

test "write -> read block" {
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.heap.page_allocator;

    const schema_arr = [_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
    };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3 } };
    const col2 = ColumnData{ .Str = &[_][]const u8{ "hello", "zig", "!" } };
    const block_data = [_]ColumnData{ col1, col2 };
    const block = Block{ .cols = &block_data };

    var write_block_file = try BlockFile.init(allocator, &schema_arr, "tmp/test.bin");
    try write_block_file.writeData(block);

    var read_block_file = try BlockFile.initFromFile(allocator, "tmp/test.bin");
    const read_block_data = try read_block_file.readBlock(0);

    try std.testing.expectEqualDeep(block_data[0..], read_block_data);
}

test "write -> append -> read block" {
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.heap.page_allocator;

    const schema_arr = [_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
    };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3 } };
    const col2 = ColumnData{ .Str = &[_][]const u8{ "hello", "zig", "!" } };
    const block_data = [_]ColumnData{ col1, col2 };
    const block = Block{ .cols = &block_data };

    var write_block_file = try BlockFile.init(allocator, &schema_arr, "tmp/test2.bin");
    try write_block_file.writeData(block);
    try write_block_file.appendData(allocator, block);

    var read_block_file = try BlockFile.initFromFile(allocator, "tmp/test2.bin");
    const read_block_data = try read_block_file.readBlock(0);

    const expected_col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3, -1, 2, 3 } };
    const expected_col2 = ColumnData{ .Str = &[_][]const u8{ "hello", "zig", "!", "hello", "zig", "!" } };
    const expected_block_data = [_]ColumnData{ expected_col1, expected_col2 };
    try std.testing.expectEqualDeep(expected_block_data[0..], read_block_data);
}
