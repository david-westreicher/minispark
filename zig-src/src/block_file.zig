const std = @import("std");

const SIZE_32KB = 32 * 1024;
pub const ROWS_PER_BLOCK = 2 * 1024 * 1024;

pub const TYPE_I32: u8 = 0;
pub const TYPE_STR: u8 = 1;
pub const TYPE_F32: u8 = 2;
pub const TYPE_I64: u8 = 3;
pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
};

pub const ColumnSchema = struct {
    typ: u8,
    name: []const u8,

    pub fn read(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) !ColumnSchema {
        const col_type = try reader.interface.takeInt(u8, .little);
        const name_len = try reader.interface.takeInt(u8, .little);
        const name_buf = try allocator.alloc(u8, name_len);
        try reader.interface.readSliceAll(name_buf);
        return ColumnSchema{ .typ = col_type, .name = name_buf };
    }

    pub fn write(self: *const ColumnSchema, writer: *std.fs.File.Writer) !void {
        if (self.name.len > 255) return Error.NameTooLong;

        try writer.interface.writeAll(&[_]u8{self.typ});
        try writer.interface.writeAll(&[_]u8{@intCast(self.name.len)});
        try writer.interface.writeAll(self.name);
    }

    pub fn deinit(self: *const ColumnSchema, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }

    pub fn clone(self: ColumnSchema, allocator: std.mem.Allocator) !ColumnSchema {
        return ColumnSchema{
            .typ = self.typ,
            .name = try allocator.dupe(u8, self.name),
        };
    }
};

pub const Schema = struct {
    columns: []const ColumnSchema,

    pub fn deinit(self: *Schema, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            col.deinit(allocator);
        }
        allocator.free(self.columns);
    }

    pub fn clone(self: Schema, allocator: std.mem.Allocator) !Schema {
        const cols = try allocator.alloc(ColumnSchema, self.columns.len);
        for (self.columns, 0..) |col, i| {
            cols[i] = try col.clone(allocator);
        }
        return Schema{ .columns = cols };
    }

    pub fn read(allocator: std.mem.Allocator, reader: *std.fs.File.Reader) !Schema {
        const column_nums = try reader.interface.takeInt(u8, .little);
        var columns = try allocator.alloc(ColumnSchema, column_nums);
        for (0..column_nums) |i| {
            columns[i] = try ColumnSchema.read(allocator, reader);
        }
        return Schema{ .columns = columns };
    }

    pub fn write(self: *Schema, writer: *std.fs.File.Writer) !void {
        if (self.columns.len > 255) return Error.SchemaTooLarge;
        try writer.interface.writeAll(&[_]u8{@intCast(self.columns.len)});
        for (self.columns) |column| {
            try column.write(writer);
        }
        try writer.end();
    }
};

pub const StringColumn = struct {
    total_buffer: []const u8,
    slices: []const []const u8,

    pub fn init(allocator: std.mem.Allocator, slices: []const []const u8) !StringColumn {
        var total_size: usize = 0;
        for (slices) |s| {
            total_size += s.len;
        }
        const total_buffer = try allocator.alloc(u8, total_size);
        var offset: usize = 0;
        for (slices) |s| {
            @memcpy(total_buffer[offset..][0..s.len], s);
            offset += s.len;
        }
        const vals = try allocator.alloc([]const u8, slices.len);
        offset = 0;
        for (slices, 0..) |s, i| {
            vals[i] = total_buffer[offset .. offset + s.len];
            offset += s.len;
        }
        return StringColumn{
            .total_buffer = total_buffer,
            .slices = vals,
        };
    }

    pub fn deinit(self: *StringColumn, allocator: std.mem.Allocator) void {
        allocator.free(self.total_buffer);
        allocator.free(self.slices);
    }

    pub fn append(self: StringColumn, allocator: std.mem.Allocator, other: StringColumn) !StringColumn {
        const new_total = try allocator.alloc(u8, self.total_buffer.len + other.total_buffer.len);
        @memcpy(new_total[0..self.total_buffer.len], self.total_buffer);
        @memcpy(new_total[self.total_buffer.len..], other.total_buffer);
        const new_vals = try allocator.alloc([]const u8, self.slices.len + other.slices.len);
        var offset: usize = 0;
        var idx: u32 = 0;
        for (self.slices) |s| {
            new_vals[idx] = new_total[offset .. offset + s.len];
            offset += s.len;
            idx += 1;
        }
        for (other.slices) |s| {
            new_vals[idx] = new_total[offset .. offset + s.len];
            offset += s.len;
            idx += 1;
        }
        return StringColumn{
            .total_buffer = new_total,
            .slices = new_vals,
        };
    }
};

pub const ColumnData = union(enum) {
    I32: []const i32,
    I64: []const i64,
    F32: []const f32,
    Str: StringColumn,

    pub fn write(self: ColumnData, allocator: std.mem.Allocator, writer: *std.fs.File.Writer, offset: ?usize, end: ?usize) !void {
        const start = offset orelse 0;
        const fini = end orelse self.len();
        switch (self) {
            .I32 => |vals| {
                const values = vals[start..fini];
                const col_len: u32 = @intCast(values.len * 4);
                try writer.interface.writeAll(std.mem.asBytes(&col_len));
                try writer.interface.writeAll(std.mem.sliceAsBytes(values));
            },
            .I64 => |vals| {
                const values = vals[start..fini];
                const col_len: u32 = @intCast(values.len * 8);
                try writer.interface.writeAll(std.mem.asBytes(&col_len));
                try writer.interface.writeAll(std.mem.sliceAsBytes(values));
            },
            .F32 => |vals| {
                const values = vals[start..fini];
                const col_len: u32 = @intCast(values.len * 4);
                try writer.interface.writeAll(std.mem.asBytes(&col_len));
                try writer.interface.writeAll(std.mem.sliceAsBytes(values));
            },
            .Str => |vals| {
                const values = vals.slices[start..fini];
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
            .I64 => |vals| vals.len,
            .F32 => |vals| vals.len,
            .Str => |vals| vals.slices.len,
        };
    }

    pub fn deinit(self: *ColumnData, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .I32 => |vals| allocator.free(vals),
            .I64 => |vals| allocator.free(vals),
            .F32 => |vals| allocator.free(vals),
            .Str => |*vals| vals.deinit(allocator),
        }
    }

    pub fn readColumn(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, typ: u8, row_count: u32) !ColumnData {
        _ = try reader.interface.takeInt(u32, .little);
        switch (typ) {
            TYPE_I32 => {
                const vals = try allocator.alloc(i32, row_count);
                try reader.interface.readSliceAll(std.mem.sliceAsBytes(vals));
                return ColumnData{ .I32 = vals };
            },
            TYPE_I64 => {
                const vals = try allocator.alloc(i64, row_count);
                try reader.interface.readSliceAll(std.mem.sliceAsBytes(vals));
                return ColumnData{ .I64 = vals };
            },
            TYPE_F32 => {
                const vals = try allocator.alloc(f32, row_count);
                try reader.interface.readSliceAll(std.mem.sliceAsBytes(vals));
                return ColumnData{ .F32 = vals };
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
                return ColumnData{ .Str = StringColumn{
                    .total_buffer = string_buffer,
                    .slices = string_columns,
                } };
            },
            else => {
                return Error.UnknownType;
            },
        }
    }
};

pub const Block = struct {
    cols: []ColumnData,

    pub fn deinit(self: *Block, allocator: std.mem.Allocator) void {
        for (self.cols) |*col| {
            col.deinit(allocator);
        }
        allocator.free(self.cols);
    }

    pub fn write(self: Block, allocator: std.mem.Allocator, writer: *std.fs.File.Writer, block_starts: *std.ArrayList(u32)) !void {
        var start: u32 = @intCast(writer.pos);
        const row_count: u32 = @intCast(self.rows());
        try writer.interface.writeAll(std.mem.asBytes(&row_count));
        var offset: usize = 0;
        while (offset < self.rows()) : (offset += ROWS_PER_BLOCK) {
            try block_starts.append(allocator, start);
            for (self.cols) |col| {
                const end = @min(offset + ROWS_PER_BLOCK, col.len());
                try col.write(allocator, writer, offset, end);
            }
            start = @intCast(writer.pos);
        }
        return;
    }

    pub fn readBlock(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, block_start: u32, schema: Schema) !Block {
        try reader.seekTo(@intCast(block_start));
        const row_count = try reader.interface.takeInt(u32, .little);
        var columns = try allocator.alloc(ColumnData, schema.columns.len);
        for (schema.columns, 0..) |column, idx| {
            const column_data = try ColumnData.readColumn(allocator, reader, column.typ, row_count);
            columns[idx] = column_data;
        }
        return Block{ .cols = columns };
    }

    pub fn append(self: *Block, allocator: std.mem.Allocator, other: Block) !Block {
        std.debug.assert(self.cols.len == other.cols.len);
        const merged_cols = try allocator.alloc(ColumnData, self.cols.len);
        for (self.cols, other.cols, 0..) |col_left, col_right, col_idx| {
            switch (col_left) {
                .I32 => |vals| {
                    const other_vals = col_right.I32;
                    const new_vals = try allocator.alloc(i32, vals.len + other_vals.len);
                    @memcpy(new_vals[0..vals.len], vals);
                    @memcpy(new_vals[vals.len..], other_vals);
                    merged_cols[col_idx] = ColumnData{ .I32 = new_vals };
                },
                .I64 => |vals| {
                    const other_vals = col_right.I64;
                    const new_vals = try allocator.alloc(i64, vals.len + other_vals.len);
                    @memcpy(new_vals[0..vals.len], vals);
                    @memcpy(new_vals[vals.len..], other_vals);
                    merged_cols[col_idx] = ColumnData{ .I64 = new_vals };
                },
                .F32 => |vals| {
                    const other_vals = col_right.F32;
                    const new_vals = try allocator.alloc(f32, vals.len + other_vals.len);
                    @memcpy(new_vals[0..vals.len], vals);
                    @memcpy(new_vals[vals.len..], other_vals);
                    merged_cols[col_idx] = ColumnData{ .F32 = new_vals };
                },
                .Str => |vals| {
                    merged_cols[col_idx] = ColumnData{ .Str = try vals.append(allocator, col_right.Str) };
                },
            }
        }
        return Block{ .cols = merged_cols };
    }

    pub fn rows(self: Block) usize {
        return self.cols[0].len();
    }

    pub fn printContent(self: Block) void {
        std.debug.print("rows: {d}\n", .{self.rows()});
        for (0..self.rows()) |row_idx| {
            for (self.cols) |col| {
                switch (col) {
                    .I32 => |vals| {
                        std.debug.print("{d}\t\t", .{vals[row_idx]});
                    },
                    .Str => |vals| {
                        std.debug.print("{s}\t\t", .{vals.slices[row_idx]});
                    },
                }
            }
            std.debug.print("\n", .{});
        }
    }
};

pub const BlockFile = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    schema: Schema,
    block_starts: std.ArrayList(u32),

    pub fn init(allocator: std.mem.Allocator, schema: Schema, file_path: []const u8) !BlockFile {
        return BlockFile{
            .allocator = allocator,
            .file_path = try allocator.dupe(u8, file_path),
            .schema = try schema.clone(allocator),
            .block_starts = try std.ArrayList(u32).initCapacity(allocator, 100),
        };
    }

    pub fn deinit(self: *BlockFile) void {
        self.block_starts.deinit(self.allocator);
        self.schema.deinit(self.allocator);
        self.allocator.free(self.file_path);
    }

    pub fn initFromFile(allocator: std.mem.Allocator, file_path: []const u8) !BlockFile {
        var file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_only });
        defer file.close();
        var file_buffer: [SIZE_32KB]u8 = undefined;
        var file_reader = file.reader(&file_buffer);
        const schema = try Schema.read(allocator, &file_reader);
        const block_starts = try BlockFile.readBlockStarts(allocator, &file_reader);

        return BlockFile{
            .allocator = allocator,
            .file_path = try allocator.dupe(u8, file_path),
            .schema = schema,
            .block_starts = block_starts,
        };
    }

    pub fn writeData(self: *BlockFile, data: Block) !void {
        var file = try std.fs.cwd().createFile(self.file_path, .{ .truncate = true });
        defer file.close();
        var write_buffer: [SIZE_32KB]u8 = undefined;
        var writer = file.writer(&write_buffer);
        try self.schema.write(&writer);
        try data.write(self.allocator, &writer, &self.block_starts);
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
        var read_block = try BlockFile.initFromFile(self.allocator, self.file_path);
        defer read_block.deinit();

        if (read_block.block_starts.items.len == 0) {
            try self.writeData(block);
            return;
        }

        var write_buffer: [SIZE_32KB]u8 = undefined;
        var writer = file.writer(&write_buffer);
        const last_block_id: u32 = @intCast(read_block.block_starts.items.len - 1);
        var last_block = try read_block.readBlock(last_block_id);
        defer last_block.deinit(allocator);

        self.block_starts.clearAndFree(allocator);
        try self.block_starts.appendSlice(allocator, read_block.block_starts.items);
        var block_to_append = block;

        if (last_block.rows() < ROWS_PER_BLOCK) {
            block_to_append = try last_block.append(allocator, block);
            const seek_pos: u64 = @intCast(self.block_starts.pop() orelse @panic("no block to pop"));
            try writer.seekTo(seek_pos);
        } else {
            const seek_pos: u64 = try file.getEndPos() - 4 * (self.block_starts.items.len + 1);
            try writer.seekTo(seek_pos); // right after last block
        }
        try block_to_append.write(allocator, &writer, &self.block_starts);
        if (last_block.rows() < ROWS_PER_BLOCK) {
            block_to_append.deinit(allocator);
        }
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

    pub fn readBlock(self: *const BlockFile, block_id: u32) !Block {
        var file = try std.fs.cwd().openFile(self.file_path, .{ .mode = .read_only });
        defer file.close();
        var file_buffer: [4096]u8 = undefined;
        var file_reader = file.reader(&file_buffer);

        std.debug.assert(block_id < self.block_starts.items.len);
        const block_start = self.block_starts.items[block_id];
        return try Block.readBlock(self.allocator, &file_reader, block_start, self.schema);
    }

    pub fn printContent(self: *const BlockFile) !void {
        for (self.schema) |column| {
            std.debug.print("{s}\t\t", .{column.name});
        }
        std.debug.print("\n", .{});
        for (self.block_starts.items, 0..) |_, i| {
            const block = try self.readBlock(@intCast(i));
            block.printContent();
        }
    }
};

test "write -> read block" {
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.testing.allocator;

    const schema = Schema{ .columns = (&[_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
        .{ .typ = TYPE_F32, .name = "float" },
    })[0..] };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3 } };
    var col_str = try StringColumn.init(allocator, &[_][]const u8{ "hello", "zig", "!" });
    const col2 = ColumnData{ .Str = col_str };
    defer col_str.deinit(allocator);
    const col3 = ColumnData{ .F32 = &[_]f32{ -1.0, 2.0, 3.0 } };
    var block_data = [_]ColumnData{ col1, col2, col3 };
    const block = Block{ .cols = block_data[0..] };

    var write_block_file = try BlockFile.init(allocator, schema, "tmp/test.bin");
    defer write_block_file.deinit();
    try write_block_file.writeData(block);

    var read_block_file = try BlockFile.initFromFile(allocator, "tmp/test.bin");
    defer read_block_file.deinit();
    var read_block_data = try read_block_file.readBlock(0);
    defer read_block_data.deinit(allocator);

    try std.testing.expectEqualDeep(block, read_block_data);
}

test "write -> append -> read block" {
    var tmp = std.testing.tmpDir(.{}); // options: .{ .keep = true } if you want to inspect files
    defer tmp.cleanup();
    const allocator = std.testing.allocator;

    const schema = Schema{ .columns = (&[_]ColumnSchema{
        .{ .typ = TYPE_I32, .name = "delta" },
        .{ .typ = TYPE_STR, .name = "msg" },
    })[0..] };
    const col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3 } };
    var col_str = try StringColumn.init(allocator, &[_][]const u8{ "hello", "zig", "!" });
    const col2 = ColumnData{ .Str = col_str };
    defer col_str.deinit(allocator);
    var block_data = [_]ColumnData{ col1, col2 };
    const block = Block{ .cols = block_data[0..] };

    var write_block_file = try BlockFile.init(allocator, schema, "tmp/test2.bin");
    defer write_block_file.deinit();
    try write_block_file.writeData(block);
    try write_block_file.appendData(allocator, block);

    var read_block_file = try BlockFile.initFromFile(allocator, "tmp/test2.bin");
    defer read_block_file.deinit();
    var read_block_data = try read_block_file.readBlock(0);
    defer read_block_data.deinit(allocator);

    const expected_col1 = ColumnData{ .I32 = &[_]i32{ -1, 2, 3, -1, 2, 3 } };
    var expected_col_str = try StringColumn.init(allocator, &[_][]const u8{ "hello", "zig", "!", "hello", "zig", "!" });
    defer expected_col_str.deinit(allocator);
    const expected_col2 = ColumnData{ .Str = expected_col_str };
    var expected_block_data = [_]ColumnData{ expected_col1, expected_col2 };
    const expected_block = Block{ .cols = expected_block_data[0..] };
    try std.testing.expectEqualDeep(expected_block, read_block_data);
}
