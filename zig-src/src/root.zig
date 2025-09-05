const std = @import("std");

pub const PARTITIONS = 16;
pub const TYPE_I32: u8 = 0;
pub const TYPE_STR: u8 = 1;

pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
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

    pub fn startEvent(self: *Tracer, name: []const u8) !void {
        try self.events.append(self.allocator, TraceEvent{
            .name = name,
            .time = self.timer.read(),
            .is_start = true,
        });
    }

    pub fn endEvent(self: *Tracer, name: []const u8) !void {
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
    trace_file: []const u8,
    input_file: []const u8 = undefined,
    input_block_id: u32 = undefined,
    shuffle_files: [][]const u8 = undefined,

    pub fn fromArgs(it: *std.process.ArgIterator) !Job {
        var args: [1024][]const u8 = undefined;
        var idx: usize = 0;

        while (it.next()) |arg| {
            if (idx == 0) {
                idx += 1;
                continue;
            }
            args[idx - 1] = arg;
            idx += 1;
        }

        var job = Job{
            .stage_id = try std.fmt.parseInt(u32, args[0], 10),
            .output_file = args[1],
            .trace_file = args[2],
        };
        const job_type = try std.fmt.parseInt(u32, args[3], 10);
        if (job_type == 0) {
            job.input_file = args[4];
            job.input_block_id = try std.fmt.parseInt(u32, args[5], 10);
            return job;
        }
        if (job_type == 1) {
            const shuffle_files_num = try std.fmt.parseInt(u32, args[4], 10);
            job.shuffle_files = args[5 .. 5 + shuffle_files_num];
            return job;
        }
        return Error.UnknownType;
    }
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

    pub fn serialize(self: ColumnData, allocator: std.mem.Allocator, file: anytype) !void {
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
                var total_size: usize = 0;
                for (vals) |s| {
                    total_size += 1;
                    total_size += s.len;
                }

                var buf = try allocator.alloc(u8, total_size);
                defer allocator.free(buf);
                var i: usize = 0;
                for (vals) |s| {
                    buf[i] = @intCast(s.len);
                    i += 1;
                }
                for (vals) |s| {
                    @memcpy(buf[i..][0..s.len], s);
                    i += s.len;
                }
                try file.writeAll(buf);
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

    pub fn serialize(self: Block, allocator: std.mem.Allocator, file: anytype) !void {
        const row_count: u32 = @intCast(self.cols[0].len());
        try file.writeAll(std.mem.asBytes(&row_count));
        for (self.cols) |col| {
            try col.serialize(allocator, file);
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
        try data.serialize(self.allocator, file);
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

pub fn fill_buckets(
    comptime T: type,
    allocator: std.mem.Allocator,
    column: []const T,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(T) {
    var buckets: [PARTITIONS]std.ArrayList(T) = undefined;
    for (0..PARTITIONS) |i| {
        buckets[i] = try std.ArrayList(T).initCapacity(allocator, bucket_sizes[i]);
    }
    for (partition_per_row, 0..) |p, row| {
        try buckets[p].append(allocator, column[row]);
    }
    return buckets;
}

pub const TaskResult = struct {
    chunk: ?[]const ColumnData = null,
    is_last: bool = false,
};

pub const OutputFile = struct {
    file_path: []const u8,
    partition_id: u8,
};

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
