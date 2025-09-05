const std = @import("std");

pub const TYPE_STR = @import("block_file.zig").TYPE_STR;
pub const TYPE_I32 = @import("block_file.zig").TYPE_I32;
pub const BlockFile = @import("block_file.zig").BlockFile;
pub const Block = @import("block_file.zig").Block;
pub const ColumnData = @import("block_file.zig").ColumnData;
pub const ColumnSchema = @import("block_file.zig").ColumnSchema;

pub const PARTITIONS = 16;

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
    worker_id: []const u8,
    output_file: []const u8,
    trace_file: []const u8,
    input_file: ?[]const u8 = null,
    input_block_id: u32 = 0,
    shuffle_files: ?[][]const u8 = null,

    pub fn fromArgs(allocator: std.mem.Allocator, it: *std.process.ArgIterator) !Job {
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
            .worker_id = try allocator.dupe(u8, args[1]),
            .output_file = try allocator.dupe(u8, args[2]),
            .trace_file = try allocator.dupe(u8, args[3]),
        };
        const job_type = try std.fmt.parseInt(u32, args[4], 10);
        if (job_type == 0) {
            job.input_file = try allocator.dupe(u8, args[5]);
            job.input_block_id = try std.fmt.parseInt(u32, args[6], 10);
            return job;
        }
        if (job_type == 1) {
            const shuffle_files_num = try std.fmt.parseInt(u32, args[5], 10);
            const shuffle_files = try allocator.alloc([]const u8, shuffle_files_num);
            for (0..shuffle_files_num) |i| {
                shuffle_files[i] = try allocator.dupe(u8, args[6 + i]);
            }
            job.shuffle_files = shuffle_files;
            return job;
        }
        return Error.UnknownType;
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
