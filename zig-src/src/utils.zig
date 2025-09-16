const std = @import("std");

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

pub const AnyList = union(enum) {
    IntList: std.ArrayList(i32),
    Int64List: std.ArrayList(i64),
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
