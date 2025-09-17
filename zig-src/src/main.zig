const std = @import("std");

const Executor = @import("executor");
const Job = @import("executor").job.Job;
const Tracer = @import("executor").utils.Tracer;
const STAGES = @import("stage.zig").STAGES;

const Arguments = struct {
    worker_id: []const u8,
    output_path: []const u8,
    trace_file: []const u8,

    pub fn parse(allocator: std.mem.Allocator, it: *std.process.ArgIterator) !Arguments {
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

        return Arguments{
            .worker_id = try allocator.dupe(u8, args[0]),
            .output_path = try allocator.dupe(u8, args[1]),
            .trace_file = try allocator.dupe(u8, args[2]),
        };
    }
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    const arguments = try Arguments.parse(allocator, &args);
    Executor.GLOBAL_TRACER = try Tracer.init(allocator);
    try Executor.GLOBAL_TRACER.startEvent("thread running");
    const stdin = std.fs.File.stdin();
    defer stdin.close();
    var stdin_buffer: [4096]u8 = undefined;
    var stdin_reader = stdin.reader(&stdin_buffer);
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    while (true) {
        var job_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const job_allocator = job_arena.allocator();
        defer job_arena.deinit();

        const job = (try Job.fromFile(job_allocator, &stdin_reader, arguments.output_path)) orelse break;
        const job_string = try std.fmt.allocPrint(allocator, "block_id_{d}", .{job.input_block_id});
        try Executor.GLOBAL_TRACER.startEvent(job_string);

        _ = try STAGES[job.stage_id](job_allocator, job);
        try stdout.print("job_finished 0\n", .{});
        try stdout.flush();

        try Executor.GLOBAL_TRACER.endEvent(job_string);
    }
    try Executor.GLOBAL_TRACER.endEvent("thread running");
    try Executor.GLOBAL_TRACER.save(arguments.trace_file);
}
