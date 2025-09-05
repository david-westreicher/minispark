const std = @import("std");
const Executor = @import("executor");
const Job = @import("executor").Job;
const Tracer = @import("executor").Tracer;
const STAGES = @import("stage.zig").STAGES;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    Executor.GLOBAL_TRACER = try Tracer.init(allocator);
    try Executor.GLOBAL_TRACER.startEvent("executor start");

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    const job = try Job.fromArgs(allocator, &args);
    const job_string = try std.fmt.allocPrint(allocator, "block_id_{d}", .{job.input_block_id});
    try Executor.GLOBAL_TRACER.startEvent(job_string);

    // TODO(david): Will the compiler not optimize the code of the stages?
    _ = try STAGES[job.stage_id](allocator, job);

    try Executor.GLOBAL_TRACER.endEvent(job_string);
    try Executor.GLOBAL_TRACER.endEvent("executor start");
    try Executor.GLOBAL_TRACER.save(job.trace_file);
}
