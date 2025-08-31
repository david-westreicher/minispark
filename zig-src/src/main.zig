const std = @import("std");
const Job = @import("executor").Job;
const STAGES = @import("stage.zig").STAGES;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    const job = try Job.fromArgs(&args);

    // TODO(david): Will the compiler not optimize the code of the stages?
    _ = try STAGES[job.stage_id](allocator, job);
}
