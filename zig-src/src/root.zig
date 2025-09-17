const std = @import("std");

pub const block_file = @import("block_file.zig");
pub const job = @import("job.zig");
pub const tasks = @import("tasks.zig");
pub const task_utils = @import("task_utils.zig");
pub const utils = @import("utils.zig");

pub const PARTITIONS = 16;
pub var GLOBAL_TRACER: utils.Tracer = undefined;

pub const Error = error{
    NameTooLong,
    StringTooLong,
    SchemaTooLarge,
    UnknownType,
    UnexpectedState,
};

test "all" {
    _ = @import("block_file.zig");
    _ = @import("job.zig");
    _ = @import("task_utils.zig");
    _ = @import("tasks.zig");
    _ = @import("utils.zig");
}
