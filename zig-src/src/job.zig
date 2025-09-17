const std = @import("std");

pub const Job = struct {
    stage_id: u32 = 0,
    output_file: []const u8,
    input_file: ?[]const u8 = null,
    input_block_id: u32 = 0,
    shuffle_files: ?[][]const u8 = null,
    right_shuffle_files: ?[][]const u8 = null,

    pub fn fromFile(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, output_path: []const u8) !?Job {
        const stage: u8 = try reader.interface.takeInt(u8, .little);
        if (stage == 255) {
            return null;
        }
        const job_type: u8 = try reader.interface.takeInt(u8, .little);
        var job = Job{
            .stage_id = stage,
            .output_file = try std.fmt.allocPrint(allocator, "{s}_result_{d}.bin", .{ output_path, stage }),
        };
        if (job_type == 0) {
            job.input_file = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            job.input_block_id = try reader.interface.takeInt(u32, .little);
            return job;
        }
        if (job_type == 1) {
            const shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const shuffle_files = try allocator.alloc([]const u8, shuffle_files_num);
            for (0..shuffle_files_num) |i| {
                shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.shuffle_files = shuffle_files;
            return job;
        }
        if (job_type == 2) {
            const shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const shuffle_files = try allocator.alloc([]const u8, shuffle_files_num);
            for (0..shuffle_files_num) |i| {
                shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.shuffle_files = shuffle_files;
            const right_shuffle_files_num = try reader.interface.takeInt(u32, .little);
            const right_shuffle_files = try allocator.alloc([]const u8, right_shuffle_files_num);
            for (0..right_shuffle_files_num) |i| {
                right_shuffle_files[i] = try reader.interface.readAlloc(allocator, try reader.interface.takeInt(u8, .little));
            }
            job.right_shuffle_files = right_shuffle_files;
            return job;
        }
        return null;
    }
};

pub const OutputFile = struct {
    file_path: []const u8,
    partition_id: u8,
};
