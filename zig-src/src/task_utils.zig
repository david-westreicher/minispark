const std = @import("std");

pub const ColumnData = @import("block_file.zig").ColumnData;
pub const StringColumn = @import("block_file.zig").StringColumn;
pub const Tracer = @import("utils.zig").Tracer;

const PARTITIONS = @import("root.zig").PARTITIONS;

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
        .I64 => |vals| {
            const filtered_vals = try allocator.alloc(i64, output_rows);
            var filter_index: u64 = 0;
            for (vals, condition_col) |input_val, c| {
                if (c) {
                    filtered_vals[filter_index] = input_val;
                    filter_index += 1;
                }
            }
            return ColumnData{ .I64 = filtered_vals };
        },
        .F32 => |vals| {
            const filtered_vals = try allocator.alloc(f32, output_rows);
            var filter_index: u32 = 0;
            for (vals, condition_col) |input_val, c| {
                if (c) {
                    filtered_vals[filter_index] = input_val;
                    filter_index += 1;
                }
            }
            return ColumnData{ .F32 = filtered_vals };
        },
        .Str => |vals| {
            var total_output_len: usize = 0;
            for (vals.slices, condition_col) |string, c| {
                if (c) {
                    total_output_len += string.len;
                }
            }
            var new_total = try allocator.alloc(u8, total_output_len);
            var offset: usize = 0;
            const filtered_vals = try allocator.alloc([]const u8, output_rows);
            var filter_index: u32 = 0;
            for (vals.slices, condition_col) |string, c| {
                if (c) {
                    @memcpy(new_total[offset .. offset + string.len], string);
                    filtered_vals[filter_index] = new_total[offset .. offset + string.len];
                    offset += string.len;
                    filter_index += 1;
                }
            }
            return ColumnData{ .Str = StringColumn{
                .total_buffer = new_total,
                .slices = filtered_vals,
            } };
        },
    }
}

pub fn fill_buckets_F32(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(f32) {
    switch (column) {
        .F32 => |vals| {
            var buckets: [PARTITIONS]std.ArrayList(f32) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList(f32).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
        .I32 => |_| {
            @panic("unexpected type");
        },
        .I64 => |_| {
            @panic("unexpected type");
        },
        .Str => |_| {
            @panic("unexpected type");
        },
    }
}
pub fn fill_buckets_I32(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(i32) {
    switch (column) {
        .I32 => |vals| {
            var buckets: [PARTITIONS]std.ArrayList(i32) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList(i32).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
        .I64 => |_| {
            @panic("unexpected type");
        },
        .F32 => |_| {
            @panic("unexpected type");
        },
        .Str => |_| {
            @panic("unexpected type");
        },
    }
}
pub fn fill_buckets_I64(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(i64) {
    switch (column) {
        .I64 => |vals| {
            var buckets: [PARTITIONS]std.ArrayList(i64) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList(i64).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
        .I32 => |_| {
            @panic("unexpected type");
        },
        .F32 => |_| {
            @panic("unexpected type");
        },
        .Str => |_| {
            @panic("unexpected type");
        },
    }
}
pub fn fill_buckets_Str(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList([]const u8) {
    switch (column) {
        .I32 => |_| {
            @panic("unexpected type");
        },
        .I64 => |_| {
            @panic("unexpected type");
        },
        .F32 => |_| {
            @panic("unexpected type");
        },
        .Str => |vals| {
            var buckets: [PARTITIONS]std.ArrayList([]const u8) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList([]const u8).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals.slices) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return buckets;
        },
    }
}
