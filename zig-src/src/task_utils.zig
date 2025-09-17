const std = @import("std");

pub const ColumnData = @import("block_file.zig").ColumnData;
pub const StringColumn = @import("block_file.zig").StringColumn;
pub const Tracer = @import("utils.zig").Tracer;

const PARTITIONS = @import("root.zig").PARTITIONS;

pub fn filter_numeric_column(comptime T: type, col: []const T, condition_col: []const bool, output_rows: usize, allocator: std.mem.Allocator) ![]T {
    const filtered_vals = try allocator.alloc(T, output_rows);
    var filter_index: usize = 0;
    for (col, condition_col) |input_val, c| {
        if (c) {
            filtered_vals[filter_index] = input_val;
            filter_index += 1;
        }
    }
    return filtered_vals;
}

pub fn filter_column(col: ColumnData, condition_col: []const bool, output_rows: usize, allocator: std.mem.Allocator) !ColumnData {
    switch (col) {
        .I32 => |vals| return ColumnData{ .I32 = try filter_numeric_column(i32, vals, condition_col, output_rows, allocator) },
        .I64 => |vals| return ColumnData{ .I64 = try filter_numeric_column(i64, vals, condition_col, output_rows, allocator) },
        .F32 => |vals| return ColumnData{ .F32 = try filter_numeric_column(f32, vals, condition_col, output_rows, allocator) },
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

pub const BucketData = union(enum) {
    I32: [PARTITIONS]std.ArrayList(i32),
    I64: [PARTITIONS]std.ArrayList(i64),
    F32: [PARTITIONS]std.ArrayList(f32),
    Str: [PARTITIONS]std.ArrayList([]const u8),
};

pub fn fill_buckets_numeric(
    comptime T: type,
    allocator: std.mem.Allocator,
    vals: []const T,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) ![PARTITIONS]std.ArrayList(T) {
    var buckets: [PARTITIONS]std.ArrayList(T) = undefined;
    for (0..PARTITIONS) |i| {
        buckets[i] = try std.ArrayList(T).initCapacity(allocator, bucket_sizes[i]);
    }
    for (partition_per_row, vals) |p, val| {
        try buckets[p].append(allocator, val);
    }
    return buckets;
}

pub fn fill_buckets(
    allocator: std.mem.Allocator,
    column: ColumnData,
    bucket_sizes: [PARTITIONS]u32,
    partition_per_row: []const u8,
) !BucketData {
    return switch (column) {
        .I32 => |vals| BucketData{ .I32 = try fill_buckets_numeric(i32, allocator, vals, bucket_sizes, partition_per_row) },
        .I64 => |vals| BucketData{ .I64 = try fill_buckets_numeric(i64, allocator, vals, bucket_sizes, partition_per_row) },
        .F32 => |vals| BucketData{ .F32 = try fill_buckets_numeric(f32, allocator, vals, bucket_sizes, partition_per_row) },
        .Str => |vals| {
            var buckets: [PARTITIONS]std.ArrayList([]const u8) = undefined;
            for (0..PARTITIONS) |i| {
                buckets[i] = try std.ArrayList([]const u8).initCapacity(allocator, bucket_sizes[i]);
            }
            for (partition_per_row, vals.slices) |p, val| {
                try buckets[p].append(allocator, val);
            }
            return BucketData{ .Str = buckets };
        },
    };
}
