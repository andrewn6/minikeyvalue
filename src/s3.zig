const std = @import("std");
const xml = @import("xml");

const CompleteMultipartUpload = struct {
    parts: []Part,
};

const Part = struct {
    part_number: u32,
}

const Delete = struct {
    keys: []const u8,
};

fn parseXML(allocator: *std.mem.Allocator, reader: anytype, dat: anytype) !void {
    var buffer = try std.io.readAllAlloc(allocator, reader);   
    defer allocator.free(buffer);

    try xml.unmarshal(buffer, dat);
}

fn parseCompleteMultipartUpload(allocator: *std.mem.Allocator, reader: anytype) !CompleteMultipartUpload {
    var cmu = CompleteMultipartUpload{ .parts = &[_]Part{} };
    try parseXML(allocator, reader, &cmu);
    return cmu;
}

fn parseDelete(allocator: *std.mem.Allocator, reader: anytype) !Delete {
    var del = Delete{ .keys = &[_]u8{} };
    try parseXML(allocator, reader, &del);
    return del;
}