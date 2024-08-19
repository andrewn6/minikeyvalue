const std = @import("std");
const http = @import("http")
const json = @import("json")
const uuid = @import("uuid")

const ListResponse = struct {
    next: []const u8,
    keys: []const []const u8.
};

pub fn handleRequest(app: *App, req: http.Request) !http.Response {
    const path = req.uri.path;
    const key = path;

    return switch (req.method) {
        .GET => handleGet(app, key, req),
        .PUT => handlePut(app, key, req),
        .DELETE => handleDelete(app, key, req, false),
        .POST => handlePost(app, key, req),
        else => http.Response{
            .status = 403,
            .body = "Forbidden",
        },
    };
}

fn handleS3ListQuery(app: *App, key: []const u8, req: http.Request) !http.Response {
    const prefix = req.uri.query.get("prefix") orelse "";
    const full_key = std.fmt.allocPrint("{}/{}", .{key, prefix}) catch return http.Response{
        .status = 500,
        .body = "Internal Server Error",
    };

    var ret = std.StringWriter.init(std.heap.page_allocator);
    defer ret.deinit();

    var iter = app.db.prefixIterator(full_key);
    while (iter.next()) |item| {
        const k = item.key;
        const v = item.value;
        const rec = try json.parse(Record, v);
        if (rec.deleted != Deleted.No) continue;
        try ret.print("<Key>{}</Key>", .{k[full_key.len()..]});
    }

    const body = try ret.toOwnedSlice();
    return http.Response{
        .status = 200,
        .body = body,
    };
}

fn handleQuery(app: *App, key: []const u8, req: http.Request) !http.Response {
    const query = req.uri.query;
    const operation = query.get("operation") orelse "";

    if (operation == "list" or operation == "unlinked") {
        const start = query.get("start") orelse "";
        const limit = query.get("limit").map(|l| std.fmt.parseInt(usize, l, 10)) orelse 0;

        var keys = std.ArrayList([]const u8).init(std.heap.page_allocator);
        defer keys.deinit();

        var iter = app.db.prefixIterator(key);
        while (iter.next()) |item| {
            const k = item.key;
            const v = item.value;
            const rec = try json.parse(Record, v);

            if ((rec.deleted != Deleted.No and operation == "list") or
                (rec.deleted != Deleted.Soft and operation == "unlinked")) {
                continue;
            }

            if (keys.items.len > 1_000_000) {
                return http.Response{
                    .status = 413,
                    .body = "Payload Too Large",
                };
            }

            if (limit > 0 and keys.items.len == limit) {
                break;
            }

            try keys.append(k);
        }

        const response = ListResponse{
            .next = "",
            .keys = keys.items,
        };
        const body = try json.stringify(response);

        return http.Response{
            .status = 200,
            .headers = &[_]http.Header{
                http.Header{ .name = "Content-Type", .value = "application/json" },
            },
            .body = body,
        };
    } else {
        return http.Response{
            .status = 403,
            .body = "Forbidden",
        };
    }
}

fn handleGet(app: *App, key: []const u8, req: http.Request) !http.Response {
    const rec = app.getRecord(key);

    const remote = if (rec.hash.len > 0) {
        var response = http.Response.builder();
        response = response.header("Content-Md5", rec.hash);

        if (rec.deleted != Deleted.No) {
            if (app.fallback.len == 0) {
                return response.status(404).body("Not Found");
            }
            std.fmt.allocPrint("http://{}{}", .{app.fallback, key2path(key)}) catch return response.status(500).body("Internal Server Error");
        } else {
            const kvolumes = key2volume(key, app.volumes, app.replicas, app.subvolumes);
            if (needsRebalance(rec.rvolumes, kvolumes)) {
                response = response.header("Key-Balance", "unbalanced");
            } else {
                response = response.header("Key-Volumes", rec.rvolumes.join(","));
            }

            var rng = std.rand.DefaultPrng.init(std.time.nanoTimestamp());
            var good = false;
            var chosen_remote = "";

            for (idx in std.rand.chooseMultiple(&rng, rec.rvolumes.len, rec.rvolumes.len)) |i| {
                const remote = std.fmt.allocPrint("http://{}{}", .{rec.rvolumes[i], key2path(key)}) catch return response.status(500).body("Internal Server Error");
                if (app.remoteHead(remote, app.voltimeout)) {
                    good = true;
                    chosen_remote = remote;
                    break;
                }
            }

            if (!good) {
                return response.status(404).body("Not Found");
            }

            chosen_remote
        }
    } else {
        return http.Response.builder().status(404).body("Not Found");
    };

    return http.Response.builder()
        .status(302)
        .header("Location", remote)
        .header("Content-Length", "0")
        .body("");
}

fn handlePut(app: *App, key: []const u8, req: http.Request) !http.Response {
    if (req.headers.get("content-length").map(|v| std.fmt.parseInt(u64, v, 10)) orelse 0 == 0) {
        return http.Response.builder().status(411).body("Length Required");
    }

    const rec = app.getRecord(key);
    if (rec.deleted == Deleted.No) {
        return http.Response.builder().status(401).body("Unauthorized");
    }

    if (req.uri.query.get("partNumber")) |part_number| {
        const upload_id = req.uri.query.get("uploadId") orelse "";
        if (!app.upload_ids.contains(upload_id)) {
            return http.Response.builder().status(403).body("Forbidden");
        }

        const part_number = std.fmt.parseInt(usize, part_number, 10) catch return http.Response.builder().status(400).body("Bad Request");
        var file = try std.fs.File.create(std.fmt.allocPrint("/tmp/{}-{}", .{upload_id, part_number}));
        defer file.close();

        var body = req.body;
        while (body.next()) |chunk| {
            try file.writeAll(chunk);
        }
        return http.Response.builder().status(200).body("");
    } else {
        var body = req.body;
        var value = std.ArrayList(u8).init(std.heap.page_allocator);
        defer value.deinit();

        while (body.next()) |chunk| {
            try value.appendSlice(chunk);
        }
        const status = try app.writeToReplicas(key, value.items);
        return http.Response.builder().status(status).body("");
    }
}

fn handleDelete(app: *App, key: []const u8, unlink: bool) !http.Response {
    const status = try app.delete(key, unlink);
    return http.Response.builder().status(status).body("");
}

fn handlePost(app: *App, key: []const u8, req: http.Request) !http.Response {
    const rec = app.getRecord(key);
    if (rec.deleted == Deleted.No) {
        return http.Response.builder().status(403).body("Forbidden");
    }

    if (req.uri.query == "uploads") {
        const upload_id = uuid.generate();
        app.upload_ids.put(upload_id, true);
        const body = std.fmt.allocPrint("<InitiateMultipartUploadResult><UploadId>{}</UploadId></InitiateMultipartUploadResult>", .{upload_id});
        return http.Response.builder().status(200).body(body);
    } else if (req.uri.query == "delete") {
        var body = req.body;
        var value = std.ArrayList(u8).init(std.heap.page_allocator);
        defer value.deinit();

        while (body.next()) |chunk| {
            try value.appendSlice(chunk);
        }
        const delete = try json.parse(Delete, value.items);
        for (subkey in delete.keys) |subkey| {
            const full_key = std.fmt.allocPrint("{}/{}", .{key, subkey});
            const status = try app.delete(full_key, false);
            if (status != 204) {
                return http.Response.builder().status(status).body("");
            }
        }
        return http.Response.builder().status(204).body("");
    } else if (req.uri.query.get("uploadId")) |upload_id| {
        if (!app.upload_ids.remove(upload_id)) {
            return http.Response.builder().status(403).body("Forbidden");
        }

        var body = req.body;
        var value = std.ArrayList(u8).init(std.heap.page_allocator);
        defer value.deinit();

        while (body.next()) |chunk| {
            try value.appendSlice(chunk);
        }
        const cmu = try json.parse(CompleteMultipartUpload, value.items);

        var parts = std.ArrayList([]const u8).init(std.heap.page_allocator);
        defer parts.deinit();
        var total_size: u64 = 0;

        for (part in cmu.parts) |part| {
            const filename = std.fmt.allocPrint("/tmp/{}-{}", .{upload_id, part.part_number});
            var file = try std.fs.File.open(filename);
            defer file.close();

            const metadata = try file.metadata();
            total_size += metadata.size;
            var content = try file.readAllAlloc(std.heap.page_allocator);
            defer std.heap.page_allocator.free(content);
            try parts.append(content);
            try std.fs.unlink(filename);
        }

        const combined = try std.mem.concatAlloc(std.heap.page_allocator, u8, parts.items);
        defer std.heap.page_allocator.free(combined);
        const status = try app.writeToReplicas(key, combined);
        return http.Response.builder().status(status).body("");
    } else {
        return http.Response.builder().status(400).body("Bad Request");
    }