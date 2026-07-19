const std = @import("std");
const geometry = @import("geometry.zig");

pub fn main() !void {
    var total: f64 = 0.0;
    const p = geometry.Point.init(3.0, 4.0);
    total += p.normSquared();

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        total += geometry.distanceSquared(geometry.origin, p);
    }

    if (total > 0.0) {
        std.debug.print("total: {d}\n", .{total});
    } else {
        std.debug.print("empty\n", .{});
    }
}

test "main smoke" {
    try std.testing.expect(true);
}
