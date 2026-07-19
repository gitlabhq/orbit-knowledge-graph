const std = @import("std");

/// Struct container declared as a value binding — the primary Zig
/// definition shape. Methods inside get FQNs scoped as `Point.<fn>`.
pub const Point = struct {
    x: f64,
    y: f64,

    pub fn init(x: f64, y: f64) Point {
        return Point{ .x = x, .y = y };
    }

    pub fn normSquared(self: Point) f64 {
        return self.x * self.x + self.y * self.y;
    }
};

pub const Shape = enum {
    circle,
    square,
    triangle,
};

pub const Value = union(enum) {
    int: i64,
    float: f64,
};

pub const Context = opaque {};

pub const origin = Point{ .x = 0.0, .y = 0.0 };
pub var counter: u32 = 0;

pub fn distanceSquared(a: Point, b: Point) f64 {
    const dx = a.x - b.x;
    const dy = a.y - b.y;
    return dx * dx + dy * dy;
}

test "point init works" {
    const p = Point.init(1.0, 2.0);
    try std.testing.expectEqual(@as(f64, 5.0), p.normSquared());
}

// Anonymous test block: valid Zig, always runs, cannot be excluded by
// name-based test filtering. Indexed via a synthesized `test@L<line>` name.
test {
    const d = distanceSquared(origin, Point.init(3.0, 4.0));
    try std.testing.expectEqual(@as(f64, 25.0), d);
}
