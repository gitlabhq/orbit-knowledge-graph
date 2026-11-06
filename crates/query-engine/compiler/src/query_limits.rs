/// An id_range wider than this is an unfiltered scan, not a selective node.
pub const MAX_ID_RANGE_SPAN: i64 = 100_000;
/// Path finding seeds a frontier per anchor, so endpoint ranges stay at the pinned-id cap.
pub const MAX_PATH_ANCHOR_RANGE: i64 = 500;
pub const MAX_PATH_ANCHOR_LIMIT: i64 = 500;
