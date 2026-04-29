//! Named histogram bucket sets shared across the metric catalog.
//!
//! Every histogram in [`crate::catalog`] references one of these constants
//! so that bucket choice is a review conversation at the catalog level, not a
//! per-call-site decision.

/// General request-latency buckets (5 ms to 10 s). Suitable for most ETL and
/// query pipelines.
pub const LATENCY: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// Coarse pipeline buckets (100 ms to 5 min). For operations that routinely
/// exceed 10 s: project indexing, repository fetch, full pipeline runs.
pub const LATENCY_SLOW: &[f64] = &[
    0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0,
];

/// Tighter latency buckets for Gitaly, content resolution, and other
/// sub-second RPC paths.
pub const LATENCY_FAST: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Batch-size distribution from single-row through moderate bulk operations.
pub const BATCH_SIZE: &[f64] = &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0];

/// Blob payload-size distribution (256 bytes through 16 MB).
pub const BLOB_BYTES: &[f64] = &[
    256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0,
];

/// Result-set row counts (1 row through ~1 M rows).
pub const ROW_COUNT: &[f64] = &[1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0];

/// Memory-usage buckets (1 MB through 10 GB). Used for per-query peak memory.
pub const MEMORY_BYTES: &[f64] = &[
    1_048_576.0,
    10_485_760.0,
    104_857_600.0,
    1_073_741_824.0,
    10_737_418_240.0,
];
