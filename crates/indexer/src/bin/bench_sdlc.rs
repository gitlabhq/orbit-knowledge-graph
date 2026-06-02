//! SDLC indexing throughput benchmark.
//!
//! Reports end-to-end and ClickHouse ingestion throughput for `run_plan`
//! against a real ClickHouse. Build/run with:
//!
//! ```sh
//! cargo run -p indexer --features bench-sdlc --release --bin bench_sdlc
//! ```
//!
//! Peak heap is measured externally to keep this crate free of a custom
//! (`unsafe`) global allocator — wrap the run in the OS reporter, e.g. on macOS
//! `/usr/bin/time -l <bin>` ("maximum resident set size") or on Linux
//! `/usr/bin/time -v <bin>` ("Maximum resident set size").
//!
//! Tunable via env: `BENCH_CH_URL`, `BENCH_CH_DATABASE`, `BENCH_CH_USER`,
//! `BENCH_CH_PASSWORD`, `BENCH_ROWS`, `BENCH_BATCH_SIZE`, `BENCH_BLOCK_SIZE`,
//! `BENCH_CHANNEL_CAP`, `BENCH_DESC_LEN`.

use indexer::modules::sdlc::bench::{BenchConfig, Benchmark};

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = BenchConfig::from_env();
    eprintln!(
        "[bench] config: rows={} batch_size={} block_size={} channel_cap={} desc_len={} url={}",
        config.rows,
        config.batch_size,
        config.stream_block_size,
        config.channel_capacity,
        config.description_len,
        config.url
    );

    eprintln!("[bench] seeding + setup...");
    let benchmark = Benchmark::setup(&config).await;

    eprintln!("[bench] running pipeline...");
    let stats = benchmark.run().await;

    println!("================ SDLC bench result ================");
    println!("rows_written         : {}", stats.rows_written);
    println!(
        "total_elapsed        : {:.3} s",
        stats.total_elapsed.as_secs_f64()
    );
    println!(
        "throughput           : {:.0} rows/s",
        stats.throughput_rows_per_sec()
    );
    println!(
        "ingestion_elapsed    : {:.3} s",
        stats.ingestion_elapsed.as_secs_f64()
    );
    println!(
        "ingestion_throughput : {:.0} rows/s",
        stats.ingestion_rows_per_sec()
    );
    println!(
        "arrow_bytes_written  : {:.1} MiB",
        mib(stats.bytes_written as usize)
    );
    println!("===================================================");
}
