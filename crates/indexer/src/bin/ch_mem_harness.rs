//! Dev-only: measure ClickHouse-path indexing memory. See indexer::mem_harness.
//! Uses mimalloc to match the server's global allocator.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let mut args = std::env::args().skip(1);
    let repo = args
        .next()
        .expect("usage: ch-mem-harness <repo-path> [threads]");
    let threads: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(8);
    indexer::mem_harness::run(std::path::Path::new(&repo), threads);
}
