//! Validation harness: three call sites with known live sizes, arranged so the
//! RSS peak happens while two of them are live at once. A correct profiler must
//! rank those two at the top and place the peak inside the overlap window.

use std::hint::black_box;
use std::thread::sleep;
use std::time::Duration;

#[global_allocator]
static ALLOC: memprof::ProfAlloc = memprof::ProfAlloc;

#[inline(never)]
fn persistent_80mib() -> Vec<Vec<u8>> {
    let mut v = Vec::new();
    for _ in 0..80 {
        v.push(vec![7u8; 1 << 20]);
    }
    v
}

#[inline(never)]
fn transient_40mib() -> Vec<Vec<u8>> {
    let mut v = Vec::new();
    for _ in 0..40 {
        v.push(vec![3u8; 1 << 20]);
    }
    v
}

#[inline(never)]
fn steady_small_work() {
    let mut acc = 0u64;
    for i in 0..300_000u64 {
        // A real heap allocation well below the capture threshold, so the
        // profiler must ignore it: proves small allocations stay off the report.
        let v: Vec<u64> = Vec::from_iter(std::iter::repeat_n(i, 8));
        acc = acc.wrapping_add(black_box(v)[0]);
    }
    black_box(acc);
}

fn main() {
    let out = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "memprof-demo".into());
    let _session = memprof::start(&out);

    steady_small_work();

    let held = persistent_80mib();
    black_box(&held);
    sleep(Duration::from_millis(40));

    {
        let spike = transient_40mib(); // peak: 80 + 40 live together
        black_box(&spike);
        sleep(Duration::from_millis(80));
    } // 40 MiB freed here

    sleep(Duration::from_millis(40));
    steady_small_work();
    black_box(&held);
}
