use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

#[cfg(feature = "track-alloc")]
pub use tracking::Tracking;

#[cfg(feature = "track-alloc")]
mod tracking {
    use std::alloc::{GlobalAlloc, Layout};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

    use super::AllocStats;

    const SHARDS: usize = 64;

    /// Per-thread allocation traffic between two high-water checks. Every check
    /// sums the shards exactly, so the peak is not a function of sampler cadence;
    /// the residual error is one threshold's worth of traffic per running thread.
    const PEAK_CHECK_BYTES: i64 = 64 * 1024;

    /// One counter set per cache line. A single global counter costs more than the
    /// allocation it measures once 14 rayon workers hammer it, so live bytes are
    /// sharded and summed by the sampler instead of maintained as one hot word.
    #[repr(align(128))]
    struct Shard {
        live: AtomicI64,
        allocs: AtomicU64,
        alloc_bytes: AtomicU64,
        /// `mi_good_size` of every live allocation. The spread against `live` is
        /// the size-class rounding the allocator adds on top of what was asked for.
        live_rounded: AtomicI64,
    }

    impl Shard {
        const fn new() -> Self {
            Self {
                live: AtomicI64::new(0),
                allocs: AtomicU64::new(0),
                alloc_bytes: AtomicU64::new(0),
                live_rounded: AtomicI64::new(0),
            }
        }
    }

    #[allow(
        clippy::declare_interior_mutable_const,
        reason = "array initializer for a const-constructible shard"
    )]
    const SHARD_INIT: Shard = Shard::new();
    static COUNTERS: [Shard; SHARDS] = [SHARD_INIT; SHARDS];

    static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);
    static PEAK_LIVE: AtomicI64 = AtomicI64::new(0);

    thread_local! {
        static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
        static UNCHECKED: Cell<i64> = const { Cell::new(0) };
    }

    #[inline]
    fn slot() -> usize {
        SLOT.with(|s| {
            let mut v = s.get();
            if v == usize::MAX {
                v = NEXT_SLOT.fetch_add(1, Ordering::Relaxed) % SHARDS;
                s.set(v);
            }
            v
        })
    }

    /// Sum the shards and raise the recorded peak. Called from the allocation path
    /// once a thread has moved [`PEAK_CHECK_BYTES`], which costs one cache-line read
    /// per shard rather than a contended `fetch_max` on every allocation.
    #[inline]
    fn check_peak() {
        let mut total = 0i64;
        for c in &COUNTERS {
            total += c.live.load(Ordering::Relaxed);
        }
        PEAK_LIVE.fetch_max(total, Ordering::Relaxed);
    }

    #[inline]
    fn note(delta: i64) {
        let pending = UNCHECKED.with(|u| {
            let v = u.get() + delta;
            u.set(if v.abs() >= PEAK_CHECK_BYTES { 0 } else { v });
            v
        });
        if pending.abs() >= PEAK_CHECK_BYTES {
            check_peak();
        }
    }

    #[cfg(feature = "good-size")]
    #[inline]
    fn good_size(size: usize) -> i64 {
        #[allow(unsafe_code)]
        unsafe {
            libmimalloc_sys::mi_good_size(size) as i64
        }
    }

    #[cfg(not(feature = "good-size"))]
    #[inline]
    fn good_size(_size: usize) -> i64 {
        0
    }

    /// Global allocator wrapper that records requested bytes. It measures what the
    /// program asked for, not what the allocator mapped; compare against
    /// [`process_memory`] for the allocator overhead and fragmentation on top.
    pub struct Tracking<A>(pub A);

    #[allow(unsafe_code)]
    unsafe impl<A: GlobalAlloc> GlobalAlloc for Tracking<A> {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { self.0.alloc(layout) };
            if !ptr.is_null() {
                let c = &COUNTERS[slot()];
                c.live.fetch_add(layout.size() as i64, Ordering::Relaxed);
                c.allocs.fetch_add(1, Ordering::Relaxed);
                c.alloc_bytes
                    .fetch_add(layout.size() as u64, Ordering::Relaxed);
                c.live_rounded
                    .fetch_add(good_size(layout.size()), Ordering::Relaxed);
                note(layout.size() as i64);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let c = &COUNTERS[slot()];
            c.live.fetch_sub(layout.size() as i64, Ordering::Relaxed);
            c.live_rounded
                .fetch_sub(good_size(layout.size()), Ordering::Relaxed);
            unsafe { self.0.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let c = &COUNTERS[slot()];
            // A grow that has to move holds the old and the new block at once while
            // it copies. Net accounting never shows more than the new size, so every
            // doubling of the largest arrays is invisible to the high-water mark.
            // Charging the new block before the call exposes it, at the cost of
            // overstating grows the allocator satisfies in place, which is why this
            // is a separate mode rather than the default.
            if cfg!(feature = "realloc-pessimistic") {
                c.live.fetch_add(new_size as i64, Ordering::Relaxed);
                c.live_rounded
                    .fetch_add(good_size(new_size), Ordering::Relaxed);
                note(new_size as i64);
            }
            let out = unsafe { self.0.realloc(ptr, layout, new_size) };
            if cfg!(feature = "realloc-pessimistic") {
                if out.is_null() {
                    c.live.fetch_sub(new_size as i64, Ordering::Relaxed);
                    c.live_rounded
                        .fetch_sub(good_size(new_size), Ordering::Relaxed);
                    return out;
                }
                c.live.fetch_sub(layout.size() as i64, Ordering::Relaxed);
                c.live_rounded
                    .fetch_sub(good_size(layout.size()), Ordering::Relaxed);
            } else if !out.is_null() {
                c.live
                    .fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
                c.live_rounded.fetch_add(
                    good_size(new_size) - good_size(layout.size()),
                    Ordering::Relaxed,
                );
                note(new_size as i64 - layout.size() as i64);
            }
            if !out.is_null() {
                c.allocs.fetch_add(1, Ordering::Relaxed);
                if new_size > layout.size() {
                    c.alloc_bytes
                        .fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
                }
            }
            out
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { self.0.alloc_zeroed(layout) };
            if !ptr.is_null() {
                let c = &COUNTERS[slot()];
                c.live.fetch_add(layout.size() as i64, Ordering::Relaxed);
                c.allocs.fetch_add(1, Ordering::Relaxed);
                c.alloc_bytes
                    .fetch_add(layout.size() as u64, Ordering::Relaxed);
                c.live_rounded
                    .fetch_add(good_size(layout.size()), Ordering::Relaxed);
                note(layout.size() as i64);
            }
            ptr
        }
    }

    pub fn alloc_stats() -> AllocStats {
        let mut out = AllocStats::default();
        for c in &COUNTERS {
            out.live_bytes += c.live.load(Ordering::Relaxed);
            out.total_allocs += c.allocs.load(Ordering::Relaxed);
            out.total_alloc_bytes += c.alloc_bytes.load(Ordering::Relaxed);
            out.live_rounded_bytes += c.live_rounded.load(Ordering::Relaxed);
        }
        out.live_bytes_peak = PEAK_LIVE.load(Ordering::Relaxed).max(out.live_bytes);
        out
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AllocStats {
    pub live_bytes: i64,
    /// High-water mark maintained on the allocation path, so it does not depend on
    /// the sampler catching the instant the peak happened.
    pub live_bytes_peak: i64,
    pub total_allocs: u64,
    pub total_alloc_bytes: u64,
    /// Zero unless the `good-size` feature is on.
    pub live_rounded_bytes: i64,
}

#[cfg(feature = "track-alloc")]
pub use tracking::alloc_stats;

/// Without the tracking allocator there is no requested-bytes signal; the
/// report falls back to RSS and phys_footprint alone.
#[cfg(not(feature = "track-alloc"))]
pub fn alloc_stats() -> AllocStats {
    AllocStats::default()
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct ProcessMemory {
    pub resident_bytes: u64,
    /// macOS phys_footprint: the number jetsam and Activity Monitor use.
    /// Counts dirty anonymous pages plus compressed pages, excludes clean
    /// file-backed pages, so it tracks allocator demand better than RSS.
    pub footprint_bytes: u64,
    /// The kernel's own high-water mark for the above. Exact regardless of how
    /// often the sampler runs, so the gap against the sampled maximum is a direct
    /// measure of what the sampler missed.
    pub lifetime_max_footprint_bytes: u64,
}

/// mimalloc's view of the same process. `peak_commit` is maintained by the
/// allocator on every commit, so like the kernel high-water mark it does not
/// depend on sampling; the spread against requested bytes is the allocator's
/// own overhead and retention.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AllocatorInfo {
    pub current_commit_bytes: u64,
    pub peak_commit_bytes: u64,
    pub page_faults: u64,
}

#[allow(unsafe_code)]
pub fn allocator_info() -> AllocatorInfo {
    let (mut elapsed, mut user, mut sys) = (0usize, 0usize, 0usize);
    let (mut rss, mut peak_rss) = (0usize, 0usize);
    let (mut commit, mut peak_commit, mut faults) = (0usize, 0usize, 0usize);
    unsafe {
        libmimalloc_sys::mi_process_info(
            &mut elapsed,
            &mut user,
            &mut sys,
            &mut rss,
            &mut peak_rss,
            &mut commit,
            &mut peak_commit,
            &mut faults,
        );
    }
    AllocatorInfo {
        current_commit_bytes: commit as u64,
        peak_commit_bytes: peak_commit as u64,
        page_faults: faults as u64,
    }
}

/// Ask mimalloc to return everything it is holding but not using. Production never
/// calls this; a settled reading with and without it separates pages the allocator
/// is caching from pages something still owns.
#[allow(unsafe_code)]
pub fn allocator_collect() {
    unsafe { libmimalloc_sys::mi_collect(true) }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn process_memory() -> ProcessMemory {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as i32,
            libc::RUSAGE_INFO_V4,
            (&mut info as *mut libc::rusage_info_v4).cast(),
        )
    };
    if rc != 0 {
        return ProcessMemory::default();
    }
    ProcessMemory {
        resident_bytes: info.ri_resident_size,
        footprint_bytes: info.ri_phys_footprint,
        lifetime_max_footprint_bytes: info.ri_lifetime_max_phys_footprint,
    }
}

/// Linux has no `phys_footprint`. `RssAnon` is the closest analogue: it counts
/// the dirty anonymous pages the allocator owns and drops them the moment
/// `MADV_DONTNEED` lands, which is what mimalloc's purge does here but not on
/// macOS, where the same pages stay in `VmRSS`.
#[cfg(target_os = "linux")]
pub fn process_memory() -> ProcessMemory {
    let mut out = ProcessMemory::default();
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return out;
    };
    for line in status.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let kb: u64 = value
            .split_whitespace()
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        match key {
            "VmRSS" => out.resident_bytes = kb * 1024,
            "RssAnon" => out.footprint_bytes = kb * 1024,
            "VmHWM" => out.lifetime_max_footprint_bytes = kb * 1024,
            _ => {}
        }
    }
    out
}

static PHASE: Mutex<String> = Mutex::new(String::new());
pub fn set_phase(name: &str) {
    set_phase_quiet(name);
    tracing::info!(target: "profiler", phase = name, "phase");
}

/// For callers already inside a tracing layer, where emitting an event would
/// re-enter the subscriber.
pub fn set_phase_quiet(name: &str) {
    if let Ok(mut p) = PHASE.lock() {
        p.clear();
        p.push_str(name);
    }
}

fn current_phase() -> String {
    PHASE.lock().map(|p| p.clone()).unwrap_or_default()
}

#[derive(serde::Serialize)]
struct Sample {
    t_ms: u128,
    phase: String,
    rss: u64,
    footprint: u64,
    alloc_live: i64,
    alloc_live_rounded: i64,
    commit: u64,
    total_allocs: u64,
    total_alloc_bytes: u64,
}

pub struct Sampler {
    stop: &'static AtomicBool,
    handle: Option<std::thread::JoinHandle<Peaks>>,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Peaks {
    pub max_rss: u64,
    pub max_footprint: u64,
    pub max_alloc_live: i64,
    /// Sampled maxima above; exact maxima below. Comparing the two pairs says how
    /// much of a peak a 30 ms sampler cadence lets through.
    pub exact_max_footprint: u64,
    pub exact_max_alloc_live: i64,
    pub exact_max_commit: u64,
    pub max_alloc_live_rounded: i64,
    pub samples: u64,
    pub max_sample_gap_ms: u64,
}

static STOP: AtomicBool = AtomicBool::new(false);

impl Sampler {
    pub fn start(path: &Path, hz: u32, start: Instant) -> anyhow::Result<Self> {
        STOP.store(false, Ordering::Release);
        let file = std::fs::File::create(path)?;
        let interval = std::time::Duration::from_micros(1_000_000 / u64::from(hz.max(1)));
        let handle = std::thread::Builder::new()
            .name("mem-sampler".into())
            .spawn(move || {
                let mut w = std::io::BufWriter::new(file);
                let mut peaks = Peaks::default();
                let mut previous = start;
                let mut next = std::time::Instant::now();
                while !STOP.load(Ordering::Acquire) {
                    let pm = process_memory();
                    let a = alloc_stats();
                    let ai = allocator_info();
                    peaks.max_rss = peaks.max_rss.max(pm.resident_bytes);
                    peaks.max_footprint = peaks.max_footprint.max(pm.footprint_bytes);
                    peaks.max_alloc_live = peaks.max_alloc_live.max(a.live_bytes);
                    peaks.max_alloc_live_rounded =
                        peaks.max_alloc_live_rounded.max(a.live_rounded_bytes);
                    let now = std::time::Instant::now();
                    peaks.max_sample_gap_ms = peaks
                        .max_sample_gap_ms
                        .max(now.duration_since(previous).as_millis() as u64);
                    previous = now;
                    peaks.samples += 1;
                    let sample = Sample {
                        t_ms: start.elapsed().as_millis(),
                        phase: current_phase(),
                        rss: pm.resident_bytes,
                        footprint: pm.footprint_bytes,
                        alloc_live: a.live_bytes,
                        alloc_live_rounded: a.live_rounded_bytes,
                        commit: ai.current_commit_bytes,
                        total_allocs: a.total_allocs,
                        total_alloc_bytes: a.total_alloc_bytes,
                    };
                    if let Ok(line) = serde_json::to_string(&sample) {
                        let _ = writeln!(w, "{line}");
                    }
                    // Absolute deadlines, so the sampling period is the interval and
                    // not the interval plus however long a poll took.
                    next += interval;
                    if let Some(d) = next.checked_duration_since(std::time::Instant::now()) {
                        std::thread::sleep(d);
                    } else {
                        next = std::time::Instant::now();
                    }
                }
                let _ = w.flush();
                let pm = process_memory();
                let a = alloc_stats();
                let ai = allocator_info();
                peaks.exact_max_footprint = pm.lifetime_max_footprint_bytes;
                peaks.exact_max_alloc_live = a.live_bytes_peak;
                peaks.exact_max_commit = ai.peak_commit_bytes;
                peaks
            })?;
        Ok(Self {
            stop: &STOP,
            handle: Some(handle),
        })
    }

    pub fn stop(mut self) -> Peaks {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
    }
}
