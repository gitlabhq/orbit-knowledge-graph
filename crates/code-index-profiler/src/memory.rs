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

    /// One counter set per cache line. A single global counter costs more than the
    /// allocation it measures once 14 rayon workers hammer it, so live bytes are
    /// sharded and summed by the sampler instead of maintained as one hot word.
    #[repr(align(128))]
    struct Shard {
        live: AtomicI64,
        allocs: AtomicU64,
        alloc_bytes: AtomicU64,
    }

    impl Shard {
        const fn new() -> Self {
            Self {
                live: AtomicI64::new(0),
                allocs: AtomicU64::new(0),
                alloc_bytes: AtomicU64::new(0),
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

    thread_local! {
        static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
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
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            COUNTERS[slot()]
                .live
                .fetch_sub(layout.size() as i64, Ordering::Relaxed);
            unsafe { self.0.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let out = unsafe { self.0.realloc(ptr, layout, new_size) };
            if !out.is_null() {
                let c = &COUNTERS[slot()];
                c.live
                    .fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
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
        }
        out
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AllocStats {
    pub live_bytes: i64,
    pub total_allocs: u64,
    pub total_alloc_bytes: u64,
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
}

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
    }
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
    pub samples: u64,
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
                while !STOP.load(Ordering::Acquire) {
                    let pm = process_memory();
                    let a = alloc_stats();
                    peaks.max_rss = peaks.max_rss.max(pm.resident_bytes);
                    peaks.max_footprint = peaks.max_footprint.max(pm.footprint_bytes);
                    peaks.max_alloc_live = peaks.max_alloc_live.max(a.live_bytes);
                    peaks.samples += 1;
                    let sample = Sample {
                        t_ms: start.elapsed().as_millis(),
                        phase: current_phase(),
                        rss: pm.resident_bytes,
                        footprint: pm.footprint_bytes,
                        alloc_live: a.live_bytes,
                        total_allocs: a.total_allocs,
                        total_alloc_bytes: a.total_alloc_bytes,
                    };
                    if let Ok(line) = serde_json::to_string(&sample) {
                        let _ = writeln!(w, "{line}");
                    }
                    std::thread::sleep(interval);
                }
                let _ = w.flush();
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
