//! memprof — a tiny RSS-correlated allocation profiler.
//!
//! Two signals, fused:
//!   1. A background thread samples process RSS on a fixed cadence (the *when*).
//!   2. A global-allocator shim records sampled alloc/free *events* with
//!      backtraces and timestamps (the *where*).
//!
//! At report time we find the moment RSS peaked, replay the event log up to
//! that instant, and aggregate still-live bytes by call site. That tells you
//! which lines of code were holding memory *when the spike happened* — without
//! ever snapshotting the whole heap.
//!
//! Sampling policy is a size threshold: we only capture a backtrace for
//! allocations >= the capture size. Large allocations dominate RSS spikes, and
//! this keeps the common path (tiny, frequent allocations) essentially free.
//! Tunable at runtime through the environment:
//!   - `MEMPROF_MIN_CAPTURE`   bytes; only allocs this large get a backtrace (default 4096)
//!   - `MEMPROF_RSS_INTERVAL_MS` RSS sampler cadence in ms (default 5)
//!
//! `start(base)` writes two artifacts on drop: `<base>.csv` (the RSS timeline)
//! and `<base>.html` (an interactive chart of RSS over time with the peak
//! marked, plus a ranked table of the call sites holding memory at the peak).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const DEFAULT_MIN_CAPTURE_SIZE: usize = 4 * 1024;
const DEFAULT_RSS_INTERVAL_MS: u64 = 5;
/// Max stack depth captured per allocation.
const MAX_DEPTH: usize = 64;
/// User frames printed per call site in the report.
const REPORT_FRAMES: usize = 6;

// ---- public API -------------------------------------------------------------

/// The global-allocator shim. Wire it up with `#[global_allocator]`.
pub struct ProfAlloc;

/// Drop guard: on drop, stops sampling and writes the CSV + HTML report.
pub struct Session {
    out_path: String,
}

/// Begin profiling. `base` is a path stem; a `.csv` suffix, if present, is
/// stripped so both `<stem>.csv` and `<stem>.html` are written on drop.
pub fn start(base: &str) -> Session {
    START.get_or_init(Instant::now);
    MIN_CAPTURE.store(
        env_usize("MEMPROF_MIN_CAPTURE", DEFAULT_MIN_CAPTURE_SIZE),
        Ordering::Relaxed,
    );
    let interval =
        Duration::from_millis(env_u64("MEMPROF_RSS_INTERVAL_MS", DEFAULT_RSS_INTERVAL_MS));

    ENABLED.store(true, Ordering::SeqCst);
    RUNNING.store(true, Ordering::SeqCst);

    std::thread::Builder::new()
        .name("rss-sampler".into())
        .spawn(move || {
            // The sampler must never record its own allocations, or it would
            // recurse into the locked state it is writing.
            IN_HOOK.with(|h| h.set(true));
            let baseline = read_rss();
            BASELINE_RSS.store(baseline, Ordering::Relaxed);
            while RUNNING.load(Ordering::Relaxed) {
                let rss = read_rss();
                let t = elapsed_ms();
                PEAK_RSS.fetch_max(rss, Ordering::Relaxed);
                if let Ok(mut st) = state().lock() {
                    st.rss.push((t, rss));
                }
                std::thread::sleep(interval);
            }
        })
        .expect("spawn sampler");

    Session {
        out_path: base.to_string(),
    }
}

unsafe impl GlobalAlloc for ProfAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if ptr.is_null() || !ENABLED.load(Ordering::Relaxed) {
            return ptr;
        }
        if layout.size() >= MIN_CAPTURE.load(Ordering::Relaxed) {
            with_guard(|| record_alloc(ptr as usize, layout.size()));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            with_guard(|| record_dealloc(ptr as usize));
        }
        unsafe { System.dealloc(ptr, layout) };
    }
    // We rely on the trait's default `realloc`, which routes through the
    // instrumented alloc/dealloc above — so a realloc shows up as free+alloc,
    // which is exactly right for live-bytes tracking.
}

impl Drop for Session {
    fn drop(&mut self) {
        RUNNING.store(false, Ordering::SeqCst);
        ENABLED.store(false, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(
            env_u64("MEMPROF_RSS_INTERVAL_MS", DEFAULT_RSS_INTERVAL_MS) * 2,
        ));
        report(&self.out_path);
    }
}

// ---- global state -----------------------------------------------------------

#[derive(Clone)]
struct Event {
    t_ms: u64,
    delta: i64, // +size on alloc, -size on free
    bt_id: u32,
}

#[derive(Default)]
struct State {
    // pointer -> (size, backtrace id) for currently-live *sampled* allocations
    live: HashMap<usize, (usize, u32)>,
    // interned backtraces: raw instruction pointers
    bt_table: Vec<Vec<usize>>,
    bt_index: HashMap<Vec<usize>, u32>,
    // time-ordered log of sampled alloc/free events
    events: Vec<Event>,
    // (t_ms, rss_bytes) timeline
    rss: Vec<(u64, u64)>,
}

fn state() -> &'static Mutex<State> {
    static S: OnceLock<Mutex<State>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(State::default()))
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static RUNNING: AtomicBool = AtomicBool::new(false);
static START: OnceLock<Instant> = OnceLock::new();
static PEAK_RSS: AtomicU64 = AtomicU64::new(0);
static BASELINE_RSS: AtomicU64 = AtomicU64::new(0);
static MIN_CAPTURE: AtomicUsize = AtomicUsize::new(DEFAULT_MIN_CAPTURE_SIZE);

// Capturing a backtrace itself allocates. While this flag is set, allocations
// take the raw System path with no recording, so we never recurse or deadlock.
thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

fn with_guard<R>(f: impl FnOnce() -> R) -> Option<R> {
    IN_HOOK.with(|h| {
        if h.get() {
            return None;
        }
        h.set(true);
        let r = f();
        h.set(false);
        Some(r)
    })
}

fn elapsed_ms() -> u64 {
    START
        .get()
        .map(|s| s.elapsed().as_millis() as u64)
        .unwrap_or(0)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---- recording --------------------------------------------------------------

fn capture_backtrace() -> Vec<usize> {
    let mut ips = Vec::with_capacity(MAX_DEPTH);
    backtrace::trace(|frame| {
        ips.push(frame.ip() as usize);
        ips.len() < MAX_DEPTH
    });
    ips
}

fn record_alloc(ptr: usize, size: usize) {
    let ips = capture_backtrace();
    let t_ms = elapsed_ms();
    let mut st = state().lock().unwrap();
    let bt_id = match st.bt_index.get(&ips) {
        Some(&id) => id,
        None => {
            let id = st.bt_table.len() as u32;
            st.bt_table.push(ips.clone());
            st.bt_index.insert(ips, id);
            id
        }
    };
    st.live.insert(ptr, (size, bt_id));
    st.events.push(Event {
        t_ms,
        delta: size as i64,
        bt_id,
    });
}

fn record_dealloc(ptr: usize) {
    let mut st = state().lock().unwrap();
    if let Some((size, bt_id)) = st.live.remove(&ptr) {
        let t_ms = elapsed_ms();
        st.events.push(Event {
            t_ms,
            delta: -(size as i64),
            bt_id,
        });
    }
}

// ---- RSS reading (platform specific) ---------------------------------------

// mach2 0.4 exposes the `MACH_TASK_BASIC_INFO` flavor constant but not the
// matching struct or its `_COUNT`, so we declare the ABI layout ourselves.
// Fields mirror `<mach/task_info.h>`; only `resident_size` is read.
#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTaskBasicInfo {
    virtual_size: u64,
    resident_size: u64,
    resident_size_max: u64,
    user_time: [i32; 2],
    system_time: [i32; 2],
    policy: i32,
    suspend_count: i32,
}

#[cfg(target_os = "macos")]
fn read_rss() -> u64 {
    use mach2::kern_return::KERN_SUCCESS;
    use mach2::task::task_info;
    use mach2::task_info::{MACH_TASK_BASIC_INFO, task_info_t};
    use mach2::traps::mach_task_self;

    const COUNT: u32 =
        (std::mem::size_of::<MachTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;
    unsafe {
        let mut info = std::mem::zeroed::<MachTaskBasicInfo>();
        let mut count = COUNT;
        let kr = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info as *mut _ as task_info_t,
            &mut count,
        );
        if kr == KERN_SUCCESS {
            info.resident_size
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn read_rss() -> u64 {
    // Linux exposes current RSS only as field 2 of /proc/self/statm, in pages.
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if page > 0 { page as u64 } else { 4096 };
    pages.saturating_mul(page)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_rss() -> u64 {
    0
}

// ---- reporting --------------------------------------------------------------

/// A call site with a byte figure (live-at-peak or total-allocated).
struct SiteRow {
    bytes: i64,
    frames: Vec<String>,
}

/// Everything the HTML report renders, gathered so `write_html` takes one arg.
struct Report<'a> {
    rss: &'a [(u64, u64)],
    live_series: &'a [(u64, i64)],
    peak_t: u64,
    peak_rss: u64,
    alloc_peak_t: u64,
    baseline: u64,
    sampled_live_at_peak: i64,
    total_allocated: i64,
    sites: &'a [SiteRow],
    churn_sites: &'a [SiteRow],
}

/// Remove rustc's `[0123abcd...]` crate-disambiguator hashes for readability.
fn strip_hashes(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '[' {
            let mut buf = String::new();
            while let Some(&n) = chars.peek() {
                if n == ']' {
                    chars.next();
                    break;
                }
                buf.push(n);
                chars.next();
            }
            if !(buf.len() >= 8 && buf.chars().all(|c| c.is_ascii_hexdigit())) {
                out.push('[');
                out.push_str(&buf);
                out.push(']');
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Drop generic type arguments (`foo::<Huge, Types>` -> `foo`) so monomorphized
/// rayon/iterator frames stay readable in the report.
fn strip_generics(s: &str) -> String {
    let mut out = String::new();
    let mut depth = 0i32;
    for c in s.chars() {
        match c {
            '<' => depth += 1,
            '>' => depth = (depth - 1).max(0),
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// Resolve raw instruction pointers to `symbol  file:line`, dropping toolchain
/// and profiler frames so what remains is the user's own call stack.
fn symbolize(ips: &[usize]) -> Vec<String> {
    let mut frames = Vec::new();
    for &ip in ips {
        let mut label = String::new();
        backtrace::resolve(ip as *mut _, |sym| {
            let name = sym
                .name()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unknown>".into());
            let file = sym.filename().map(|f| f.display().to_string());
            // Toolchain/profiler/allocator frames are never the user's own code.
            // With `line-tables-only` builds the symbol name is unqualified
            // (just `trace`, `capture_backtrace`), so filter on the source path
            // first and only fall back to the name when no path is available.
            let path_is_glue = file.as_deref().is_some_and(|f| {
                f.contains("memprof/src/lib.rs")
                    || f.contains("/backtrace-")
                    || f.contains("/backtrace/")
                    || f.contains("/rustc/")
                    || f.contains("library/std")
                    || f.contains("library/core")
                    || f.contains("library/alloc")
            });
            let name_is_glue = file.is_none()
                && (name.contains("memprof")
                    || name.contains("backtrace")
                    || name.starts_with("__rust")
                    || name.contains("__rustc")
                    || name.contains("alloc_zeroed")
                    || name.contains("alloc_impl")
                    || name == "malloc"
                    || name == "realloc");
            if path_is_glue || name_is_glue {
                return;
            }
            let loc = match (&file, sym.lineno()) {
                (Some(f), Some(l)) => format!("  {f}:{l}"),
                _ => String::new(),
            };
            if label.is_empty() {
                let mut short = strip_generics(&strip_hashes(&name));
                if short.len() > 90 {
                    short.truncate(90);
                    short.push('…');
                }
                label = format!("{short}{loc}");
            }
        });
        if !label.is_empty() {
            frames.push(label);
        }
        if frames.len() >= REPORT_FRAMES {
            break;
        }
    }
    frames
}

/// Reconstruct net concurrent-live bytes over time from the event log,
/// downsampled to at most `max_points` (keeping the max within each bucket so
/// spikes survive). Overlaying this against RSS shows the retention gap.
fn net_live_series(events: &[Event], max_points: usize) -> Vec<(u64, i64)> {
    let Some(last) = events.last() else {
        return Vec::new();
    };
    let span = last.t_ms.max(1);
    let mut out: Vec<(u64, i64)> = Vec::new();
    let (mut running, mut cur_bucket) = (0i64, u64::MAX);
    for e in events {
        running += e.delta;
        let bucket = e.t_ms.saturating_mul(max_points as u64) / (span + 1);
        if bucket != cur_bucket {
            out.push((e.t_ms, running.max(0)));
            cur_bucket = bucket;
        } else if let Some(p) = out.last_mut()
            && running > p.1
        {
            *p = (e.t_ms, running);
        }
    }
    out
}

/// Find peak RSS, replay events up to that instant, aggregate live bytes by
/// call site, write the CSV timeline + HTML chart, and print a text summary.
fn report(out_path: &str) {
    let st = state().lock().unwrap();

    let (mut peak_t, mut peak_rss) = (0u64, 0u64);
    for &(t, rss) in &st.rss {
        if rss > peak_rss {
            peak_rss = rss;
            peak_t = t;
        }
    }

    // Attribute at the reconstructed allocation high-water, not at the RSS-sample
    // peak. The OS reports RSS with lag (freed pages linger), so the RSS peak
    // sample can land after a transient allocation is already freed in our log.
    // The argmax of live sampled bytes is the instant our own events say memory
    // was highest, so replaying up to it is internally consistent.
    let mut events = st.events.clone();
    events.sort_by_key(|e| e.t_ms);
    let (mut running, mut peak_idx, mut sampled_live_at_peak) = (0i64, None::<usize>, 0i64);
    for (i, e) in events.iter().enumerate() {
        running += e.delta;
        if running > sampled_live_at_peak {
            sampled_live_at_peak = running;
            peak_idx = Some(i);
        }
    }
    let alloc_peak_t = peak_idx.map(|i| events[i].t_ms).unwrap_or(0);

    let mut live_by_bt: HashMap<u32, i64> = HashMap::new();
    if let Some(idx) = peak_idx {
        for e in &events[..=idx] {
            *live_by_bt.entry(e.bt_id).or_insert(0) += e.delta;
        }
    }
    let mut ranked: Vec<(u32, i64)> = live_by_bt.into_iter().filter(|&(_, b)| b > 0).collect();
    ranked.sort_by_key(|&(_, b)| std::cmp::Reverse(b));
    let sites: Vec<SiteRow> = ranked
        .iter()
        .take(25)
        .map(|&(bt_id, bytes)| SiteRow {
            bytes,
            frames: symbolize(&st.bt_table[bt_id as usize]),
        })
        .collect();

    // Churn: total bytes ever allocated per call site, regardless of when freed.
    // When peak RSS greatly exceeds peak concurrent live, the blowup is the
    // allocator holding high-water pages against transient churn, and these are
    // the sites to optimize (reduce allocations / reuse buffers).
    let mut churn_by_bt: HashMap<u32, i64> = HashMap::new();
    let mut total_allocated: i64 = 0;
    for e in &events {
        if e.delta > 0 {
            *churn_by_bt.entry(e.bt_id).or_insert(0) += e.delta;
            total_allocated += e.delta;
        }
    }
    let mut churn_ranked: Vec<(u32, i64)> = churn_by_bt.into_iter().collect();
    churn_ranked.sort_by_key(|&(_, b)| std::cmp::Reverse(b));
    let churn_sites: Vec<SiteRow> = churn_ranked
        .iter()
        .take(25)
        .map(|&(bt_id, bytes)| SiteRow {
            bytes,
            frames: symbolize(&st.bt_table[bt_id as usize]),
        })
        .collect();

    let live_series = net_live_series(&events, 1500);

    let csv_path = if let Some(s) = out_path.strip_suffix(".csv") {
        format!("{s}.csv")
    } else {
        format!("{out_path}.csv")
    };
    if let Ok(mut f) = std::fs::File::create(&csv_path) {
        let _ = writeln!(f, "t_ms,rss_bytes");
        for &(t, rss) in &st.rss {
            let _ = writeln!(f, "{t},{rss}");
        }
    }

    let html_path = out_path
        .strip_suffix(".csv")
        .map(|s| format!("{s}.html"))
        .unwrap_or_else(|| format!("{out_path}.html"));
    let baseline = BASELINE_RSS.load(Ordering::Relaxed);
    write_html(
        &html_path,
        &Report {
            rss: &st.rss,
            live_series: &live_series,
            peak_t,
            peak_rss,
            alloc_peak_t,
            baseline,
            sampled_live_at_peak,
            total_allocated,
            sites: &sites,
            churn_sites: &churn_sites,
        },
    );

    let mib = |b: f64| b / 1_048_576.0;
    let retention = peak_rss as f64 / sampled_live_at_peak.max(1) as f64;
    eprintln!("\n================ memprof report ================");
    eprintln!("baseline RSS:         {:.1} MiB", mib(baseline as f64));
    eprintln!(
        "peak RSS:             {:.1} MiB at t = {} ms (OS sampler)",
        mib(peak_rss as f64),
        peak_t
    );
    eprintln!(
        "peak concurrent live: {:.1} MiB at t = {} ms (net, through the Rust global allocator)",
        mib(sampled_live_at_peak as f64),
        alloc_peak_t
    );
    eprintln!(
        "total allocated:      {:.1} MiB (churn — sum of every allocation)",
        mib(total_allocated as f64)
    );
    eprintln!(
        "diagnosis: peak RSS is {retention:.1}x peak concurrent live. {}",
        if retention > 3.0 {
            "Most RSS is allocator high-water against transient churn (or memory outside the Rust allocator, e.g. tree-sitter's C malloc), not retained live objects — see the churn table."
        } else {
            "RSS tracks retained live objects — see the live-at-peak table."
        }
    );
    eprintln!("timeline CSV: {csv_path}");
    eprintln!("chart HTML:   {html_path}");
    eprintln!(
        "(sampled: allocations >= {} bytes)",
        MIN_CAPTURE.load(Ordering::Relaxed)
    );

    eprintln!("\ntop sites by total bytes allocated (churn):");
    for (rank, s) in churn_sites.iter().take(10).enumerate() {
        eprintln!("  #{}  {:.1} MiB allocated", rank + 1, mib(s.bytes as f64));
        for fr in &s.frames {
            eprintln!("        {fr}");
        }
    }
    eprintln!("\ntop sites live at the allocation high-water (retained):");
    for (rank, s) in sites.iter().take(6).enumerate() {
        eprintln!("  #{}  {:.1} MiB live", rank + 1, mib(s.bytes as f64));
        for fr in &s.frames {
            eprintln!("        {fr}");
        }
    }
    eprintln!("================================================\n");
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn site_rows(sites: &[SiteRow]) -> String {
    let mib = |b: f64| b / 1_048_576.0;
    let mut rows = String::new();
    for (rank, s) in sites.iter().enumerate() {
        let top = s
            .frames
            .first()
            .cloned()
            .unwrap_or_else(|| "<unresolved>".into());
        let rest = s
            .frames
            .iter()
            .skip(1)
            .map(|f| html_escape(f))
            .collect::<Vec<_>>()
            .join("<br>");
        rows.push_str(&format!(
            "<tr><td class=r>#{}</td><td class=b>{:.1}</td><td><div class=top>{}</div>\
             <div class=stack>{}</div></td></tr>",
            rank + 1,
            mib(s.bytes as f64),
            html_escape(&top),
            rest
        ));
    }
    rows
}

fn json_pairs<T: std::fmt::Display + Copy>(series: &[(u64, T)]) -> String {
    let mut s = String::from("[");
    for (i, &(t, v)) in series.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("[{t},{v}]"));
    }
    s.push(']');
    s
}

fn write_html(path: &str, r: &Report) {
    let Report {
        rss,
        live_series,
        peak_t,
        peak_rss,
        alloc_peak_t,
        baseline,
        sampled_live_at_peak,
        total_allocated,
        sites,
        churn_sites,
    } = *r;

    let points = json_pairs(rss);
    let live_points = json_pairs(live_series);
    let mib = |b: f64| b / 1_048_576.0;
    let rows = site_rows(sites);
    let churn_rows = site_rows(churn_sites);
    let retention = peak_rss as f64 / sampled_live_at_peak.max(1) as f64;

    let html = format!(
        r#"<!doctype html><html><head><meta charset=utf-8>
<title>memprof — RSS correlated to call sites</title>
<style>
  :root {{ --ink:#1c2333; --muted:#6b7280; --line:#e5e7eb; --accent:#3843d0; --peak:#d33; --bg:#fbfbfd; }}
  body {{ font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:var(--ink); background:var(--bg); margin:0; padding:24px 32px; }}
  h1 {{ font-size:18px; margin:0 0 4px; }}
  .sub {{ color:var(--muted); margin-bottom:20px; }}
  .cards {{ display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:12px 16px; min-width:150px; }}
  .card .k {{ color:var(--muted); font-size:12px; }}
  .card .v {{ font-size:20px; font-weight:600; margin-top:2px; }}
  canvas {{ background:#fff; border:1px solid var(--line); border-radius:10px; width:100%; }}
  table {{ width:100%; border-collapse:collapse; margin-top:24px; background:#fff; border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
  th,td {{ text-align:left; padding:8px 12px; border-bottom:1px solid var(--line); vertical-align:top; }}
  th {{ background:#f5f6fa; font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; }}
  td.r {{ color:var(--muted); white-space:nowrap; }}
  td.b {{ font-variant-numeric:tabular-nums; font-weight:600; white-space:nowrap; }}
  .top {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }}
  .stack {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:11px; color:var(--muted); margin-top:2px; }}
</style></head><body>
<h1>memprof — memory usage correlated to lines of code</h1>
<div class=sub>Blue = process RSS. Green = net concurrent-live bytes through the Rust global allocator. The gap between them is memory the allocator holds but no live object owns (churn high-water, or non-Rust allocations).</div>
<div class=cards>
  <div class=card><div class=k>baseline RSS</div><div class=v>{baseline_mib:.0} MiB</div></div>
  <div class=card><div class=k>peak RSS</div><div class=v>{peak_mib:.0} MiB</div></div>
  <div class=card><div class=k>peak concurrent live</div><div class=v>{alloc_mib:.0} MiB</div></div>
  <div class=card><div class=k>total allocated (churn)</div><div class=v>{churn_mib:.0} MiB</div></div>
  <div class=card><div class=k>RSS / live</div><div class=v>{retention:.1}x</div></div>
</div>
<canvas id=c height=380></canvas>
<h2 style="font-size:15px;margin:28px 0 0">Top sites by total bytes allocated (churn)</h2>
<div class=sub style="margin-top:2px">What to optimize when RSS ≫ live: these lines allocate the most, driving the allocator's high-water even after the memory is freed.</div>
<table><thead><tr><th>rank</th><th>MiB alloc'd</th><th>call site (top frame + stack)</th></tr></thead>
<tbody>{churn_rows}</tbody></table>
<h2 style="font-size:15px;margin:28px 0 0">Top sites live at the allocation high-water (retained)</h2>
<div class=sub style="margin-top:2px">Call sites still holding memory at t={alloc_peak_t} ms (dashed blue on the chart). The red marker is the OS RSS peak (t={peak_t} ms); it lags because freed pages linger.</div>
<table><thead><tr><th>rank</th><th>MiB live</th><th>call site (top frame + stack)</th></tr></thead>
<tbody>{rows}</tbody></table>
<script>
const DATA={points}, LIVE={live_points}, PEAK_T={peak_t}, PEAK_RSS={peak_rss}, ALLOC_T={alloc_peak_t};
const cv=document.getElementById('c'), ctx=cv.getContext('2d');
function draw(){{
  const W=cv.clientWidth, H=380, dpr=devicePixelRatio||1;
  cv.width=W*dpr; cv.height=H*dpr; ctx.setTransform(dpr,0,0,dpr,0,0);
  ctx.clearRect(0,0,W,H);
  const pad={{l:64,r:16,t:16,b:28}};
  const tMax=DATA.length?DATA[DATA.length-1][0]:1;
  let rMax=0; for(const p of DATA) rMax=Math.max(rMax,p[1]); rMax=rMax||1;
  const X=t=>pad.l+(W-pad.l-pad.r)*(t/tMax);
  const Y=r=>H-pad.b-(H-pad.t-pad.b)*(r/rMax);
  ctx.strokeStyle='#eceef4'; ctx.fillStyle='#9aa1b2'; ctx.font='11px sans-serif';
  for(let i=0;i<=4;i++){{const r=rMax*i/4,y=Y(r); ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();
    ctx.fillText((r/1048576).toFixed(0)+' MiB',6,y+3);}}
  ctx.beginPath(); ctx.strokeStyle='#3843d0'; ctx.lineWidth=1.5;
  DATA.forEach((p,i)=>{{const x=X(p[0]),y=Y(p[1]); i?ctx.lineTo(x,y):ctx.moveTo(x,y);}});
  ctx.stroke();
  ctx.lineTo(X(tMax),Y(0)); ctx.lineTo(X(0),Y(0)); ctx.closePath();
  ctx.fillStyle='rgba(56,67,208,.08)'; ctx.fill();
  ctx.beginPath(); ctx.strokeStyle='#1a9d6a'; ctx.lineWidth=1.5;
  LIVE.forEach((p,i)=>{{const x=X(p[0]),y=Y(p[1]); i?ctx.lineTo(x,y):ctx.moveTo(x,y);}});
  ctx.stroke();
  ctx.fillStyle='#1a9d6a'; ctx.fillText('net live',pad.l+6,pad.t+40);
  ctx.fillStyle='#3843d0'; ctx.fillText('RSS',pad.l+6,pad.t+12);
  const ax=X(ALLOC_T);
  ctx.strokeStyle='#3843d0'; ctx.setLineDash([4,3]); ctx.beginPath();ctx.moveTo(ax,pad.t);ctx.lineTo(ax,H-pad.b);ctx.stroke();
  ctx.fillStyle='#3843d0'; ctx.fillText('alloc high-water @ '+ALLOC_T+'ms',ax+6,pad.t+26);
  const px=X(PEAK_T),py=Y(PEAK_RSS);
  ctx.strokeStyle='#d33'; ctx.beginPath();ctx.moveTo(px,pad.t);ctx.lineTo(px,H-pad.b);ctx.stroke(); ctx.setLineDash([]);
  ctx.fillStyle='#d33'; ctx.beginPath();ctx.arc(px,py,4,0,7);ctx.fill();
  ctx.fillText('RSS peak '+(PEAK_RSS/1048576).toFixed(0)+' MiB @ '+PEAK_T+'ms',px+6,pad.t+12);
}}
draw(); addEventListener('resize',draw);
</script>
</body></html>"#,
        baseline_mib = mib(baseline as f64),
        peak_mib = mib(peak_rss as f64),
        alloc_mib = mib(sampled_live_at_peak as f64),
        churn_mib = mib(total_allocated as f64),
    );
    let _ = std::fs::write(path, html);
}
