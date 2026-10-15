use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use code_graph_incremental::pipeline::{Each, Prepare, SourceFile};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{
    Context, Env, Error, ItemPhase, Killed, Limits, Observer, Phase, Pipeline, Sentinel,
};
use std::sync::atomic::{AtomicU64, Ordering};
mod common;

struct Double;

impl Phase<u32> for Double {
    type Output = u32;
    fn name(&self) -> Cow<'static, str> {
        "double".into()
    }
    fn run(self, _: &mut Context, n: u32) -> Result<u32, Error> {
        Ok(n * 2)
    }
}

struct Stringify;

impl Phase<u32> for Stringify {
    type Output = String;
    fn name(&self) -> Cow<'static, str> {
        "stringify".into()
    }
    fn run(self, _: &mut Context, n: u32) -> Result<String, Error> {
        Ok(n.to_string())
    }
}

struct Skip(&'static str);

impl Phase<u32> for Skip {
    type Output = u32;
    fn name(&self) -> Cow<'static, str> {
        "skip".into()
    }
    fn run(self, context: &mut Context, n: u32) -> Result<u32, Error> {
        context.skip(Sentinel::new("item", self.0, 0).check().unwrap_err());
        Ok(n)
    }
}

#[derive(Default, Clone)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn lines(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

impl Observer for Log {
    fn started(&mut self, phase: &str) {
        self.0.lock().unwrap().push(format!("start {phase}"));
    }
    fn finished(&mut self, phase: &str, _: Duration) {
        self.0.lock().unwrap().push(format!("finish {phase}"));
    }
    fn skipped(&mut self, killed: &Killed) {
        self.0.lock().unwrap().push(format!("skip {}", killed.path));
    }
    fn failed(&mut self, phase: &str, error: &Error) {
        self.0
            .lock()
            .unwrap()
            .push(format!("fail {phase}: {error}"));
    }
}

fn unlimited() -> Env {
    Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap()
}

fn empty_sources() -> code_graph_incremental::pipeline::Sources {
    Arc::new(
        orbit_utils::vfs::Vfs::load(
            orbit_utils::vfs::sources::Memory(vec![]),
            (),
            Default::default(),
            Default::default(),
        )
        .unwrap(),
    )
}

#[test]
fn phases_chain_by_type_and_every_boundary_is_reported() {
    let env = unlimited();
    let (log, second) = (Log::default(), Log::default());
    let context = Context::new(&env, empty_sources())
        .observe(log.clone())
        .observe(second.clone());
    let (context, value) = Pipeline::new(context, 21)
        .then(Double)
        .unwrap()
        .then(Stringify)
        .unwrap()
        .finish();

    assert_eq!(value, "42");
    let phases: Vec<_> = context
        .report
        .phases
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    assert_eq!(phases, ["double", "stringify"]);
    assert_eq!(
        log.lines(),
        [
            "start double",
            "finish double",
            "start stringify",
            "finish stringify"
        ]
    );
    assert_eq!(second.lines(), log.lines());
}

#[test]
fn run_budget_stops_the_run_at_the_next_phase_boundary() {
    let env = Env::with_limits(
        SupportLang::Python,
        Limits {
            total_ms: 0,
            ..Limits::UNLIMITED
        },
    )
    .unwrap();
    let log = Log::default();

    let result =
        Pipeline::new(Context::new(&env, empty_sources()).observe(log.clone()), 1).then(Double);

    let Err(Error::Killed(killed)) = result else {
        panic!("a run past its budget must fail with Killed");
    };
    assert_eq!(killed.label, "run");
    assert_eq!(
        log.lines(),
        [
            "start double".to_string(),
            format!("fail double: budget: {killed}")
        ]
    );
}

#[test]
fn a_skipped_item_is_reported_and_the_run_continues() {
    let env = unlimited();
    let log = Log::default();
    let (context, value) =
        Pipeline::new(Context::new(&env, empty_sources()).observe(log.clone()), 1)
            .then(Skip("big.py"))
            .unwrap()
            .then(Double)
            .unwrap()
            .finish();

    assert_eq!(value, 2);
    assert_eq!(context.report.skipped.len(), 1);
    assert_eq!(context.report.skipped[0].path, "big.py");
    assert_eq!(context.report.skipped[0].label, "item");
    assert!(log.lines().contains(&"skip big.py".to_string()));
}

struct AddOne;

impl ItemPhase<u32> for AddOne {
    type Output = u32;
    fn name(&self) -> Cow<'static, str> {
        "add_one".into()
    }
    fn run(&self, _: &Env, run: &Sentinel, n: u32) -> Result<u32, Killed> {
        run.check()?;
        Ok(n + 1)
    }
}

#[test]
fn item_phases_pipe_into_one_named_pass() {
    let env = unlimited();
    let chain = AddOne.pipe(AddOne);

    assert_eq!(chain.name(), "add_one+add_one");
    assert_eq!(chain.run(&env, &Sentinel::disabled(), 1).unwrap(), 3);
    let killed = chain
        .run(&env, &Sentinel::new("run", "", 0), 1)
        .unwrap_err();
    assert_eq!(killed.label, "run");
}

#[derive(Clone, Default)]
struct InflightProbe {
    inflight: Arc<AtomicU64>,
    peak: Arc<AtomicU64>,
}

impl ItemPhase<SourceFile> for InflightProbe {
    type Output = ();
    fn name(&self) -> Cow<'static, str> {
        "probe".into()
    }
    fn run(&self, _: &Env, _: &Sentinel, file: SourceFile) -> Result<(), Killed> {
        let bytes = file.content.len() as u64;
        let now = self.inflight.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(30));
        self.inflight.fetch_sub(bytes, Ordering::Relaxed);
        Ok(())
    }
}

fn peak_inflight(files: &[(String, String)], max_inflight_bytes: u64) -> (u64, usize) {
    let repo = tempfile::tempdir().unwrap();
    for (path, content) in files {
        std::fs::write(repo.path().join(path), content).unwrap();
    }
    let env = Env::with_limits(
        SupportLang::Python,
        Limits {
            max_inflight_bytes,
            ..Limits::UNLIMITED
        },
    )
    .unwrap();
    let probe = InflightProbe::default();
    let items = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .install(|| {
            Pipeline::new(Context::new(&env, common::walk(repo.path())), ())
                .then(Prepare)
                .unwrap()
                .then(Each(probe.clone()))
                .unwrap()
                .into_value()
                .items
                .len()
        });
    (probe.peak.load(Ordering::Relaxed), items)
}

#[test]
fn source_bytes_in_flight_stay_under_the_budget_and_an_oversize_file_runs_alone() {
    const MIB: usize = 1024 * 1024;
    let files: Vec<_> = (0..8)
        .map(|i| (format!("f{i}.py"), "x = 1\n".repeat(MIB / 6)))
        .collect();
    let (unbounded, items) = peak_inflight(&files, 0);
    assert_eq!(items, 8);
    assert!(unbounded > 2 * MIB as u64, "{unbounded}");
    let (capped, items) = peak_inflight(&files, 2 * MIB as u64);
    assert_eq!(items, 8);
    assert!(capped <= 2 * MIB as u64, "{capped}");
    let big = [("big.py".to_string(), "x = 1\n".repeat(MIB / 2))];
    let (alone, items) = peak_inflight(&big, 2 * MIB as u64);
    assert_eq!((items, alone), (1, big[0].1.len() as u64));
}
