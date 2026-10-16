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

#[derive(Default)]
struct Ledger {
    admitted: Mutex<Vec<u64>>,
    inflight: AtomicU64,
}

impl code_graph_incremental::Admission for Ledger {
    fn admit(&self, bytes: u64) {
        self.admitted.lock().unwrap().push(bytes);
        self.inflight.fetch_add(bytes, Ordering::Relaxed);
    }
    fn release(&self, bytes: u64) {
        self.inflight.fetch_sub(bytes, Ordering::Relaxed);
    }
}

struct SeesItsSlot(Arc<Ledger>);

impl ItemPhase<SourceFile> for SeesItsSlot {
    type Output = ();
    fn name(&self) -> Cow<'static, str> {
        "slot".into()
    }
    fn run(&self, _: &Env, _: &Sentinel, file: SourceFile) -> Result<(), Killed> {
        assert!(self.0.inflight.load(Ordering::Relaxed) >= file.content.len() as u64);
        Ok(())
    }
}

#[test]
fn each_admits_every_item_before_its_phase_and_releases_it_after() {
    // Each splits per item, so an admission that blocks holds only its own item.
    let files: Vec<_> = (1..=8)
        .map(|i| (format!("f{i}.py"), "x = 1\n".repeat(i * 1000)))
        .collect();
    let repo = tempfile::tempdir().unwrap();
    for (path, content) in &files {
        std::fs::write(repo.path().join(path), content).unwrap();
    }
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ledger = Arc::new(Ledger::default());
    let items = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .install(|| {
            Pipeline::new(
                Context::new(&env, common::walk(repo.path())).admit_with(ledger.clone()),
                (),
            )
            .then(Prepare)
            .unwrap()
            .then(Each(SeesItsSlot(ledger.clone())))
            .unwrap()
            .into_value()
            .items
            .len()
        });
    assert_eq!(items, 8);
    let mut admitted = ledger.admitted.lock().unwrap().clone();
    admitted.sort_unstable();
    let mut sizes: Vec<u64> = files.iter().map(|(_, c)| c.len() as u64).collect();
    sizes.sort_unstable();
    assert_eq!(admitted, sizes);
    assert_eq!(ledger.inflight.load(Ordering::Relaxed), 0);
}
