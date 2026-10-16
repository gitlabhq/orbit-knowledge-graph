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
    fn admit(&self, bytes: u64, _: &Sentinel) -> Result<(), Killed> {
        self.admitted.lock().unwrap().push(bytes);
        self.inflight.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
    fn release(&self, bytes: u64) {
        self.inflight.fetch_sub(bytes, Ordering::Relaxed);
    }
}

struct RefuseOver(u64);

impl code_graph_incremental::Admission for RefuseOver {
    fn admit(&self, bytes: u64, _: &Sentinel) -> Result<(), Killed> {
        if bytes > self.0 {
            return Err(Killed::limit(
                "admission",
                "",
                bytes as usize,
                self.0 as usize,
            ));
        }
        Ok(())
    }
    fn release(&self, _: u64) {}
}

struct FailsOn(&'static str, Arc<Ledger>);

impl ItemPhase<SourceFile> for FailsOn {
    type Output = ();
    fn name(&self) -> Cow<'static, str> {
        "fails".into()
    }
    fn run(&self, _: &Env, _: &Sentinel, file: SourceFile) -> Result<(), Killed> {
        assert!(self.1.inflight.load(Ordering::Relaxed) >= file.content.len() as u64);
        if file.path == self.0 {
            return Err(Killed::limit("phase", &file.path, 1, 0));
        }
        Ok(())
    }
}

struct Passes;

impl ItemPhase<SourceFile> for Passes {
    type Output = ();
    fn name(&self) -> Cow<'static, str> {
        "passes".into()
    }
    fn run(&self, _: &Env, _: &Sentinel, _: SourceFile) -> Result<(), Killed> {
        Ok(())
    }
}

struct PanicsOn(&'static str);

impl ItemPhase<SourceFile> for PanicsOn {
    type Output = ();
    fn name(&self) -> Cow<'static, str> {
        "panics".into()
    }
    fn run(&self, _: &Env, _: &Sentinel, file: SourceFile) -> Result<(), Killed> {
        assert_ne!(file.path, self.0, "the item phase panics on {}", self.0);
        Ok(())
    }
}

fn write_files(repo: &std::path::Path, files: &[(String, String)]) {
    for (path, content) in files {
        std::fs::write(repo.join(path), content).unwrap();
    }
}

fn sized_files(count: usize) -> Vec<(String, String)> {
    (1..=count)
        .map(|i| (format!("f{i}.py"), "x = 1\n".repeat(i * 1000)))
        .collect()
}

fn run_each<P>(
    repo: &std::path::Path,
    admission: Arc<dyn code_graph_incremental::Admission>,
    phase: P,
) -> (usize, Vec<Killed>)
where
    P: ItemPhase<SourceFile, Output = ()> + Send + Sync,
{
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .install(|| {
            let (context, workset) = Pipeline::new(
                Context::new(&env, common::walk(repo)).admit_with(admission),
                (),
            )
            .then(Prepare)
            .unwrap()
            .then(Each(phase))
            .unwrap()
            .finish();
            (workset.items.len(), context.report.skipped)
        })
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

#[test]
fn a_refused_item_is_reported_as_skipped_and_the_others_run() {
    let files = sized_files(3);
    let repo = tempfile::tempdir().unwrap();
    write_files(repo.path(), &files);
    let limit = files[1].1.len() as u64;
    let (items, skipped) = run_each(repo.path(), Arc::new(RefuseOver(limit)), Passes);
    assert_eq!(items, 2);
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].label, "admission");
}

#[test]
fn a_slot_is_released_after_a_killed_item_and_after_a_panic() {
    let files = sized_files(4);
    let repo = tempfile::tempdir().unwrap();
    write_files(repo.path(), &files);
    let ledger = Arc::new(Ledger::default());
    let (items, skipped) = run_each(
        repo.path(),
        ledger.clone(),
        FailsOn("f2.py", ledger.clone()),
    );
    assert_eq!((items, skipped.len()), (3, 1));
    assert_eq!(ledger.inflight.load(Ordering::Relaxed), 0);

    let ledger = Arc::new(Ledger::default());
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_each(repo.path(), ledger.clone(), PanicsOn("f3.py"))
    }));
    assert!(panicked.is_err());
    assert_eq!(ledger.inflight.load(Ordering::Relaxed), 0);
}

#[test]
fn the_admission_in_the_options_reaches_every_family() {
    use code_graph_incremental::export::ExportConfig;
    use code_graph_incremental::{Envelope, Scalar, templates};
    let files = vec![
        ("a.py".to_string(), "x = 1\n".repeat(500)),
        ("b.js".to_string(), "let y = 2;\n".repeat(400)),
    ];
    let repo = tempfile::tempdir().unwrap();
    write_files(repo.path(), &files);
    let ledger = Arc::new(Ledger::default());
    let ontology = ontology::Ontology::load_embedded().unwrap();
    templates::repository(
        common::walk(repo.path()),
        templates::Options {
            limits: Limits::UNLIMITED,
            worker_threads: 2,
            concurrent_families: 2,
            small_family_files: 0,
            batch_bytes: orbit_arrow::BATCH_BYTES,
            cancelled: Arc::new(|| false),
            family_finished: Arc::new(|| {}),
            phase_started: Arc::new(|_, _, _| {}),
            admission: ledger.clone(),
        },
        &Envelope::new([
            ("project_id", Scalar::Int(1)),
            ("branch", Scalar::Str("main")),
            ("commit_sha", Scalar::Str("abc")),
        ]),
        &ontology,
        ExportConfig::embedded(),
        |_| Ok::<(), String>(()),
    )
    .unwrap();
    let mut admitted = ledger.admitted.lock().unwrap().clone();
    admitted.sort_unstable();
    let mut sizes: Vec<u64> = files.iter().map(|(_, c)| c.len() as u64).collect();
    sizes.sort_unstable();
    assert_eq!(admitted, sizes);
    assert_eq!(ledger.inflight.load(Ordering::Relaxed), 0);
}
