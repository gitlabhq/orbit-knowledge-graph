mod schema;
mod sources;

use std::io;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

use orbit_utils::vfs::{Decision, File, Pass, Vfs};
use schema::*;

struct Policy(Vec<(Rule, Decision<Tag>)>);

impl Policy {
    fn new(rules: &[Rule]) -> Self {
        Self(
            rules
                .iter()
                .map(|rule| {
                    let decision = match &rule.decision {
                        Verdict::Pending => Decision::Pending,
                        Verdict::Keep(tag) => Decision::Keep(*tag),
                        Verdict::List(reason) => Decision::List(reason_label(reason)),
                        Verdict::Drop(reason) => Decision::Drop(reason_label(reason)),
                    };
                    (rule.clone(), decision)
                })
                .collect(),
        )
    }

    fn apply(&self, phase: Phase, file: &mut File<Tag>, bytes: &[u8]) {
        for (rule, decision) in &self.0 {
            if rule.phase == phase
                && rule
                    .suffix
                    .as_ref()
                    .is_none_or(|suffix| file.path.ends_with(suffix))
                && rule.contains.as_ref().is_none_or(|data| {
                    let needle = data.bytes();
                    needle.is_empty() || bytes.windows(needle.len()).any(|window| window == needle)
                })
            {
                file.decide(*decision);
            }
        }
    }
}

impl Pass for Policy {
    type Tag = Tag;
    fn header(&self, file: &mut File<Tag>) {
        self.apply(Phase::Header, file, &[]);
    }
    fn content(&self, file: &mut File<Tag>, bytes: &[u8]) {
        self.apply(Phase::Content, file, bytes);
    }
}

fn reason_label(value: &str) -> &'static str {
    match value {
        "binary" => "binary",
        "excluded" => "excluded",
        "log" => "log",
        "content" => "content",
        _ => panic!("unknown policy reason {value:?}"),
    }
}

fn verdict(decision: Decision<Tag>) -> Verdict {
    match decision {
        Decision::Pending => panic!("Pending is observable after loading"),
        Decision::Keep(tag) => Verdict::Keep(tag),
        Decision::List(reason) => Verdict::List(reason.into()),
        Decision::Drop(reason) => Verdict::Drop(reason.into()),
    }
}

pub(super) fn error_name(error: &io::Error) -> String {
    format!("{:?}", error.kind())
}

fn check<T: std::fmt::Debug + PartialEq>(actual: io::Result<T>, expected: Outcome<T>) {
    match (actual, expected) {
        (Ok(actual), Outcome::Ok(expected)) => assert_eq!(actual, expected.ok),
        (Err(actual), Outcome::Err(expected)) => assert_eq!(error_name(&actual), expected.error),
        (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
    }
}

pub fn run(yaml: &str) {
    let scenario: Scenario = orbit_utils::yaml::from_str(yaml).expect("invalid VFS scenario");
    assert!(!scenario.sources.is_empty(), "scenario has no sources");
    for (index, source) in scenario.sources.iter().enumerate() {
        assert!(
            !scenario.sources[..index].contains(source),
            "duplicate source"
        );
    }
    for rule in &scenario.rules {
        assert!(
            rule.phase == Phase::Content || rule.contains.is_none(),
            "header rules cannot inspect bytes"
        );
    }
    assert!(
        scenario.load_error.is_some() || !scenario.steps.is_empty(),
        "scenario has no assertions"
    );
    for kind in &scenario.sources {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_source(yaml, *kind)));
        if let Err(error) = result {
            eprintln!("VFS scenario source: {kind:?}");
            std::panic::resume_unwind(error);
        }
    }
}

fn run_source(yaml: &str, kind: SourceKind) {
    let scenario: Scenario = orbit_utils::yaml::from_str(yaml).unwrap();
    sources::validate(&scenario, kind);
    let root = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let reads = AtomicUsize::new(0);
    let puts = Arc::new(AtomicUsize::new(0));
    let cancel = scenario.options.cancel_after.map(|after| {
        Box::new(move || puts.fetch_add(1, SeqCst) >= after) as Box<dyn Fn() -> bool + Send + Sync>
    });
    let options = orbit_utils::vfs::Options {
        scratch_dir: match scenario.options.scratch {
            Scratch::Default => None,
            Scratch::Existing => Some(scratch.path().into()),
            Scratch::Missing => Some(scratch.path().join("missing")),
        },
        compress_spill: scenario.options.compress_spill,
        cancelled: cancel,
    };
    let middle = scenario.rules.len() / 2;
    let policy =
        Policy::new(&scenario.rules[..middle]).then(Policy::new(&scenario.rules[middle..]));
    let vfs = Vfs::load(
        sources::Input {
            scenario: &scenario,
            kind,
            root: root.path(),
            reads: &reads,
        },
        policy,
        scenario.limits.store(),
        options,
    );
    let vfs = match (vfs, &scenario.load_error) {
        (Err(error), Some(expected)) => {
            let actual = match error {
                orbit_utils::vfs::SourceError::Cap(cap) => format!("cap:{}", cap.metric),
                orbit_utils::vfs::SourceError::Io(error) => error_name(&error),
                orbit_utils::vfs::SourceError::Empty => "empty".into(),
                orbit_utils::vfs::SourceError::Cancelled => "cancelled".into(),
            };
            assert_eq!(&actual, expected);
            assert!(
                scenario.steps.is_empty(),
                "steps cannot run after a failed load"
            );
            return;
        }
        (Ok(vfs), None) => vfs,
        (actual, expected) => panic!("load expected {expected:?}, got {actual:?}"),
    };
    for (index, step) in scenario.steps.into_iter().enumerate() {
        eprintln!("step {index}: {step:?}");
        match step {
            Step::Read { path, expect } => match expect {
                Outcome::Ok(value) => check(
                    vfs.read(Path::new(&path)).map(|b| b.to_vec()),
                    Outcome::Ok(Success {
                        ok: value.ok.bytes(),
                    }),
                ),
                Outcome::Err(error) => check(
                    vfs.read(Path::new(&path)).map(|b| b.to_vec()),
                    Outcome::Err(error),
                ),
            },
            Step::ReadDir { path, expect } => check(vfs.read_dir(Path::new(&path)), expect),
            Step::Stat { path, expect } => check(
                vfs.stat(Path::new(&path)).map(|stat| Stat {
                    path: stat.path.to_string_lossy().into_owned(),
                    kind: match stat.kind {
                        orbit_utils::vfs::Kind::File => Kind::File,
                        orbit_utils::vfs::Kind::Dir => Kind::Dir,
                    },
                    len: stat.len,
                    decision: stat.decision.map(verdict),
                    link: stat.link.map(|p| p.to_string_lossy().into_owned()),
                }),
                expect,
            ),
            Step::Files { expect } => assert_eq!(
                vfs.files()
                    .map(|file| Row {
                        path: file.path.clone(),
                        size: file.size,
                        decision: verdict(file.decision())
                    })
                    .collect::<Vec<_>>(),
                expect
            ),
            Step::Subtree { path, expect } => assert_eq!(
                vfs.subtree(Path::new(&path))
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>(),
                expect
            ),
            Step::LazyReads { expect } => assert_eq!(reads.load(SeqCst), expect),
            Step::Usage { expect } => check_usage(&vfs, expect),
            Step::Write { path, data } => {
                assert!(matches!(kind, SourceKind::Checkout | SourceKind::Changed));
                sources::write(root.path(), &path, &data.bytes());
            }
            Step::Remove { path } => {
                assert!(matches!(kind, SourceKind::Checkout | SourceKind::Changed));
                std::fs::remove_file(sources::disk_path(root.path(), &path)).unwrap();
            }
        }
    }
}

fn check_usage(vfs: &Vfs<Tag>, expected: Usage) {
    let usage = vfs.usage();
    let pairs = [
        (
            "files",
            expected.files.map(|n| n as u64),
            usage.files as u64,
        ),
        ("bytes", expected.bytes, usage.bytes),
        ("kept", expected.kept, usage.kept),
        ("resident", expected.resident, usage.resident),
        ("spilled", expected.spilled, usage.spilled),
        ("deduped_bytes", expected.deduped_bytes, usage.deduped_bytes),
        (
            "duplicate_paths",
            expected.duplicate_paths.map(|n| n as u64),
            usage.duplicate_paths as u64,
        ),
    ];
    assert!(
        pairs.iter().any(|(_, value, _)| value.is_some()) || expected.spilled_below.is_some(),
        "empty usage assertion"
    );
    for (name, expected, actual) in pairs {
        if let Some(expected) = expected {
            assert_eq!(actual, expected, "{name}");
        }
    }
    if let Some(max) = expected.spilled_below {
        assert!(usage.spilled < max, "spilled {} >= {max}", usage.spilled);
    }
}
