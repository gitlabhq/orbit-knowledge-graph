mod schema;
mod sources;

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

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
                    };
                    (rule.clone(), decision)
                })
                .collect(),
        )
    }

    fn apply(
        &self,
        phase: Phase,
        path: &str,
        bytes: &[u8],
        mut result: Decision<Tag>,
    ) -> Decision<Tag> {
        for (rule, decision) in &self.0 {
            if rule.phase == phase
                && rule
                    .suffix
                    .as_ref()
                    .is_none_or(|suffix| path.ends_with(suffix))
                && rule.contains.as_ref().is_none_or(|data| {
                    let needle = data.as_bytes();
                    needle.is_empty() || bytes.windows(needle.len()).any(|window| window == needle)
                })
            {
                result = *decision;
            }
        }
        result
    }
}

impl Pass for Policy {
    type Tag = Tag;
    fn metadata(&self, file: &File<'_, Tag>) -> Decision<Tag> {
        assert!(file.bytes().is_none());
        self.apply(Phase::Metadata, &file.path, &[], file.decision())
    }
    fn content(&self, file: &File<'_, Tag>) -> Decision<Tag> {
        self.apply(
            Phase::Content,
            &file.path,
            file.bytes().expect("content phase supplies bytes"),
            file.decision(),
        )
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
        Decision::Pending => Verdict::Pending,
        Decision::Keep(tag) => Verdict::Keep(tag),
        Decision::List(reason) => Verdict::List(reason.into()),
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
    let suite: Suite = orbit_utils::yaml::from_str(yaml).expect("invalid VFS suite");
    match suite {
        Suite::Scenario(scenario) => run_scenario(&scenario),
        Suite::Scenarios(scenarios) => {
            assert!(!scenarios.is_empty(), "suite has no scenarios");
            for scenario in scenarios {
                run_scenario(&scenario);
            }
        }
    }
}

fn run_scenario(scenario: &Scenario) {
    assert!(!scenario.name.trim().is_empty(), "scenario has no name");
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
            "metadata rules cannot inspect bytes"
        );
    }
    assert!(
        scenario.load_error.is_some() || !scenario.tests.is_empty(),
        "scenario has no assertions"
    );
    assert!(
        scenario.load_error.is_none() || scenario.tests.is_empty(),
        "tests cannot run after a failed load"
    );
    for test in &scenario.tests {
        assert!(!test.name.trim().is_empty(), "test has no name");
        assert!(
            test.assert
                .iter()
                .any(|step| !matches!(step, Step::Write { .. } | Step::Remove { .. })),
            "test has no assertions"
        );
    }
    for kind in &scenario.sources {
        assert!(
            scenario.changeset.is_none() || *kind == SourceKind::Changeset,
            "changeset paths require the changeset source"
        );
        for file in &scenario.fixtures {
            assert!(
                file.link.is_none()
                    || (file.content.is_empty()
                        && matches!(
                            kind,
                            SourceKind::Directory | SourceKind::Changeset | SourceKind::Archive
                        )),
                "links require a filesystem source and cannot have content"
            );
        }
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_source(scenario, *kind)));
        if let Err(error) = result {
            eprintln!("VFS scenario: {}, source: {kind:?}", scenario.name);
            std::panic::resume_unwind(error);
        }
    }
}

fn run_source(scenario: &Scenario, kind: SourceKind) {
    let root = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let puts = AtomicUsize::new(0);
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
            scenario,
            kind,
            root: root.path(),
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
            return;
        }
        (Ok(vfs), None) => vfs,
        (actual, expected) => panic!("load expected {expected:?}, got {actual:?}"),
    };
    for test in &scenario.tests {
        eprintln!("test: {}", test.name);
        for step in test.assert.clone() {
            eprintln!("assert: {step:?}");
            match step {
                Step::Read { path, expect } => match expect {
                    Outcome::Ok(value) => check(
                        vfs.read(Path::new(&path)).map(|b| b.to_vec()),
                        Outcome::Ok(Success {
                            ok: value.ok.into_bytes(),
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
                            path: file.path.to_string(),
                            size: file.size,
                            decision: verdict(file.decision())
                        })
                        .collect::<Vec<_>>(),
                    expect
                ),
                Step::Subtree { path, expect } => assert_eq!(
                    vfs.subtree(Path::new(&path))
                        .map(|file| file.path.to_string())
                        .collect::<Vec<_>>(),
                    expect
                ),
                Step::Usage { expect } => check_usage(&vfs, expect),
                Step::Write { path, content } => {
                    assert!(matches!(
                        kind,
                        SourceKind::Directory | SourceKind::Changeset
                    ));
                    sources::write(root.path(), &path, content.as_bytes());
                }
                Step::Remove { path } => {
                    assert!(matches!(
                        kind,
                        SourceKind::Directory | SourceKind::Changeset
                    ));
                    std::fs::remove_file(sources::disk_path(root.path(), &path)).unwrap();
                }
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
