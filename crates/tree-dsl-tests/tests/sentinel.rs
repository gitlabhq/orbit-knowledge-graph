//! The sentinel end to end: a file that overruns its budget keeps its
//! `File` row, tagged with why, and nothing else, while the rest of the run
//! completes; a run that overruns the total budget stops.

use tree_dsl::error::Error;
use tree_dsl::sentinel::{Killed, Limits};
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, State};
use tree_dsl::{inventory, templates};

fn small() -> (String, String) {
    (
        "small.py".into(),
        "from big import work\n\ndef run():\n    work()\n".into(),
    )
}

/// A 2MB file of chained reassignments: parsing alone outlasts a 50ms budget.
fn big() -> (String, String) {
    let mut src = String::from("def work():\n    x = start()\n");
    for i in 0..100_000 {
        src.push_str(&format!("    x = x.step{i}()\n"));
    }
    src.push_str("    return x\n");
    ("big.py".into(), src)
}

fn env(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).expect("python rules compile")
}

fn index(env: &Env) -> Result<(State, Vec<Killed>), Error> {
    let repo = tempfile::tempdir().unwrap();
    for (path, content) in [small(), big()] {
        std::fs::write(repo.path().join(path), content).unwrap();
    }
    let inventory = inventory::walk(repo.path()).unwrap().to_vec();
    let (context, resolved) = templates::index(Context::new(env), repo.path(), inventory)?.finish();
    Ok((resolved.state, context.report.skipped))
}

#[test]
fn file_over_budget_keeps_a_row_with_the_reason_and_the_rest_indexes() {
    // Wide enough for a four-line file even unoptimised, far too tight for 2MB.
    let limits = Limits {
        file_rewrite_ms: 50,
        ..Limits::UNLIMITED
    };
    let env = env(limits);
    let (state, killed) = index(&env).unwrap();

    let big = state
        .trees
        .iter()
        .find(|t| t.label == "big.py")
        .expect("big.py has a File row");
    assert_eq!(
        big.root().descendants().count(),
        0,
        "nothing of big.py was parsed"
    );
    let reason = big
        .get_tag(0, env.lang.syms.intern("reason"))
        .map(|v| env.lang.syms.resolve(v));
    assert_eq!(reason, Some("skip_timeout_walk"));
    assert!(state.trees.iter().any(|t| t.label == "small.py"));
    assert_eq!(killed.len(), 1);
    assert_eq!(
        (killed[0].label, killed[0].path.as_str()),
        ("rewrite", "big.py")
    );
}

#[test]
fn generous_budget_keeps_every_file() {
    let limits = Limits {
        file_rewrite_ms: 120_000,
        file_link_ms: 120_000,
        file_resolve_ms: 120_000,
        total_ms: 600_000,
    };
    let (state, killed) = index(&env(limits)).unwrap();
    assert!(killed.is_empty(), "{killed:?}");
    assert_eq!(state.trees.len(), 2);
}

#[test]
fn total_budget_ends_the_run() {
    let limits = Limits {
        total_ms: 0,
        ..Limits::UNLIMITED
    };
    let err = index(&env(limits)).err();
    assert!(err.is_some(), "a zero total budget must end the run");
}
