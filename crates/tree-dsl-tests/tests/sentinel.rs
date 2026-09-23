//! The sentinel end to end: a file that overruns its budget is dropped from
//! the graph while the rest of the run completes, and a run that overruns
//! the total budget stops.

use tree_dsl::error::Error;
use tree_dsl::pipeline;
use tree_dsl::sentinel::{Killed, Limits};
use tree_dsl::treesitter::SupportLang;
use tree_dsl::{Context, Env, State};

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
    let sources = vec![small().into(), big().into()];
    let (context, resolved) = pipeline::index(Context::new(env), sources)?.finish();
    Ok((resolved.state, context.report.skipped))
}

#[test]
fn file_over_budget_is_dropped_and_the_rest_indexes() {
    // Wide enough for a four-line file even unoptimised, far too tight for 2MB.
    let limits = Limits {
        file_rewrite_ms: 50,
        ..Limits::UNLIMITED
    };
    let (state, killed) = index(&env(limits)).unwrap();

    let labels: Vec<&str> = state.trees.iter().map(|t| t.label.as_str()).collect();
    assert!(labels.contains(&"small.py"), "{labels:?}");
    assert!(
        !labels.contains(&"big.py"),
        "big.py should have been dropped"
    );
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
