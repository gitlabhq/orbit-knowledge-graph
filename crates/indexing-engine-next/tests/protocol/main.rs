mod cluster;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster::{
    Bounds, SMALL_CLUSTER, SMALL_CLUSTER_WITH_DUPLICATE, SMALL_CLUSTER_WITH_NATS_WIPE,
    SMALL_CLUSTER_WITH_TWO_TASKS, StateGraph, explore_graph, read_spec_graph,
};
use indexing_engine_next::protocol::{
    Claim, Fence, Generation, HolderId, Lease, LeaseEntry, LeaseRules, Publish, Renewal, Revision,
    TaskId, decide_submit,
};
use uuid::Uuid;

const SMALL_CLUSTERS: [(&str, Bounds); 4] = [
    ("no_nats_faults", SMALL_CLUSTER),
    ("two_tasks", SMALL_CLUSTER_WITH_TWO_TASKS),
    ("nats_wipe", SMALL_CLUSTER_WITH_NATS_WIPE),
    ("duplicate", SMALL_CLUSTER_WITH_DUPLICATE),
];
const FIRST_GENERATION: Generation = Generation(1);

const RULES: LeaseRules = LeaseRules {
    ttl: Duration::from_secs(60),
    renew_every: Duration::from_secs(20),
    skew_margin: Duration::from_secs(5),
};

fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn task(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn holder(name: &str) -> HolderId {
    HolderId::new(name)
}

fn held_by(name: &str, expires_at: u64, revision: u64) -> LeaseEntry {
    LeaseEntry {
        lease: Some(Lease {
            holder: holder(name),
            expires_at: at(expires_at),
        }),
        revision: Revision(revision),
        generation: Some(FIRST_GENERATION),
    }
}

fn compare_with_spec(name: &str, bounds: Bounds, spec_graph: &Path) {
    let none = HashSet::new();
    let (code, _) = explore_graph(bounds, &none);
    let spec = read_spec_graph(spec_graph, &none).graph;
    if code.initial == spec.initial && code.states == spec.states && code.steps == spec.steps {
        println!(
            "{name}: the code and the spec have the same {} states and {} steps",
            code.states.len(),
            code.steps.len()
        );
        return;
    }

    let code_only: HashSet<_> = code
        .states
        .difference(&spec.states)
        .copied()
        .take(3)
        .collect();
    let spec_only: HashSet<_> = spec
        .states
        .difference(&code.states)
        .copied()
        .take(3)
        .collect();
    let (_, code_examples) = explore_graph(bounds, &code_only);
    let spec_examples = read_spec_graph(spec_graph, &spec_only).labels;
    panic!(
        "{name}: the code and the spec differ\n{}\nonly the code reaches:\n{}\nonly the spec reaches:\n{}",
        describe_difference(&code, &spec),
        code_examples.join("\n\n"),
        spec_examples.into_values().collect::<Vec<_>>().join("\n\n"),
    );
}

fn describe_difference(code: &StateGraph, spec: &StateGraph) -> String {
    format!(
        "initial states: code {} spec {}, states: code {} spec {} (only code {}, only spec {}), steps: code {} spec {} (only code {}, only spec {})",
        code.initial.len(),
        spec.initial.len(),
        code.states.len(),
        spec.states.len(),
        code.states.difference(&spec.states).count(),
        spec.states.difference(&code.states).count(),
        code.steps.len(),
        spec.steps.len(),
        code.steps.difference(&spec.steps).count(),
        spec.steps.difference(&code.steps).count(),
    )
}

#[test]
fn an_expired_lease_is_free_only_after_the_skew_margin() {
    let claim = Claim::deliver(holder("p2"), task(1));
    let entry = held_by("p1", 100, 7);

    assert_eq!(claim.acquire_lease(&RULES, &entry, at(104)), None);
    assert_eq!(
        claim.acquire_lease(&RULES, &entry, at(105)),
        Some(Lease {
            holder: holder("p2"),
            expires_at: at(165)
        })
    );
}

#[test]
fn the_fence_is_the_lease_revision_when_the_holder_took_it() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let lease = claim
        .acquire_lease(&RULES, &held_by("p0", 0, 3), at(10))
        .unwrap();
    let mut hold = claim.hold(&RULES, &lease, Revision(4), FIRST_GENERATION, at(10));

    let renewal = hold.renew(&RULES, &held_by("p1", 70, 4), at(30));
    let Renewal::Extend(renewed) = renewal else {
        panic!("the holder of the current lease renews it");
    };
    hold.renewed(&RULES, &renewed, Revision(5), at(30));

    assert_eq!(
        hold.fence(),
        Fence {
            generation: FIRST_GENERATION,
            revision: Revision(4)
        }
    );
    assert_eq!(hold.seen_revision(), Revision(5));
}

#[test]
fn a_renewal_is_lost_once_another_holder_took_the_lease() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let lease = claim
        .acquire_lease(&RULES, &held_by("p0", 0, 3), at(10))
        .unwrap();
    let hold = claim.hold(&RULES, &lease, Revision(4), FIRST_GENERATION, at(10));

    assert_eq!(
        hold.renew(&RULES, &held_by("p2", 200, 5), at(90)),
        Renewal::Lost
    );
    assert_eq!(hold.release(&held_by("p2", 200, 5)), None);
    assert_eq!(hold.release(&held_by("p1", 70, 4)), Some(Revision(4)));
}

#[test]
fn a_renewal_is_lost_once_the_same_holder_took_the_lease_again_for_another_run() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let lease = claim
        .acquire_lease(&RULES, &held_by("p0", 0, 3), at(10))
        .unwrap();
    let hold = claim.hold(&RULES, &lease, Revision(4), FIRST_GENERATION, at(10));

    assert_eq!(
        hold.renew(&RULES, &held_by("p1", 200, 6), at(90)),
        Renewal::Lost
    );
    assert_eq!(hold.release(&held_by("p1", 200, 6)), None);
}

#[test]
fn a_start_clears_only_the_marker_of_its_own_task() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let lease = claim
        .acquire_lease(&RULES, &held_by("p0", 0, 3), at(10))
        .unwrap();
    let hold = claim.hold(&RULES, &lease, Revision(4), FIRST_GENERATION, at(10));

    assert!(hold.start_clears_marker(Some(task(1))));
    assert!(!hold.start_clears_marker(Some(task(2))));
    assert!(!hold.start_clears_marker(None));
}

#[test]
fn a_submit_that_finds_a_marker_publishes_that_task_again() {
    assert_eq!(decide_submit(None, task(2)), Publish::New(task(2)));
    assert_eq!(
        decide_submit(Some(task(1)), task(2)),
        Publish::Pending(task(1))
    );
}

#[test]
fn no_lease_is_taken_until_the_key_value_store_is_recovered_after_a_wipe() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let wiped = LeaseEntry {
        lease: None,
        revision: Revision(0),
        generation: None,
    };

    assert_eq!(claim.acquire_lease(&RULES, &wiped, at(10)), None);
}

#[test]
fn a_fence_of_a_newer_generation_beats_any_revision_of_an_older_one() {
    let before_wipe = Fence {
        generation: Generation(1),
        revision: Revision(4_000),
    };
    let after_wipe = Fence {
        generation: Generation(2),
        revision: Revision(1),
    };

    assert!(before_wipe < after_wipe);
}

#[test]
fn every_state_of_a_small_cluster_keeps_the_rules() {
    let (graph, _) = explore_graph(SMALL_CLUSTER, &HashSet::new());
    assert_eq!(graph.initial.len(), 4);
    assert!(graph.states.len() > graph.initial.len());
}

#[test]
#[ignore = "needs the state graphs that TLC dumps; mise run spec:engine:code writes them and runs this test"]
fn every_state_and_step_of_the_small_clusters_matches_the_spec() {
    let graphs = PathBuf::from(
        std::env::var("ENGINE_SPEC_TLC_GRAPHS")
            .expect("ENGINE_SPEC_TLC_GRAPHS names the folder of the TLC graphs"),
    );
    for (name, bounds) in SMALL_CLUSTERS {
        compare_with_spec(name, bounds, &graphs.join(format!("{name}.dot")));
    }
}
