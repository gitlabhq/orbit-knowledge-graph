mod cluster;

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster::{
    Bounds, Coverage, RANDOM_RUN_BOUNDS, RandomRun, SMALL_CLUSTER, SMALL_CLUSTER_WITH_DUPLICATE,
    SMALL_CLUSTER_WITH_NATS_WIPE, explore_every_state, random_run, write_runs_module,
};
use indexing_engine_next::protocol::{
    Claim, Fence, Generation, HolderId, Lease, LeaseEntry, LeaseRules, Publish, Renewal, Revision,
    TaskId, decide_submit,
};
use uuid::Uuid;

const DEFAULT_RUNS: u64 = 2_000;
const RUNS_PER_TLC_CHUNK: usize = 100;
const STEPS: usize = 60;
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

fn environment_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn random_runs() -> &'static [RandomRun] {
    static RUNS: OnceLock<Vec<RandomRun>> = OnceLock::new();
    RUNS.get_or_init(|| {
        let first = environment_number("ENGINE_SPEC_FIRST_SEED", 0);
        let count = environment_number("ENGINE_SPEC_RUNS", DEFAULT_RUNS);
        (first..first + count)
            .map(|seed| random_run(seed, STEPS))
            .collect()
    })
}

fn spec_output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("indexing-engine-next-spec")
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
fn random_runs_with_stalls_crashes_and_nats_faults_keep_the_rules() {
    let runs = random_runs();
    assert!(!runs.is_empty());
}

#[test]
fn random_runs_reach_the_path_of_each_fix() {
    let mut coverage = Coverage::default();
    for run in random_runs() {
        coverage += run.coverage;
    }

    assert!(coverage.fence_rejections > 0, "{coverage:?}");
    assert!(coverage.foreign_markers_kept > 0, "{coverage:?}");
    assert!(coverage.republished_markers > 0, "{coverage:?}");
    assert!(coverage.lost_renewals > 0, "{coverage:?}");
    assert!(coverage.releases > 0, "{coverage:?}");
    assert!(coverage.recoveries > 0, "{coverage:?}");
    assert!(coverage.duplicates > 0, "{coverage:?}");
}

#[test]
fn random_runs_are_written_in_chunks_for_the_tlc_check_against_the_spec() {
    let runs = random_runs();
    let dir = spec_output_dir().join("runs");
    let _ = std::fs::remove_dir_all(&dir);

    for (n, chunk) in runs.chunks(RUNS_PER_TLC_CHUNK).enumerate() {
        let chunk_dir = dir.join(format!("chunk-{n:03}"));
        std::fs::create_dir_all(&chunk_dir).unwrap();
        let module = write_runs_module(chunk, RANDOM_RUN_BOUNDS.holders);
        std::fs::write(chunk_dir.join("LeaseDedupeRuns.tla"), module).unwrap();
    }

    let seeds: Vec<u64> = runs.iter().map(|run| run.seed).collect();
    println!(
        "wrote {} runs, seeds {}..={}",
        runs.len(),
        seeds.first().unwrap(),
        seeds.last().unwrap()
    );
    let chunks = std::fs::read_dir(&dir).unwrap().count();
    assert_eq!(chunks, runs.len().div_ceil(RUNS_PER_TLC_CHUNK));
}

fn explore_and_write_summary(name: &str, bounds: Bounds) {
    let exploration = explore_every_state(bounds);
    let dir = spec_output_dir().join("small-cluster");
    std::fs::create_dir_all(&dir).unwrap();
    let summary = format!("{}\n", exploration.states);
    std::fs::write(dir.join(format!("{name}.txt")), summary).unwrap();

    println!("{name}: the code reaches {} states", exploration.states);
    assert!(exploration.states > 1);
}

#[test]
fn every_state_of_a_small_cluster_keeps_the_rules() {
    explore_and_write_summary("no_nats_faults", SMALL_CLUSTER);
}

#[test]
#[ignore = "explores 1.1 million states, about 11 s in a release build; engine-spec-check runs it"]
fn every_state_of_a_small_cluster_with_a_nats_wipe_keeps_the_rules() {
    explore_and_write_summary("nats_wipe", SMALL_CLUSTER_WITH_NATS_WIPE);
}

#[test]
#[ignore = "explores 4.2 million states, about 40 s in a release build; engine-spec-check runs it"]
fn every_state_of_a_small_cluster_with_a_duplicate_keeps_the_rules() {
    explore_and_write_summary("duplicate", SMALL_CLUSTER_WITH_DUPLICATE);
}
