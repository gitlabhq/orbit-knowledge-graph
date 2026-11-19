mod cluster;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster::{Coverage, random_run, write_runs_module};
use indexing_engine_next::protocol::{
    Claim, HolderId, Lease, LeaseEntry, LeaseRules, Publish, Renewal, Revision, TaskId,
    decide_submit,
};
use uuid::Uuid;

const RUNS: u64 = 300;
const STEPS: usize = 60;

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
    }
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
    let mut hold = claim.hold(&RULES, &lease, Revision(4), at(10));

    let renewal = hold.renew(&RULES, &held_by("p1", 70, 4), at(30));
    let Renewal::Extend(renewed) = renewal else {
        panic!("the holder of the current lease renews it");
    };
    hold.renewed(&RULES, &renewed, Revision(5), at(30));

    assert_eq!(hold.fence().0, 4);
    assert_eq!(hold.seen_revision(), Revision(5));
}

#[test]
fn a_renewal_is_lost_once_another_holder_took_the_lease() {
    let claim = Claim::deliver(holder("p1"), task(1));
    let lease = claim
        .acquire_lease(&RULES, &held_by("p0", 0, 3), at(10))
        .unwrap();
    let hold = claim.hold(&RULES, &lease, Revision(4), at(10));

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
    let hold = claim.hold(&RULES, &lease, Revision(4), at(10));

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
    let hold = claim.hold(&RULES, &lease, Revision(4), at(10));

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
fn random_runs_with_stalls_and_crashes_keep_one_pending_task_and_lose_no_submit() {
    for seed in 0..RUNS {
        random_run(seed, STEPS);
    }
}

#[test]
fn random_runs_reach_the_path_of_each_fix() {
    let mut coverage = Coverage::default();
    for seed in 0..RUNS {
        coverage += random_run(seed, STEPS).coverage;
    }

    assert!(coverage.fence_rejections > 0, "{coverage:?}");
    assert!(coverage.foreign_markers_kept > 0, "{coverage:?}");
    assert!(coverage.republished_markers > 0, "{coverage:?}");
    assert!(coverage.lost_renewals > 0, "{coverage:?}");
    assert!(coverage.releases > 0, "{coverage:?}");
}

#[test]
fn random_runs_are_written_for_the_tlc_check_against_the_spec() {
    let runs: Vec<_> = (0..RUNS).map(|seed| random_run(seed, STEPS)).collect();
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("indexing-engine-next-spec");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("LeaseDedupeRuns.tla"), write_runs_module(&runs)).unwrap();

    let written = std::fs::read_to_string(dir.join("LeaseDedupeRuns.tla")).unwrap();
    assert_eq!(written.matches("<<\n[now |-> 0,").count(), RUNS as usize);
}
