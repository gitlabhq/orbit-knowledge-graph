use std::path::PathBuf;
use std::time::Duration;

use indexing_engine_next::protocol::{Fence, Generation, Revision, TaskId};
use indexing_engine_next::testkit::simulation::{
    DatabaseWrite, FaultBudget, MessageState, RevisionConflict, SimulatedBroker, SimulatedDatabase,
    SimulatedKeyValue, Simulation, SimulationReport, TickClock,
};
use uuid::Uuid;

const SEEDS: u64 = 300;
const SEEDS_PER_TRACE_MODULE: usize = 50;

fn task(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn fence(generation: u64, revision: u64) -> Fence {
    Fence {
        generation: Generation(generation),
        revision: Revision(revision),
    }
}

#[test]
fn a_key_value_write_with_a_stale_revision_is_a_conflict() {
    let mut key_value = SimulatedKeyValue::default();
    let first = key_value.put(Revision(0), Some(task(1))).unwrap();

    assert_eq!(
        key_value.put(Revision(0), Some(task(2))),
        Err(RevisionConflict { current: first })
    );
    assert_eq!(key_value.get(), Some(&task(1)));
    assert_eq!(key_value.put(first, None), Ok(Revision(2)));
    assert_eq!(key_value.get(), None);
}

#[test]
fn a_publish_of_a_task_with_a_live_message_adds_no_message() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.publish(task(1));
    assert_eq!(broker.messages().len(), 1);

    broker.acknowledge(0);
    broker.publish(task(1));
    assert_eq!(broker.messages().len(), 2);
}

#[test]
fn a_message_comes_back_after_the_acknowledge_wait_unless_its_consumer_acknowledges_progress() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.deliver(0, 0, 10);

    assert_eq!(broker.expired_messages(11).count(), 0);
    broker.acknowledge_progress(0, 1, 11);
    assert_eq!(broker.expired_messages(12).collect::<Vec<_>>(), [0]);

    broker.acknowledge_progress(0, 0, 11);
    assert_eq!(broker.expired_messages(12).count(), 0);

    broker.redeliver(0);
    assert_eq!(broker.messages()[0].state, MessageState::Ready);
    assert_eq!(broker.messages()[0].consumer, None);
}

#[test]
fn the_database_rejects_a_write_with_a_lower_fence_than_one_it_kept() {
    let mut database = SimulatedDatabase::default();

    assert_eq!(database.write(fence(1, 4)), DatabaseWrite::Kept);
    assert_eq!(database.write(fence(1, 3)), DatabaseWrite::Rejected);
    assert_eq!(database.write(fence(1, 4)), DatabaseWrite::Kept);
    assert_eq!(database.highest_fence(), fence(1, 4));
}

#[test]
fn a_new_generation_survives_in_the_database_and_beats_older_fences() {
    let mut database = SimulatedDatabase::default();
    assert_eq!(database.write(fence(1, 4_000)), DatabaseWrite::Kept);

    let next = database.start_next_generation();

    assert_eq!(next, Generation(2));
    assert_eq!(database.write(fence(next.0, 1)), DatabaseWrite::Kept);
    assert_eq!(database.write(fence(1, 4_001)), DatabaseWrite::Rejected);
}

#[test]
fn a_wiped_key_value_entry_starts_its_revisions_again() {
    let mut key_value = SimulatedKeyValue::default();
    let first = key_value.put(Revision(0), Some(task(1))).unwrap();
    key_value.put(first, Some(task(2))).unwrap();

    key_value.wipe();

    assert_eq!(key_value.get(), None);
    assert_eq!(key_value.put(Revision(0), Some(task(3))), Ok(Revision(1)));
}

#[test]
fn a_wipe_loses_every_live_message_and_an_acknowledge_cannot_bring_one_back() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.publish(task(2));
    broker.deliver(1, 0, 0);
    broker.acknowledge(1);
    broker.publish(task(3));

    broker.wipe();
    broker.acknowledge(0);

    let states: Vec<MessageState> = broker.messages().iter().map(|m| m.state).collect();
    assert_eq!(
        states,
        [
            MessageState::Lost,
            MessageState::Acknowledged,
            MessageState::Lost
        ]
    );
    assert!(!broker.has_live_message(task(1)));
}

#[test]
fn a_duplicate_is_a_second_ready_message_of_the_same_task() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.deliver(0, 0, 0);

    broker.duplicate(0);

    assert_eq!(broker.messages().len(), 2);
    assert_eq!(broker.messages()[1].task, task(1));
    assert_eq!(broker.ready_messages().collect::<Vec<_>>(), [1]);
}

#[test]
fn a_lost_message_is_no_longer_live() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));

    broker.lose(0);

    assert!(!broker.has_live_message(task(1)));
    assert_eq!(broker.ready_messages().count(), 0);
}

#[test]
fn a_skewed_holder_reads_a_later_time_from_the_same_clock() {
    let mut clock = TickClock::new(Duration::from_secs(20));
    clock.advance();

    assert_eq!(clock.ticks_at(clock.local_now(0)), 1);
    assert_eq!(clock.ticks_at(clock.local_now(1)), 2);
}

fn two_processes() -> Simulation {
    Simulation::new()
}

fn three_processes() -> Simulation {
    Simulation::new()
        .processes(3)
        .submits(3)
        .faults(FaultBudget {
            stalls: 2,
            crashes: 2,
            wipes: 1,
            duplicates: 1,
            losses: 0,
        })
}

async fn run_seeds(simulation: &Simulation) -> Vec<SimulationReport> {
    let mut reports = Vec::new();
    for seed in 0..SEEDS {
        reports.push(simulation.run(seed).await);
    }
    reports
}

fn sum(reports: &[SimulationReport], name: &str) -> usize {
    reports.iter().map(|report| report.count(name)).sum()
}

#[tokio::test(start_paused = true)]
async fn every_seed_keeps_the_rules_and_ends_with_every_submit_done() {
    for simulation in [two_processes(), three_processes()] {
        for report in run_seeds(&simulation).await {
            report.assert_rules_held();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn every_seed_keeps_the_rules_when_other_steps_run_inside_a_step() {
    for simulation in [two_processes(), three_processes()] {
        let reports = run_seeds(&simulation.interleave_inside_steps()).await;
        for report in &reports {
            report.assert_rules_held();
        }
        assert!(sum(&reports, "LeaseConflict") > 0);
    }
}

#[tokio::test(start_paused = true)]
async fn the_seeds_reach_every_fault_and_every_fence_rejection() {
    let reports = run_seeds(&three_processes()).await;

    for step in [
        "Stall",
        "Resume",
        "Crash",
        "RenewFails",
        "PlannerCrash",
        "NatsWipe",
        "RecoverNats",
        "DuplicateCopy",
        "Redeliver",
        "RejectedWrite",
        "Renew",
        "Finish",
    ] {
        assert!(sum(&reports, step) > 0, "no seed reached the step {step}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_seed_replays_the_same_steps() {
    let first = three_processes().run(7).await;
    let second = three_processes().run(7).await;

    assert_eq!(format!("{first:?}"), format!("{second:?}"));
}

#[tokio::test(start_paused = true)]
async fn an_interleaved_simulation_writes_no_trace_for_tlc() {
    let simulation = two_processes().interleave_inside_steps();
    let reports = vec![simulation.run(0).await];

    let written = simulation.write_trace_check(&std::env::temp_dir(), &reports);

    assert!(written.is_err());
}

#[tokio::test(start_paused = true)]
#[ignore = "writes the traces that TLC checks; mise run spec:engine:trace runs this test and TLC"]
async fn every_step_of_the_simulated_lane_loop_is_a_step_of_the_spec() {
    let folder = PathBuf::from(
        std::env::var("ENGINE_SPEC_TRACES")
            .expect("ENGINE_SPEC_TRACES names the folder of the traces"),
    );
    for (name, simulation) in [
        ("TwoProcesses", two_processes()),
        ("ThreeProcesses", three_processes()),
    ] {
        let reports = run_seeds(&simulation).await;
        for (chunk, reports) in reports.chunks(SEEDS_PER_TRACE_MODULE).enumerate() {
            for report in reports {
                report.assert_rules_held();
            }
            simulation
                .write_trace_check(&folder.join(format!("{name}{chunk}")), reports)
                .expect("the trace module is written");
        }
    }
}
