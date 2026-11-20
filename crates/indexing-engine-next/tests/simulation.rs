use std::path::PathBuf;

use indexing_engine_next::testkit::simulator::FaultBudget;
use indexing_engine_next::testkit::{Simulation, SimulationReport};

const SEEDS: u64 = 300;
const SEEDS_PER_TRACE_MODULE: usize = 50;

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
