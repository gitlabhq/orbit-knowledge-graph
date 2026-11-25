mod broker;
mod faults;
mod key_value;
mod ports;
mod simulated_nats;
mod tick_clock;
mod trace;
mod worker;
mod world;

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::JoinHandle;

use crate::builder::Engine;
use crate::config::{EngineConfig, LaneConfig};
use crate::event::Observer;
use crate::lane::{Lane, LaneId};
use crate::lease::{HolderId, LeaseRules};
use crate::payload::Tenant;
use crate::worker::Worker;

pub use broker::{BrokerMessage, MessageState, SimulatedBroker};
pub use faults::FaultBudget;
pub use key_value::{RevisionConflict, SimulatedKeyValue};
pub use simulated_nats::SimulatedNats;
pub use tick_clock::TickClock;

use ports::{build_ports, build_world_observer};
use trace::TraceStep;
use worker::{SimulatedWrite, WriteInput};
use world::{Actor, SharedWorld, StepOrder, TICK};

const RULES: LeaseRules = LeaseRules {
    ttl: Duration::from_secs(60),
    renew_every: Duration::from_secs(20),
    skew_margin: Duration::from_secs(20),
};
const FAULT_TICKS: u32 = 16;
const HEAL_TICKS: u32 = 30;
const LANE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub struct Simulation {
    processes: usize,
    submits: usize,
    faults: FaultBudget,
    step_order: StepOrder,
    observers: Vec<Arc<dyn Observer>>,
}

#[derive(Clone, Debug)]
pub struct SimulationReport {
    seed: u64,
    broken: Option<String>,
    timeline: Vec<String>,
    trace: Vec<TraceStep>,
    counts: BTreeMap<String, usize>,
}

struct Cluster {
    world: SharedWorld,
    planner: Engine,
    processes: Vec<JoinHandle<()>>,
    restarts: Vec<usize>,
    observers: Vec<Arc<dyn Observer>>,
}

impl Simulation {
    pub fn new() -> Self {
        Self {
            processes: 2,
            submits: 2,
            faults: FaultBudget {
                stalls: 1,
                crashes: 1,
                wipes: 1,
                duplicates: 1,
                losses: 0,
            },
            step_order: StepOrder::Atomic,
            observers: Vec::new(),
        }
    }

    pub fn processes(mut self, processes: usize) -> Self {
        self.processes = processes;
        self
    }

    pub fn submits(mut self, submits: usize) -> Self {
        self.submits = submits;
        self
    }

    pub fn faults(mut self, faults: FaultBudget) -> Self {
        self.faults = faults;
        self
    }

    pub fn observer<O: Observer>(mut self, observer: O) -> Self {
        self.observers.push(Arc::new(observer));
        self
    }

    pub fn interleave_inside_steps(mut self) -> Self {
        self.step_order = StepOrder::Interleaved;
        self
    }

    pub async fn run(&self, seed: u64) -> SimulationReport {
        if Handle::current().runtime_flavor() != RuntimeFlavor::CurrentThread {
            panic!(
                "a simulation replays a seed only on a current-thread runtime, so use #[tokio::test(start_paused = true)]"
            );
        }
        let world = SharedWorld::new(
            seed,
            self.processes,
            self.submits,
            self.faults,
            self.step_order,
        );
        let mut cluster = Cluster::start(world, self.processes, self.observers.clone());

        let start = tokio::time::Instant::now();
        for tick in 0..=(FAULT_TICKS + HEAL_TICKS) {
            tokio::time::sleep_until(start + TICK * tick).await;
            cluster.world.yield_randomly().await;
            let healed = tick >= FAULT_TICKS;
            cluster.advance_broker_and_faults(healed);
            cluster.submit_when_drawn(healed).await;
            if cluster.has_ended(healed) {
                break;
            }
        }
        cluster.stop_and_report()
    }

    pub(super) fn count_ticks(&self) -> u32 {
        FAULT_TICKS + HEAL_TICKS
    }
}

impl Default for Simulation {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulationReport {
    pub fn assert_rules_held(&self) {
        if let Some(rule) = &self.broken {
            panic!(
                "seed {} broke {rule}\n{}",
                self.seed,
                self.timeline.join("\n")
            );
        }
    }

    pub fn count(&self, name: &str) -> usize {
        self.counts.get(name).copied().unwrap_or(0)
    }
}

impl Cluster {
    fn start(world: SharedWorld, processes: usize, observers: Vec<Arc<dyn Observer>>) -> Self {
        let planner = build_engine(
            &world,
            Actor::Planner,
            0,
            EngineConfig::default(),
            &observers,
        );
        let mut cluster = Self {
            world,
            planner,
            processes: Vec::new(),
            restarts: vec![0; processes],
            observers,
        };
        cluster.processes = (0..processes)
            .map(|process| cluster.spawn_process(process))
            .collect();
        cluster
    }

    fn advance_broker_and_faults(&mut self, healed: bool) {
        let mut world = self.world.lock();
        let mut changed = world.redeliver_expired_messages();
        changed |= world.resume_stalls(false);
        if healed {
            changed |= world.stop_faults();
        } else if let Some(fault) = world.draw_fault() {
            let crashed = world.inject_fault(fault);
            drop(world);
            if let Some(process) = crashed {
                self.restart_process(process);
            }
            world = self.world.lock();
            changed = true;
        }
        drop(world);
        if changed {
            self.world.notify_changed();
        }
    }

    async fn submit_when_drawn(&self, healed: bool) {
        let drawn = {
            let mut world = self.world.lock();
            world.has_submits_left() && (healed || world.draw_number(0..=1) == 0)
        };
        if !drawn {
            return;
        }
        let ports = build_ports(&self.world, Actor::Planner);
        let tenant = Tenant::new("simulation");
        let _recorded_by_the_world = self
            .planner
            .submit::<SimulatedWrite>(&ports, tenant, &WriteInput, Lane::DEFAULT)
            .await;
    }

    fn has_ended(&self, healed: bool) -> bool {
        let world = self.world.lock();
        world.is_broken() || (healed && world.are_all_submits_done())
    }

    fn stop_and_report(self) -> SimulationReport {
        {
            let mut world = self.world.lock();
            if !world.are_all_submits_done() {
                world.break_rule("EverySubmitIsDone".to_owned());
            }
        }
        for process in &self.processes {
            process.abort();
        }
        let outcome = self.world.into_outcome();
        SimulationReport {
            seed: outcome.seed,
            broken: outcome.broken,
            timeline: outcome.timeline,
            trace: outcome.trace,
            counts: outcome.counts,
        }
    }

    fn restart_process(&mut self, process: usize) {
        self.processes[process].abort();
        self.restarts[process] += 1;
        self.processes[process] = self.spawn_process(process);
    }

    fn spawn_process(&self, process: usize) -> JoinHandle<()> {
        let holder = HolderId::new(format!("p{}-{}", process + 1, self.restarts[process]));
        let actor = self.world.lock().start_process(holder.clone(), process);
        let lane = LaneId::new(SimulatedWrite::NAME, Lane::DEFAULT);
        let config =
            EngineConfig::default().with_lane(lane, LaneConfig::new(NonZeroU32::MIN, LANE_TIMEOUT));
        let engine = build_engine(&self.world, actor, process, config, &self.observers);
        let ports = build_ports(&self.world, actor);
        tokio::spawn(async move {
            if let Err(error) = engine.serve(ports, holder, RULES).await {
                panic!("the simulated task queue never closes, yet it did: {error:#}");
            }
        })
    }
}

fn build_engine(
    world: &SharedWorld,
    actor: Actor,
    process: usize,
    config: EngineConfig,
    observers: &[Arc<dyn Observer>],
) -> Engine {
    let worker = SimulatedWrite::new(world, actor, process);
    let world_observer = build_world_observer(world, actor);
    let built = Engine::builder()
        .config(config)
        .module(|module| {
            module.worker(worker);
            module.observer(world_observer);
            for observer in observers {
                module.observer(Arc::clone(observer));
            }
        })
        .build();
    match built {
        Ok(engine) => engine,
        Err(error) => panic!("the simulated engine failed to build: {error}"),
    }
}
