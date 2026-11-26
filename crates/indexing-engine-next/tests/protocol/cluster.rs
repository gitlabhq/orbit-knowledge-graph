mod tlc;

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::time::{Duration, SystemTime};

use indexing_engine_next::lease::{
    Claim, Fence, Generation, Hold, HolderId, Lease, LeaseRules, Renewal,
};
use indexing_engine_next::task::TaskId;
use indexing_engine_next::testkit::simulation::{
    DatabaseWrite, FaultBudget, MessageState, SimulatedBroker, SimulatedDatabase,
    SimulatedKeyValue, TickClock,
};
use uuid::Uuid;

pub use tlc::{StateGraph, StateId, read_spec_graph};

const UNIT: Duration = Duration::from_secs(20);
const OFFSETS: [u64; 2] = [0, 1];
const TTL: u32 = 3;
const SKEW_MARGIN: u32 = 1;
const ACK_WAIT: u64 = 2;

#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    pub holders: usize,
    pub max_time: u64,
    pub max_events: usize,
    pub faults: FaultBudget,
}

pub const SMALL_CLUSTER: Bounds = Bounds {
    holders: 2,
    max_time: 4,
    max_events: 1,
    faults: FaultBudget {
        stalls: 1,
        crashes: 1,
        wipes: 0,
        duplicates: 0,
        losses: 0,
    },
};

pub const SMALL_CLUSTER_WITH_TWO_TASKS: Bounds = Bounds {
    holders: 2,
    max_time: 4,
    max_events: 2,
    faults: FaultBudget {
        stalls: 0,
        crashes: 1,
        wipes: 0,
        duplicates: 0,
        losses: 0,
    },
};

pub const SMALL_CLUSTER_WITH_NATS_WIPE: Bounds = Bounds {
    holders: 2,
    max_time: 4,
    max_events: 1,
    faults: FaultBudget {
        stalls: 1,
        crashes: 1,
        wipes: 1,
        duplicates: 0,
        losses: 0,
    },
};

pub const SMALL_CLUSTER_WITH_DUPLICATE: Bounds = Bounds {
    holders: 2,
    max_time: 3,
    max_events: 1,
    faults: FaultBudget {
        stalls: 1,
        crashes: 1,
        wipes: 0,
        duplicates: 1,
        losses: 0,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Tick,
    Submit,
    Publish,
    PlannerCrash,
    NatsWipe,
    RecoverNats,
    Redeliver(usize),
    DuplicateCopy(usize),
    LoseMessage(usize),
    Deliver(usize, usize),
    AcquireLease(usize),
    StartTask(usize),
    Renew(usize),
    RenewFails(usize),
    BeginWrite(usize),
    CommitWrite(usize),
    Finish(usize),
    Stall(usize),
    Resume(usize),
    Crash(usize),
}

pub fn explore_graph(
    bounds: Bounds,
    keep_states_of: &HashSet<StateId>,
) -> (StateGraph, Vec<String>) {
    let mut graph = StateGraph::default();
    let mut kept = Vec::new();
    let mut queue = VecDeque::new();

    for skews in skew_combinations(bounds.holders) {
        let cluster = Cluster::init(bounds, skews);
        let id = cluster.state_id();
        graph.initial.insert(id);
        if graph.states.insert(id) {
            queue.push_back((cluster, id));
        }
    }

    while let Some((cluster, id)) = queue.pop_front() {
        if let Err(broken) = cluster.check_invariants() {
            panic!("{broken} in state\n{}", cluster.canonical_state());
        }
        if keep_states_of.contains(&id) {
            kept.push(cluster.canonical_state());
        }
        for action in cluster.enabled_actions() {
            let mut next = cluster.clone();
            next.apply(action);
            let next_id = next.state_id();
            graph.steps.insert((id, next_id));
            if graph.states.insert(next_id) {
                queue.push_back((next, next_id));
            }
        }
    }

    (graph, kept)
}

fn skew_combinations(holders: usize) -> Vec<Vec<u64>> {
    (0..holders).fold(vec![Vec::new()], |combinations, _| {
        combinations
            .iter()
            .flat_map(|prefix| {
                OFFSETS.iter().map(move |skew| {
                    let mut next = prefix.clone();
                    next.push(*skew);
                    next
                })
            })
            .collect()
    })
}

#[derive(Clone)]
enum Step {
    Idle,
    Leasing(Claim),
    Leased(Hold),
    Running(Hold),
    Writing(Hold),
    Done(Hold),
}

impl Step {
    fn hold(&self) -> Option<&Hold> {
        match self {
            Step::Leased(hold) | Step::Running(hold) | Step::Writing(hold) | Step::Done(hold) => {
                Some(hold)
            }
            Step::Idle | Step::Leasing(_) => None,
        }
    }

    fn hold_mut(&mut self) -> Option<&mut Hold> {
        match self {
            Step::Leased(hold) | Step::Running(hold) | Step::Writing(hold) | Step::Done(hold) => {
                Some(hold)
            }
            Step::Idle | Step::Leasing(_) => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Step::Idle => "idle",
            Step::Leasing(_) => "leasing",
            Step::Leased(_) => "leased",
            Step::Running(_) => "running",
            Step::Writing(_) => "writing",
            Step::Done(_) => "done",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct LastHold {
    token_generation: u64,
    token: u64,
    seen: u64,
    expiry: u64,
    renew_at: u64,
}

#[derive(Clone)]
struct Holder {
    id: HolderId,
    skew: u64,
    stalled: bool,
    step: Step,
    held: Option<usize>,
    last_hold: LastHold,
}

#[derive(Clone)]
struct Cluster {
    bounds: Bounds,
    rules: LeaseRules,
    clock: TickClock,
    holders: Vec<Holder>,
    lease: SimulatedKeyValue<Lease>,
    pending: SimulatedKeyValue<TaskId>,
    generation: SimulatedKeyValue<Generation>,
    broker: SimulatedBroker,
    database: SimulatedDatabase,
    faults: FaultBudget,
    next_id: u64,
    publishing: Option<TaskId>,
    covers: Vec<TaskId>,
    started: BTreeSet<TaskId>,
    lost_in_wipe: BTreeSet<TaskId>,
    rejected_current: bool,
}

impl Cluster {
    fn init(bounds: Bounds, skews: Vec<u64>) -> Self {
        let holders = skews
            .into_iter()
            .enumerate()
            .map(|(n, skew)| Holder {
                id: HolderId::new(format!("p{}", n + 1)),
                skew,
                stalled: false,
                step: Step::Idle,
                held: None,
                last_hold: LastHold::default(),
            })
            .collect();
        let database = SimulatedDatabase::default();
        let mut generation = SimulatedKeyValue::default();
        let written = generation.put(generation.revision(), Some(database.generation()));
        assert!(
            written.is_ok(),
            "a fresh key-value entry takes its first write"
        );

        Self {
            bounds,
            rules: LeaseRules {
                ttl: UNIT * TTL,
                renew_every: UNIT,
                skew_margin: UNIT * SKEW_MARGIN,
            },
            clock: TickClock::new(UNIT),
            holders,
            lease: SimulatedKeyValue::default(),
            pending: SimulatedKeyValue::default(),
            generation,
            broker: SimulatedBroker::new(ACK_WAIT),
            database,
            faults: bounds.faults,
            next_id: 1,
            publishing: None,
            covers: Vec::new(),
            started: BTreeSet::new(),
            lost_in_wipe: BTreeSet::new(),
            rejected_current: false,
        }
    }

    fn lease_entry(&self) -> indexing_engine_next::lease::LeaseEntry {
        self.lease.entry(self.generation.get().copied())
    }

    fn local_now(&self, h: usize) -> SystemTime {
        self.clock.local_now(self.holders[h].skew)
    }

    fn holds(&self, h: usize) -> Option<&Hold> {
        self.holders[h].step.hold()
    }

    fn is_renew_due(&self, h: usize) -> bool {
        self.holds(h)
            .is_some_and(|hold| hold.is_renew_due(self.local_now(h)))
    }

    fn enabled_actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        let renewals_on_time =
            (0..self.holders.len()).all(|h| self.holders[h].stalled || !self.is_renew_due(h));

        if self.clock.ticks() < self.bounds.max_time && renewals_on_time {
            actions.push(Action::Tick);
        }
        if self.publishing.is_none() && self.covers.len() < self.bounds.max_events {
            actions.push(Action::Submit);
        }
        if self.publishing.is_some() {
            actions.push(Action::Publish);
            if self.faults.crashes > 0 {
                actions.push(Action::PlannerCrash);
            }
        }
        if self.faults.wipes > 0 {
            actions.push(Action::NatsWipe);
        }
        if self.generation.get().is_none() {
            actions.push(Action::RecoverNats);
        }
        actions.extend(
            self.broker
                .expired_messages(self.clock.ticks())
                .map(Action::Redeliver),
        );
        for (c, message) in self.broker.messages().iter().enumerate() {
            if self.faults.duplicates > 0 && message.state != MessageState::Lost {
                actions.push(Action::DuplicateCopy(c));
            }
        }
        for (c, message) in self.broker.messages().iter().enumerate() {
            let live = matches!(message.state, MessageState::Ready | MessageState::Inflight);
            if self.faults.losses > 0 && live {
                actions.push(Action::LoseMessage(c));
            }
        }
        for h in 0..self.holders.len() {
            self.push_holder_actions(h, &mut actions);
        }

        actions
    }

    fn push_holder_actions(&self, h: usize, actions: &mut Vec<Action>) {
        let holder = &self.holders[h];
        let awake = !holder.stalled;

        if awake {
            match &holder.step {
                Step::Idle => {
                    actions.extend(self.broker.ready_messages().map(|c| Action::Deliver(h, c)))
                }
                Step::Leasing(claim) => {
                    if claim
                        .lease_if_free(&self.rules, &self.lease_entry(), self.local_now(h))
                        .is_some()
                    {
                        actions.push(Action::AcquireLease(h));
                    }
                }
                Step::Leased(_) => actions.push(Action::StartTask(h)),
                Step::Running(_) => actions.push(Action::BeginWrite(h)),
                Step::Writing(_) => actions.push(Action::CommitWrite(h)),
                Step::Done(_) => actions.push(Action::Finish(h)),
            }
        }
        if awake && self.is_renew_due(h) {
            actions.push(Action::Renew(h));
            if self.faults.crashes > 0 {
                actions.push(Action::RenewFails(h));
            }
        }
        if awake && holder.step.hold().is_some() && self.faults.stalls > 0 {
            actions.push(Action::Stall(h));
        }
        if holder.stalled {
            actions.push(Action::Resume(h));
        }
        if !matches!(holder.step, Step::Idle) && self.faults.crashes > 0 {
            actions.push(Action::Crash(h));
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Tick => self.clock.advance(),
            Action::Submit => self.submit(),
            Action::Publish => self.publish(),
            Action::PlannerCrash => {
                self.faults.crashes -= 1;
                self.publishing = None;
            }
            Action::NatsWipe => self.wipe_nats(),
            Action::RecoverNats => self.recover_nats(),
            Action::Redeliver(c) => self.broker.redeliver(c),
            Action::DuplicateCopy(c) => {
                self.faults.duplicates -= 1;
                self.broker.duplicate(c);
            }
            Action::LoseMessage(c) => {
                self.faults.losses -= 1;
                self.broker.lose(c);
            }
            Action::Deliver(h, c) => self.deliver(h, c),
            Action::AcquireLease(h) => self.acquire_lease(h),
            Action::StartTask(h) => self.start_task(h),
            Action::Renew(h) => self.renew(h),
            Action::RenewFails(h) => {
                self.faults.crashes -= 1;
                self.drop_run(h);
            }
            Action::BeginWrite(h) => self.advance(h),
            Action::CommitWrite(h) => self.commit_write(h),
            Action::Finish(h) => self.finish(h),
            Action::Stall(h) => {
                self.faults.stalls -= 1;
                self.holders[h].stalled = true;
            }
            Action::Resume(h) => self.holders[h].stalled = false,
            Action::Crash(h) => {
                self.faults.crashes -= 1;
                self.drop_run(h);
                self.holders[h].stalled = false;
            }
        }
        self.remember_holds();
    }

    fn submit(&mut self) {
        match self.pending.get().copied() {
            Some(pending) => self.publishing = Some(pending),
            None => {
                let task = task_id(self.next_id);
                let created = self.pending.put(self.pending.revision(), Some(task));
                assert!(created.is_ok(), "a submit is atomic in the spec");
                self.next_id += 1;
                self.publishing = Some(task);
            }
        }
    }

    fn publish(&mut self) {
        let Some(task) = self.publishing.take() else {
            return;
        };
        self.broker.publish(task);
        self.covers.push(task);
    }

    fn deliver(&mut self, h: usize, c: usize) {
        let task = self.broker.deliver(c, h, self.clock.ticks());
        let holder = &mut self.holders[h];
        holder.step = Step::Leasing(Claim::new(holder.id.clone(), task));
        holder.held = Some(c);
    }

    fn acquire_lease(&mut self, h: usize) {
        let local_now = self.local_now(h);
        let Step::Leasing(claim) = std::mem::replace(&mut self.holders[h].step, Step::Idle) else {
            return;
        };
        let entry = self.lease_entry();
        let Some(lease) = claim.lease_if_free(&self.rules, &entry, local_now) else {
            return;
        };
        let Some(generation) = entry.generation else {
            return;
        };
        let Ok(written) = self.lease.put(entry.revision, Some(lease.clone())) else {
            return;
        };
        let fence = Fence {
            generation,
            revision: written,
        };
        self.holders[h].step = Step::Leased(claim.into_hold(&self.rules, &lease, fence, local_now));
    }

    fn start_task(&mut self, h: usize) {
        let Step::Leased(hold) = std::mem::replace(&mut self.holders[h].step, Step::Idle) else {
            return;
        };
        self.started.insert(hold.task());
        if self.pending.get().copied() == Some(hold.task()) {
            let cleared = self.pending.put(self.pending.revision(), None);
            assert!(cleared.is_ok(), "a start is atomic in the spec");
        }
        self.holders[h].step = Step::Running(hold);
    }

    fn renew(&mut self, h: usize) {
        let local_now = self.local_now(h);
        let entry = self.lease_entry();
        let Some(renewal) = self
            .holds(h)
            .map(|hold| hold.build_renewal(&self.rules, &entry, local_now))
        else {
            return;
        };
        match renewal {
            Renewal::Extend(lease) => {
                let Ok(written) = self.lease.put(entry.revision, Some(lease.clone())) else {
                    return;
                };
                if let Some(hold) = self.holders[h].step.hold_mut() {
                    hold.record_renewal(&self.rules, &lease, written, local_now);
                }
                if let Some(c) = self.holders[h].held {
                    self.broker.ack_progress(c, h, self.clock.ticks());
                }
            }
            Renewal::Lost => {
                self.drop_run(h);
            }
        }
    }

    fn advance(&mut self, h: usize) {
        let step = std::mem::replace(&mut self.holders[h].step, Step::Idle);
        self.holders[h].step = match step {
            Step::Running(hold) => Step::Writing(hold),
            Step::Writing(hold) => Step::Done(hold),
            other => other,
        };
    }

    fn commit_write(&mut self, h: usize) {
        let Some(fence) = self.holds(h).map(Hold::fence) else {
            return;
        };
        match self.database.write(fence) {
            DatabaseWrite::Kept => self.advance(h),
            DatabaseWrite::Rejected => {
                let entry = self.lease_entry();
                let holds_current = self
                    .holds(h)
                    .is_some_and(|hold| hold.revision_to_release(&entry).is_some());
                self.rejected_current |= holds_current;
                self.drop_run(h);
            }
        }
    }

    fn finish(&mut self, h: usize) {
        if let Some(c) = self.holders[h].held {
            self.broker.ack(c);
        }
        let release = self
            .holds(h)
            .and_then(|hold| hold.revision_to_release(&self.lease_entry()));
        if let Some(expected) = release
            && self.lease.put(expected, None).is_ok()
        {}
        self.drop_run(h);
    }

    fn wipe_nats(&mut self) {
        self.faults.wipes -= 1;
        self.lease.wipe();
        self.pending.wipe();
        self.generation.wipe();
        self.broker.wipe();
        let unfinished: Vec<TaskId> = (1..self.next_id)
            .map(task_id)
            .filter(|task| !self.broker.is_done(*task))
            .collect();
        self.lost_in_wipe.extend(unfinished);
    }

    fn recover_nats(&mut self) {
        let next = self.database.start_next_generation();
        let written = self.generation.put(self.generation.revision(), Some(next));
        assert!(written.is_ok(), "a recovery is atomic in the spec");
    }

    fn drop_run(&mut self, h: usize) {
        self.holders[h].step = Step::Idle;
        self.holders[h].held = None;
    }

    fn remember_holds(&mut self) {
        for holder in &mut self.holders {
            if let Some(hold) = holder.step.hold() {
                holder.last_hold = LastHold {
                    token_generation: hold.fence().generation.0,
                    token: hold.fence().revision.0,
                    seen: hold.seen_revision().0,
                    expiry: self.clock.ticks_at(hold.expires_at()),
                    renew_at: self.clock.ticks_at(hold.renew_at()),
                };
            }
        }
    }

    fn check_invariants(&self) -> Result<(), String> {
        if self.rejected_current {
            return Err("NoRejectedCurrentHolder: the database refused the write of the current lease holder".into());
        }

        let pending_task = self.pending.get().copied();
        let waiting: Vec<TaskId> = self
            .broker
            .live_tasks()
            .filter(|task| !self.started.contains(task) && !self.lost_in_wipe.contains(task))
            .collect();
        if waiting.iter().any(|task| Some(*task) != pending_task) {
            return Err(format!(
                "AtMostOnePending: waiting {waiting:?}, pending {pending_task:?}"
            ));
        }

        let lost = self.covers.iter().find(|task| {
            !self.started.contains(task)
                && !self.lost_in_wipe.contains(task)
                && !self.broker.has_live_message(**task)
                && self.publishing != Some(**task)
        });
        if let Some(task) = lost {
            return Err(format!("EverySubmitCovered: task {task:?} is lost"));
        }

        Ok(())
    }

    fn canonical_state(&self) -> String {
        tlc::canonical(tlc::state_lines(self))
    }

    fn state_id(&self) -> StateId {
        tlc::state_id(&self.canonical_state())
    }
}

fn task_id(spec_id: u64) -> TaskId {
    TaskId::new(Uuid::from_u128(spec_id.into()))
}
