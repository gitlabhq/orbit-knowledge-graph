mod tla;

use std::collections::BTreeSet;
use std::fmt;
use std::time::{Duration, SystemTime};

use indexing_engine_next::protocol::{
    Claim, Hold, HolderId, Lease, LeaseRules, Publish, Renewal, TaskId, decide_submit,
};
use indexing_engine_next::testkit::simulator::{
    FaultBudget, SimulatedClock, SimulatedKeyValue, SimulatedStore, SimulatedStream, StoreWrite,
};
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};
use uuid::Uuid;

pub use tla::write_runs_module;

const UNIT: Duration = Duration::from_secs(20);
const HOLDERS: usize = 3;
const OFFSETS: [u64; 2] = [0, 1];
const TTL: u32 = 3;
const SKEW_MARGIN: u32 = 1;
const ACKNOWLEDGE_WAIT: u64 = 2;
const MAX_TIME: u64 = 12;
const MAX_EVENTS: usize = 3;
const MAX_STALLS: u32 = 2;
const MAX_CRASHES: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Tick,
    Submit,
    Publish,
    PlannerCrash,
    Redeliver(usize),
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

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Action::Tick => f.write_str("Tick"),
            Action::Submit => f.write_str("Submit"),
            Action::Publish => f.write_str("Publish"),
            Action::PlannerCrash => f.write_str("PlannerCrash"),
            Action::Redeliver(c) => write!(f, "Redeliver({})", c + 1),
            Action::Deliver(p, c) => write!(f, "Deliver(p{},{})", p + 1, c + 1),
            Action::AcquireLease(p) => write!(f, "AcquireLease(p{})", p + 1),
            Action::StartTask(p) => write!(f, "StartTask(p{})", p + 1),
            Action::Renew(p) => write!(f, "Renew(p{})", p + 1),
            Action::RenewFails(p) => write!(f, "RenewFails(p{})", p + 1),
            Action::BeginWrite(p) => write!(f, "BeginWrite(p{})", p + 1),
            Action::CommitWrite(p) => write!(f, "CommitWrite(p{})", p + 1),
            Action::Finish(p) => write!(f, "Finish(p{})", p + 1),
            Action::Stall(p) => write!(f, "Stall(p{})", p + 1),
            Action::Resume(p) => write!(f, "Resume(p{})", p + 1),
            Action::Crash(p) => write!(f, "Crash(p{})", p + 1),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub republished_markers: u32,
    pub foreign_markers_kept: u32,
    pub fence_rejections: u32,
    pub lost_renewals: u32,
    pub releases: u32,
}

impl std::ops::AddAssign for Coverage {
    fn add_assign(&mut self, other: Self) {
        self.republished_markers += other.republished_markers;
        self.foreign_markers_kept += other.foreign_markers_kept;
        self.fence_rejections += other.fence_rejections;
        self.lost_renewals += other.lost_renewals;
        self.releases += other.releases;
    }
}

pub struct Walk {
    pub states: Vec<String>,
    pub coverage: Coverage,
}

pub fn walk(seed: u64, max_steps: usize) -> Walk {
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let mut world = World::init(&mut rng);
    let mut states = vec![world.to_tla()];

    for step in 1..=max_steps {
        let actions = world.enabled_actions();
        if actions.is_empty() {
            break;
        }
        let action = actions[rng.random_range(0..actions.len())];
        world.apply(action);
        if let Err(broken) = world.check_invariants() {
            panic!("seed {seed}, step {step}, {action:?}: {broken}");
        }
        states.push(world.to_tla());
    }

    Walk {
        states,
        coverage: world.coverage,
    }
}

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
    token: u64,
    seen: u64,
    expiry: u64,
    renew_at: u64,
}

struct Holder {
    id: HolderId,
    skew: u64,
    stalled: bool,
    step: Step,
    held: Option<usize>,
    last_hold: LastHold,
}

struct World {
    rules: LeaseRules,
    clock: SimulatedClock,
    holders: Vec<Holder>,
    lease: SimulatedKeyValue<Lease>,
    marker: SimulatedKeyValue<TaskId>,
    stream: SimulatedStream,
    store: SimulatedStore,
    faults: FaultBudget,
    next_id: u64,
    publishing: Option<TaskId>,
    covers: Vec<TaskId>,
    started: BTreeSet<TaskId>,
    coverage: Coverage,
}

impl World {
    fn init(rng: &mut Xoshiro256PlusPlus) -> Self {
        let holders = (1..=HOLDERS)
            .map(|n| Holder {
                id: HolderId::new(format!("p{n}")),
                skew: OFFSETS[rng.random_range(0..OFFSETS.len())],
                stalled: false,
                step: Step::Idle,
                held: None,
                last_hold: LastHold::default(),
            })
            .collect();

        Self {
            rules: LeaseRules {
                ttl: UNIT * TTL,
                renew_every: UNIT,
                skew_margin: UNIT * SKEW_MARGIN,
            },
            clock: SimulatedClock::new(UNIT),
            holders,
            lease: SimulatedKeyValue::default(),
            marker: SimulatedKeyValue::default(),
            stream: SimulatedStream::new(ACKNOWLEDGE_WAIT),
            store: SimulatedStore::default(),
            faults: FaultBudget::new(MAX_STALLS, MAX_CRASHES),
            next_id: 1,
            publishing: None,
            covers: Vec::new(),
            started: BTreeSet::new(),
            coverage: Coverage::default(),
        }
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
            (0..HOLDERS).all(|h| self.holders[h].stalled || !self.is_renew_due(h));

        if self.clock.ticks() < MAX_TIME && renewals_on_time {
            actions.push(Action::Tick);
        }
        if self.publishing.is_none() && self.covers.len() < MAX_EVENTS {
            actions.push(Action::Submit);
        }
        if self.publishing.is_some() {
            actions.push(Action::Publish);
            if self.faults.can_crash() {
                actions.push(Action::PlannerCrash);
            }
        }
        actions.extend(
            self.stream
                .expired_copies(self.clock.ticks())
                .map(Action::Redeliver),
        );
        for h in 0..HOLDERS {
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
                    actions.extend(self.stream.ready_copies().map(|c| Action::Deliver(h, c)))
                }
                Step::Leasing(claim) => {
                    if claim
                        .acquire_lease(&self.rules, &self.lease.entry(), self.local_now(h))
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
            if self.faults.can_crash() {
                actions.push(Action::RenewFails(h));
            }
        }
        if awake && holder.step.hold().is_some() && self.faults.can_stall() {
            actions.push(Action::Stall(h));
        }
        if holder.stalled {
            actions.push(Action::Resume(h));
        }
        if !matches!(holder.step, Step::Idle) && self.faults.can_crash() {
            actions.push(Action::Crash(h));
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Tick => self.clock.advance(),
            Action::Submit => self.submit(),
            Action::Publish => self.publish(),
            Action::PlannerCrash => {
                self.faults.take_crash();
                self.publishing = None;
            }
            Action::Redeliver(c) => self.stream.redeliver(c),
            Action::Deliver(h, c) => self.deliver(h, c),
            Action::AcquireLease(h) => self.acquire_lease(h),
            Action::StartTask(h) => self.start_task(h),
            Action::Renew(h) => self.renew(h),
            Action::RenewFails(h) => {
                self.faults.take_crash();
                self.drop_run(h);
            }
            Action::BeginWrite(h) => self.advance(h),
            Action::CommitWrite(h) => self.commit_write(h),
            Action::Finish(h) => self.finish(h),
            Action::Stall(h) => {
                self.faults.take_stall();
                self.holders[h].stalled = true;
            }
            Action::Resume(h) => self.holders[h].stalled = false,
            Action::Crash(h) => {
                self.faults.take_crash();
                self.drop_run(h);
                self.holders[h].stalled = false;
            }
        }
        self.remember_holds();
    }

    fn submit(&mut self) {
        match decide_submit(self.marker.get().copied(), task_id(self.next_id)) {
            Publish::New(task) => {
                let created = self.marker.put(self.marker.revision(), Some(task));
                assert!(created.is_ok(), "a submit is atomic in the spec");
                self.next_id += 1;
                self.publishing = Some(task);
            }
            Publish::Pending(task) => {
                self.coverage.republished_markers += 1;
                self.publishing = Some(task);
            }
        }
    }

    fn publish(&mut self) {
        let Some(task) = self.publishing.take() else {
            return;
        };
        self.stream.publish(task);
        self.covers.push(task);
    }

    fn deliver(&mut self, h: usize, c: usize) {
        let task = self.stream.deliver(c, h, self.clock.ticks());
        let holder = &mut self.holders[h];
        holder.step = Step::Leasing(Claim::deliver(holder.id.clone(), task));
        holder.held = Some(c);
    }

    fn acquire_lease(&mut self, h: usize) {
        let local_now = self.local_now(h);
        let Step::Leasing(claim) = std::mem::replace(&mut self.holders[h].step, Step::Idle) else {
            return;
        };
        let entry = self.lease.entry();
        let Some(lease) = claim.acquire_lease(&self.rules, &entry, local_now) else {
            return;
        };
        let Ok(written) = self.lease.put(entry.revision, Some(lease.clone())) else {
            return;
        };
        self.holders[h].step = Step::Leased(claim.hold(&self.rules, &lease, written, local_now));
    }

    fn start_task(&mut self, h: usize) {
        let Step::Leased(hold) = std::mem::replace(&mut self.holders[h].step, Step::Idle) else {
            return;
        };
        self.started.insert(hold.task());
        let marker = self.marker.get().copied();
        if hold.start_clears_marker(marker) {
            let cleared = self.marker.put(self.marker.revision(), None);
            assert!(cleared.is_ok(), "a start is atomic in the spec");
        } else if marker.is_some() {
            self.coverage.foreign_markers_kept += 1;
        }
        self.holders[h].step = Step::Running(hold);
    }

    fn renew(&mut self, h: usize) {
        let local_now = self.local_now(h);
        let entry = self.lease.entry();
        let Some(renewal) = self
            .holds(h)
            .map(|hold| hold.renew(&self.rules, &entry, local_now))
        else {
            return;
        };
        match renewal {
            Renewal::Extend(lease) => {
                let Ok(written) = self.lease.put(entry.revision, Some(lease.clone())) else {
                    return;
                };
                if let Some(hold) = self.holders[h].step.hold_mut() {
                    hold.renewed(&self.rules, &lease, written, local_now);
                }
                if let Some(c) = self.holders[h].held {
                    self.stream.acknowledge_progress(c, h, self.clock.ticks());
                }
            }
            Renewal::Lost => {
                self.coverage.lost_renewals += 1;
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
        match self.store.write(fence) {
            StoreWrite::Kept => self.advance(h),
            StoreWrite::Rejected => {
                self.coverage.fence_rejections += 1;
                self.drop_run(h);
            }
        }
    }

    fn finish(&mut self, h: usize) {
        if let Some(c) = self.holders[h].held {
            self.stream.acknowledge(c);
        }
        let release = self
            .holds(h)
            .and_then(|hold| hold.release(&self.lease.entry()));
        if let Some(expected) = release
            && self.lease.put(expected, None).is_ok()
        {
            self.coverage.releases += 1;
        }
        self.drop_run(h);
    }

    fn drop_run(&mut self, h: usize) {
        self.holders[h].step = Step::Idle;
        self.holders[h].held = None;
    }

    fn remember_holds(&mut self) {
        for holder in &mut self.holders {
            if let Some(hold) = holder.step.hold() {
                holder.last_hold = LastHold {
                    token: hold.fence().0,
                    seen: hold.seen_revision().0,
                    expiry: self.clock.ticks_at(hold.expires_at()),
                    renew_at: self.clock.ticks_at(hold.renew_at()),
                };
            }
        }
    }

    fn check_invariants(&self) -> Result<(), String> {
        let marker = self.marker.get().copied();
        let pending: Vec<TaskId> = self
            .stream
            .live_tasks()
            .filter(|task| !self.started.contains(task))
            .collect();
        if pending.iter().any(|task| Some(*task) != marker) {
            return Err(format!(
                "AtMostOnePending: pending {pending:?}, marker {marker:?}"
            ));
        }

        let lost = self.covers.iter().find(|task| {
            !self.started.contains(task)
                && !self.stream.has_live_copy(**task)
                && self.publishing != Some(**task)
        });
        if let Some(task) = lost {
            return Err(format!("EverySubmitCovered: task {task:?} is lost"));
        }

        Ok(())
    }

    fn to_tla(&self) -> String {
        tla::Tla(self).to_string()
    }
}

fn task_id(spec_id: u64) -> TaskId {
    TaskId::new(Uuid::from_u128(spec_id.into()))
}
