use std::collections::BTreeSet;
use std::fmt::{self, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use indexing_engine_next::protocol::{
    Claim, Hold, Lease, LeaseEntry, LeaseRules, PodId, Publish, Renewal, Revision, TaskId,
    decide_submit,
};
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};
use uuid::Uuid;

const UNIT: Duration = Duration::from_secs(20);
const PODS: usize = 3;
const OFFSETS: [u64; 2] = [0, 1];
const TTL: u32 = 3;
const SKEW_MARGIN: u32 = 1;
const ACK_WAIT: u64 = 2;
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

struct Pod {
    id: PodId,
    offset: u64,
    stalled: bool,
    step: Step,
    held: Option<usize>,
    last_hold: LastHold,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CopyState {
    Ready,
    Inflight,
    Acked,
}

struct Copy {
    task: TaskId,
    state: CopyState,
    deadline: u64,
    pod: Option<usize>,
}

struct World {
    rules: LeaseRules,
    now: u64,
    pods: Vec<Pod>,
    lease: LeaseEntry,
    marker: Option<TaskId>,
    next_id: u64,
    copies: Vec<Copy>,
    publishing: Option<TaskId>,
    covers: Vec<TaskId>,
    started: BTreeSet<TaskId>,
    stalls_left: u32,
    crashes_left: u32,
    store_fence: u64,
    coverage: Coverage,
}

impl World {
    fn init(rng: &mut Xoshiro256PlusPlus) -> Self {
        let pods = (1..=PODS)
            .map(|n| Pod {
                id: PodId::new(format!("p{n}")),
                offset: OFFSETS[rng.random_range(0..OFFSETS.len())],
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
            now: 0,
            pods,
            lease: LeaseEntry {
                lease: None,
                revision: Revision(0),
            },
            marker: None,
            next_id: 1,
            copies: Vec::new(),
            publishing: None,
            covers: Vec::new(),
            started: BTreeSet::new(),
            stalls_left: MAX_STALLS,
            crashes_left: MAX_CRASHES,
            store_fence: 0,
            coverage: Coverage::default(),
        }
    }

    fn local_now(&self, pod: usize) -> SystemTime {
        at(self.now + self.pods[pod].offset)
    }

    fn is_awake(&self, pod: usize) -> bool {
        !self.pods[pod].stalled
    }

    fn holds(&self, pod: usize) -> Option<&Hold> {
        self.pods[pod].step.hold()
    }

    fn is_renew_due(&self, pod: usize) -> bool {
        self.holds(pod)
            .is_some_and(|hold| hold.is_renew_due(self.local_now(pod)))
    }

    fn has_live_copy(&self, task: TaskId) -> bool {
        self.copies
            .iter()
            .any(|copy| copy.task == task && copy.state != CopyState::Acked)
    }

    fn enabled_actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        let renewals_on_time = (0..PODS).all(|p| !self.is_awake(p) || !self.is_renew_due(p));

        if self.now < MAX_TIME && renewals_on_time {
            actions.push(Action::Tick);
        }
        if self.publishing.is_none() && self.covers.len() < MAX_EVENTS {
            actions.push(Action::Submit);
        }
        if self.publishing.is_some() {
            actions.push(Action::Publish);
            if self.crashes_left > 0 {
                actions.push(Action::PlannerCrash);
            }
        }
        for (c, copy) in self.copies.iter().enumerate() {
            if copy.state == CopyState::Inflight && self.now >= copy.deadline {
                actions.push(Action::Redeliver(c));
            }
        }
        for p in 0..PODS {
            self.push_pod_actions(p, &mut actions);
        }

        actions
    }

    fn push_pod_actions(&self, p: usize, actions: &mut Vec<Action>) {
        let pod = &self.pods[p];
        let awake = !pod.stalled;

        if awake {
            match &pod.step {
                Step::Idle => {
                    for (c, copy) in self.copies.iter().enumerate() {
                        if copy.state == CopyState::Ready {
                            actions.push(Action::Deliver(p, c));
                        }
                    }
                }
                Step::Leasing(claim) => {
                    if claim
                        .acquire_lease(&self.rules, &self.lease, self.local_now(p))
                        .is_some()
                    {
                        actions.push(Action::AcquireLease(p));
                    }
                }
                Step::Leased(_) => actions.push(Action::StartTask(p)),
                Step::Running(_) => actions.push(Action::BeginWrite(p)),
                Step::Writing(_) => actions.push(Action::CommitWrite(p)),
                Step::Done(_) => actions.push(Action::Finish(p)),
            }
        }
        if awake && self.is_renew_due(p) {
            actions.push(Action::Renew(p));
            if self.crashes_left > 0 {
                actions.push(Action::RenewFails(p));
            }
        }
        if awake && pod.step.hold().is_some() && self.stalls_left > 0 {
            actions.push(Action::Stall(p));
        }
        if pod.stalled {
            actions.push(Action::Resume(p));
        }
        if !matches!(pod.step, Step::Idle) && self.crashes_left > 0 {
            actions.push(Action::Crash(p));
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Tick => self.now += 1,
            Action::Submit => self.submit(),
            Action::Publish => self.publish(),
            Action::PlannerCrash => {
                self.crashes_left -= 1;
                self.publishing = None;
            }
            Action::Redeliver(c) => {
                self.copies[c].state = CopyState::Ready;
                self.copies[c].pod = None;
            }
            Action::Deliver(p, c) => self.deliver(p, c),
            Action::AcquireLease(p) => self.acquire_lease(p),
            Action::StartTask(p) => self.start_task(p),
            Action::Renew(p) => self.renew(p),
            Action::RenewFails(p) => {
                self.crashes_left -= 1;
                self.drop_run(p);
            }
            Action::BeginWrite(p) => self.advance(p),
            Action::CommitWrite(p) => self.commit_write(p),
            Action::Finish(p) => self.finish(p),
            Action::Stall(p) => {
                self.stalls_left -= 1;
                self.pods[p].stalled = true;
            }
            Action::Resume(p) => self.pods[p].stalled = false,
            Action::Crash(p) => {
                self.crashes_left -= 1;
                self.drop_run(p);
                self.pods[p].stalled = false;
            }
        }
        self.remember_holds();
    }

    fn submit(&mut self) {
        match decide_submit(self.marker, task_id(self.next_id)) {
            Publish::New(task) => {
                self.marker = Some(task);
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
        if !self.has_live_copy(task) {
            self.copies.push(Copy {
                task,
                state: CopyState::Ready,
                deadline: 0,
                pod: None,
            });
        }
        self.covers.push(task);
    }

    fn deliver(&mut self, p: usize, c: usize) {
        let copy = &mut self.copies[c];
        copy.state = CopyState::Inflight;
        copy.pod = Some(p);
        copy.deadline = self.now + ACK_WAIT;

        let pod = &mut self.pods[p];
        pod.step = Step::Leasing(Claim::deliver(pod.id.clone(), copy.task));
        pod.held = Some(c);
    }

    fn acquire_lease(&mut self, p: usize) {
        let local_now = self.local_now(p);
        let Step::Leasing(claim) = std::mem::replace(&mut self.pods[p].step, Step::Idle) else {
            return;
        };
        let Some(lease) = claim.acquire_lease(&self.rules, &self.lease, local_now) else {
            return;
        };
        let written = self.write_lease(Some(lease.clone()));
        self.pods[p].step = Step::Leased(claim.hold(&self.rules, &lease, written, local_now));
    }

    fn start_task(&mut self, p: usize) {
        let Step::Leased(hold) = std::mem::replace(&mut self.pods[p].step, Step::Idle) else {
            return;
        };
        self.started.insert(hold.task());
        if hold.start_clears_marker(self.marker) {
            self.marker = None;
        } else if self.marker.is_some() {
            self.coverage.foreign_markers_kept += 1;
        }
        self.pods[p].step = Step::Running(hold);
    }

    fn renew(&mut self, p: usize) {
        let local_now = self.local_now(p);
        let Some(renewal) = self
            .holds(p)
            .map(|hold| hold.renew(&self.rules, &self.lease, local_now))
        else {
            return;
        };
        match renewal {
            Renewal::Extend(lease) => {
                let written = self.write_lease(Some(lease.clone()));
                if let Some(hold) = hold_mut(&mut self.pods[p].step) {
                    hold.renewed(&self.rules, &lease, written, local_now);
                }
                self.extend_ack_wait(p);
            }
            Renewal::Lost => {
                self.coverage.lost_renewals += 1;
                self.drop_run(p);
            }
        }
    }

    fn extend_ack_wait(&mut self, p: usize) {
        let Some(c) = self.pods[p].held else {
            return;
        };
        let copy = &mut self.copies[c];
        if copy.state == CopyState::Inflight && copy.pod == Some(p) {
            copy.deadline = self.now + ACK_WAIT;
        }
    }

    fn advance(&mut self, p: usize) {
        let step = std::mem::replace(&mut self.pods[p].step, Step::Idle);
        self.pods[p].step = match step {
            Step::Running(hold) => Step::Writing(hold),
            Step::Writing(hold) => Step::Done(hold),
            other => other,
        };
    }

    fn commit_write(&mut self, p: usize) {
        let Some(fence) = self.holds(p).map(|hold| hold.fence().0) else {
            return;
        };
        if fence < self.store_fence {
            self.coverage.fence_rejections += 1;
            self.drop_run(p);
        } else {
            self.store_fence = self.store_fence.max(fence);
            self.advance(p);
        }
    }

    fn finish(&mut self, p: usize) {
        if let Some(c) = self.pods[p].held {
            self.copies[c].state = CopyState::Acked;
        }
        let release = self.holds(p).and_then(|hold| hold.release(&self.lease));
        if let Some(expected) = release
            && expected == self.lease.revision
        {
            self.coverage.releases += 1;
            self.write_lease(None);
        }
        self.drop_run(p);
    }

    fn write_lease(&mut self, lease: Option<Lease>) -> Revision {
        let written = Revision(self.lease.revision.0 + 1);
        self.lease = LeaseEntry {
            lease,
            revision: written,
        };
        written
    }

    fn drop_run(&mut self, p: usize) {
        self.pods[p].step = Step::Idle;
        self.pods[p].held = None;
    }

    fn remember_holds(&mut self) {
        for pod in &mut self.pods {
            if let Some(hold) = pod.step.hold() {
                pod.last_hold = LastHold {
                    token: hold.fence().0,
                    seen: hold.seen_revision().0,
                    expiry: units(hold.expires_at()),
                    renew_at: units(hold.renew_at()),
                };
            }
        }
    }

    fn check_invariants(&self) -> Result<(), String> {
        let pending: Vec<TaskId> = self
            .live_tasks()
            .filter(|task| !self.started.contains(task))
            .collect();
        if pending.iter().any(|task| Some(*task) != self.marker) {
            return Err(format!(
                "AtMostOnePending: pending {pending:?}, marker {:?}",
                self.marker
            ));
        }

        let lost = self.covers.iter().find(|task| {
            !self.started.contains(task)
                && !self.has_live_copy(**task)
                && self.publishing != Some(**task)
        });
        if let Some(task) = lost {
            return Err(format!("EverySubmitCovered: task {task:?} is lost"));
        }

        Ok(())
    }

    fn live_tasks(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.copies
            .iter()
            .filter(|copy| copy.state != CopyState::Acked)
            .map(|copy| copy.task)
    }

    fn to_tla(&self) -> String {
        Tla(self).to_string()
    }
}

fn hold_mut(step: &mut Step) -> Option<&mut Hold> {
    match step {
        Step::Leased(hold) | Step::Running(hold) | Step::Writing(hold) | Step::Done(hold) => {
            Some(hold)
        }
        Step::Idle | Step::Leasing(_) => None,
    }
}

fn at(units: u64) -> SystemTime {
    UNIX_EPOCH + UNIT * u32::try_from(units).expect("a test time fits in u32 units")
}

fn units(time: SystemTime) -> u64 {
    let since_epoch = time
        .duration_since(UNIX_EPOCH)
        .expect("a test time is after the epoch");
    since_epoch.as_secs() / UNIT.as_secs()
}

fn task_id(spec_id: u64) -> TaskId {
    TaskId::new(Uuid::from_u128(spec_id.into()))
}

fn spec_id(task: TaskId) -> u128 {
    task.as_uuid().as_u128()
}

struct Tla<'a>(&'a World);

impl Tla<'_> {
    fn per_pod(&self, f: &mut fmt::Formatter<'_>, value: impl Fn(&Pod) -> String) -> fmt::Result {
        let entries: Vec<String> = self
            .0
            .pods
            .iter()
            .map(|pod| format!("{} :> {}", pod.id.as_str(), value(pod)))
            .collect();
        write!(f, "({})", entries.join(" @@ "))
    }
}

impl fmt::Display for Tla<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let world = self.0;
        let pod_name = |p: Option<usize>| p.map_or("NoPod".to_string(), |p| format!("p{}", p + 1));
        let marker_id = |task: Option<TaskId>| task.map_or(0, spec_id);

        write!(f, "[now |-> {}, offset |-> ", world.now)?;
        self.per_pod(f, |pod| pod.offset.to_string())?;

        let (holder, expires_at) = match &world.lease.lease {
            Some(lease) => (lease.holder.as_str().to_string(), units(lease.expires_at)),
            None => ("NoPod".to_string(), 0),
        };
        write!(
            f,
            ", lease |-> [holder |-> {holder}, expiresAt |-> {expires_at}, rev |-> {}]",
            world.lease.revision.0
        )?;
        write!(
            f,
            ", marker |-> {}, nextId |-> {}",
            marker_id(world.marker),
            world.next_id
        )?;

        let copies: Vec<String> = world
            .copies
            .iter()
            .map(|copy| {
                let state = match copy.state {
                    CopyState::Ready => "ready",
                    CopyState::Inflight => "inflight",
                    CopyState::Acked => "acked",
                };
                format!(
                    "[id |-> {}, state |-> \"{state}\", deadline |-> {}, pod |-> {}]",
                    spec_id(copy.task),
                    copy.deadline,
                    pod_name(copy.pod)
                )
            })
            .collect();
        write!(f, ", copies |-> <<{}>>", copies.join(", "))?;

        let planner = match world.publishing {
            Some(task) => format!("[phase |-> \"publishing\", id |-> {}]", spec_id(task)),
            None => "[phase |-> \"idle\", id |-> 0]".to_string(),
        };
        let mut cover: Vec<String> = world
            .covers
            .iter()
            .map(|t| spec_id(*t).to_string())
            .collect();
        cover.resize(MAX_EVENTS, "0".to_string());
        let started: Vec<String> = world
            .started
            .iter()
            .map(|t| spec_id(*t).to_string())
            .collect();
        write!(
            f,
            ", planner |-> {planner}, submitted |-> {}, cover |-> <<{}>>, started |-> {{{}}}",
            world.covers.len(),
            cover.join(", "),
            started.join(", ")
        )?;

        f.write_str(", pc |-> ")?;
        self.per_pod(f, |pod| format!("\"{}\"", pod.step.name()))?;
        f.write_str(", held |-> ")?;
        self.per_pod(f, |pod| pod.held.map_or(0, |c| c + 1).to_string())?;
        f.write_str(", token |-> ")?;
        self.per_pod(f, |pod| pod.last_hold.token.to_string())?;
        f.write_str(", seenRev |-> ")?;
        self.per_pod(f, |pod| pod.last_hold.seen.to_string())?;
        f.write_str(", expiry |-> ")?;
        self.per_pod(f, |pod| pod.last_hold.expiry.to_string())?;
        f.write_str(", renewAt |-> ")?;
        self.per_pod(f, |pod| pod.last_hold.renew_at.to_string())?;
        f.write_str(", stalled |-> ")?;
        self.per_pod(f, |pod| tla_bool(pod.stalled).to_string())?;

        write!(
            f,
            ", stallsLeft |-> {}, crashesLeft |-> {}, storeFence |-> {}, staleWrite |-> FALSE]",
            world.stalls_left, world.crashes_left, world.store_fence
        )
    }
}

fn tla_bool(value: bool) -> &'static str {
    if value { "TRUE" } else { "FALSE" }
}

pub fn write_runs_module(runs: &[Walk]) -> String {
    let mut module = String::new();
    module.push_str(
        "---------------------------- MODULE LeaseDedupeRuns ----------------------------\n",
    );
    module.push_str("EXTENDS LeaseDedupe\n\nCONSTANTS p1, p2, p3\n\nRuns == <<\n");
    for (n, run) in runs.iter().enumerate() {
        let separator = if n + 1 == runs.len() { "" } else { "," };
        let _ = writeln!(module, "<<\n{}\n>>{separator}", run.states.join(",\n"));
    }
    module.push_str(
        ">>\n=============================================================================\n",
    );
    module
}
