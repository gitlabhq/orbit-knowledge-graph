// ADR 023 lease and pending-dedupe protocol: one pending task per key, and every acked submit runs.
// why: NATS is the broker and the key-value store. It can wipe all its state, deliver a message
// twice, or lose one. Workers write rows versioned by their source, so a rare overlap of two runs
// of one key is harmless.
module leaseDedupeProtocol {
  const Pods: Set[str]
  const NoPod: str
  const Offsets: Set[int]
  const Ttl: int
  const RenewEvery: int
  const SkewMargin: int
  const AckWait: int
  const GiveBackDelay: int
  const MaxTime: int
  const MaxEvents: int
  const MaxStalls: int
  const MaxCrashes: int
  const MaxWipes: int
  const MaxDuplicates: int
  const MaxLosses: int
  const StartClearsOnlyOwnPending: bool
  const SubmitRepublishesFoundPending: bool

  type Lease = { holder: str, expiresAt: int, rev: int }
  type Copy = { id: int, state: str, deadline: int, pod: str }
  type Planner = { phase: str, id: int }

  pure val NoId = 0
  pure val MaxIds = MaxEvents + MaxCrashes
  pure val Ids = 1.to(MaxIds)
  pure val HoldingPhases = Set("leased", "running", "done")
  pure val RunPhases = Set("running", "done")
  pure val LiveStates = Set("ready", "inflight")

  var now: int
  var offset: str -> int
  var lease: Lease
  var pending: int
  var nextId: int
  var copies: List[Copy]
  var planner: Planner
  var submitted: int
  var cover: int -> int
  var started: Set[int]
  var pc: str -> str
  var held: str -> int
  var seenRev: str -> int
  var renewAt: str -> int
  var stalled: str -> bool
  var stallsLeft: int
  var crashesLeft: int
  var wipesLeft: int
  var duplicatesLeft: int
  var lossesLeft: int
  var lostInWipe: Set[int]

  pure def LoseCopy(cp: Copy): Copy =
    if (LiveStates.contains(cp.state)) cp.with("state", "lost") else cp

  def Local(p: str): int = now + offset.get(p)

  def HasLiveCopy(id: int): bool =
    copies.indices().exists(c => copies[c].id == id and LiveStates.contains(copies[c].state))

  def IsDone(id: int): bool =
    copies.indices().exists(c => copies[c].id == id and copies[c].state == "acked")

  def LeaseAcquirable(p: str): bool =
    lease.holder == NoPod or Local(p) >= lease.expiresAt + SkewMargin

  def HoldsCurrentLease(p: str): bool =
    lease.holder == p and lease.rev == seenRev.get(p)

  def Awake(p: str): bool = not(stalled.get(p))

  def heldIndex(p: str): int = held.get(p) - 1

  action DropRun(p: str): bool = all {
    pc' = pc.set(p, "idle"),
    held' = held.set(p, 0),
  }

  action unchangedPodVars = all {
    pc' = pc, held' = held, seenRev' = seenRev,
    renewAt' = renewAt, stalled' = stalled,
  }

  action unchangedPlannerVars = all {
    pending' = pending, nextId' = nextId, planner' = planner,
    submitted' = submitted, cover' = cover,
  }

  action unchangedNatsVars = all {
    wipesLeft' = wipesLeft,
    duplicatesLeft' = duplicatesLeft, lossesLeft' = lossesLeft, lostInWipe' = lostInWipe,
  }

  action init = nondet offsets = oneOf(Pods.setOfMaps(Offsets)) initWith(offsets)

  action initWith(offsets: str -> int): bool = all {
    now' = 0,
    offset' = offsets,
    lease' = { holder: NoPod, expiresAt: 0, rev: 0 },
    pending' = NoId,
    nextId' = 1,
    copies' = [],
    planner' = { phase: "idle", id: NoId },
    submitted' = 0,
    cover' = 1.to(MaxEvents).mapBy(e => NoId),
    started' = Set(),
    pc' = Pods.mapBy(p => "idle"),
    held' = Pods.mapBy(p => 0),
    seenRev' = Pods.mapBy(p => 0),
    renewAt' = Pods.mapBy(p => 0),
    stalled' = Pods.mapBy(p => false),
    stallsLeft' = MaxStalls,
    crashesLeft' = MaxCrashes,
    wipesLeft' = MaxWipes,
    duplicatesLeft' = MaxDuplicates,
    lossesLeft' = MaxLosses,
    lostInWipe' = Set(),
  }

  action Tick = all {
    now < MaxTime,
    Pods.forall(p => (HoldingPhases.contains(pc.get(p)) and Awake(p)) implies Local(p) < renewAt.get(p)),
    now' = now + 1,
    offset' = offset, lease' = lease, copies' = copies, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    unchangedPodVars, unchangedPlannerVars, unchangedNatsVars,
  }

  action Submit = all {
    planner.phase == "idle",
    submitted < MaxEvents,
    if (pending == NoId) all {
      pending' = nextId,
      nextId' = nextId + 1,
      planner' = { phase: "publishing", id: nextId },
      submitted' = submitted, cover' = cover,
    } else if (SubmitRepublishesFoundPending) all {
      planner' = { phase: "publishing", id: pending },
      pending' = pending, nextId' = nextId, submitted' = submitted, cover' = cover,
    } else all {
      submitted' = submitted + 1,
      cover' = cover.set(submitted + 1, pending),
      pending' = pending, nextId' = nextId, planner' = planner,
    },
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    unchangedPodVars, unchangedNatsVars,
  }

  action Publish = all {
    planner.phase == "publishing",
    copies' = if (HasLiveCopy(planner.id)) copies
              else copies.append({ id: planner.id, state: "ready", deadline: 0, pod: NoPod }),
    submitted' = submitted + 1,
    cover' = cover.set(submitted + 1, planner.id),
    planner' = { phase: "idle", id: NoId },
    now' = now, offset' = offset, lease' = lease, pending' = pending, nextId' = nextId,
    started' = started, stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    unchangedPodVars, unchangedNatsVars,
  }

  action PlannerCrash = all {
    planner.phase == "publishing",
    crashesLeft > 0,
    crashesLeft' = crashesLeft - 1,
    planner' = { phase: "idle", id: NoId },
    now' = now, offset' = offset, lease' = lease, pending' = pending, nextId' = nextId,
    copies' = copies, submitted' = submitted, cover' = cover, started' = started, stallsLeft' = stallsLeft,
    unchangedPodVars, unchangedNatsVars,
  }

  action Deliver(p: str, c: int): bool = all {
    pc.get(p) == "idle",
    Awake(p),
    copies[c].state == "ready",
    copies' = copies.replaceAt(c, copies[c].with("state", "inflight").with("pod", p).with("deadline", now + AckWait)),
    pc' = pc.set(p, "leasing"),
    held' = held.set(p, c + 1),
    now' = now, offset' = offset, lease' = lease, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Redeliver(c: int): bool = all {
    copies[c].state == "inflight",
    now >= copies[c].deadline,
    copies' = copies.replaceAt(c, copies[c].with("state", "ready").with("pod", NoPod)),
    now' = now, offset' = offset, lease' = lease, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    unchangedPodVars, unchangedPlannerVars, unchangedNatsVars,
  }

  action AcquireLease(p: str): bool = all {
    pc.get(p) == "leasing",
    Awake(p),
    LeaseAcquirable(p),
    lease' = { holder: p, expiresAt: Local(p) + Ttl, rev: lease.rev + 1 },
    seenRev' = seenRev.set(p, lease.rev + 1),
    renewAt' = renewAt.set(p, Local(p) + RenewEvery),
    pc' = pc.set(p, "leased"),
    now' = now, offset' = offset, copies' = copies, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    held' = held, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  // why: a pod that finds the lease of its key held gives the message back to the broker with a
  // delay, so it frees its slot and doesn't wait for the lease.
  action GiveBack(p: str): bool = all {
    pc.get(p) == "leasing",
    Awake(p),
    not(LeaseAcquirable(p)),
    copies' = if (copies[heldIndex(p)].state == "inflight")
                copies.replaceAt(heldIndex(p), copies[heldIndex(p)].with("pod", NoPod).with("deadline", now + GiveBackDelay))
              else copies,
    DropRun(p),
    now' = now, offset' = offset, lease' = lease, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action StartTask(p: str): bool = all {
    pc.get(p) == "leased",
    Awake(p),
    started' = started.union(Set(copies[heldIndex(p)].id)),
    pending' = if (StartClearsOnlyOwnPending and pending != copies[heldIndex(p)].id) pending else NoId,
    pc' = pc.set(p, "running"),
    now' = now, offset' = offset, lease' = lease, nextId' = nextId, copies' = copies,
    planner' = planner, submitted' = submitted, cover' = cover,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    held' = held, seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedNatsVars,
  }

  action Renew(p: str): bool = all {
    HoldingPhases.contains(pc.get(p)),
    Awake(p),
    Local(p) >= renewAt.get(p),
    if (HoldsCurrentLease(p)) all {
      lease' = { holder: p, expiresAt: Local(p) + Ttl, rev: lease.rev + 1 },
      seenRev' = seenRev.set(p, lease.rev + 1),
      renewAt' = renewAt.set(p, Local(p) + RenewEvery),
      copies' = if (copies[heldIndex(p)].state == "inflight" and copies[heldIndex(p)].pod == p)
                  copies.replaceAt(heldIndex(p), copies[heldIndex(p)].with("deadline", now + AckWait))
                else copies,
      pc' = pc, held' = held,
    } else all {
      DropRun(p),
      lease' = lease, seenRev' = seenRev, renewAt' = renewAt, copies' = copies,
    },
    now' = now, offset' = offset, started' = started, stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action RenewFails(p: str): bool = all {
    HoldingPhases.contains(pc.get(p)),
    Awake(p),
    Local(p) >= renewAt.get(p),
    crashesLeft > 0,
    crashesLeft' = crashesLeft - 1,
    DropRun(p),
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started,
    stallsLeft' = stallsLeft,
    seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Work(p: str): bool = all {
    pc.get(p) == "running",
    Awake(p),
    pc' = pc.set(p, "done"),
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    held' = held, seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Finish(p: str): bool = all {
    pc.get(p) == "done",
    Awake(p),
    copies' = if (copies[heldIndex(p)].state == "lost") copies
              else copies.replaceAt(heldIndex(p), copies[heldIndex(p)].with("state", "acked")),
    lease' = if (HoldsCurrentLease(p)) { holder: NoPod, expiresAt: 0, rev: lease.rev + 1 } else lease,
    DropRun(p),
    now' = now, offset' = offset, started' = started, stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    seenRev' = seenRev, renewAt' = renewAt, stalled' = stalled,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Stall(p: str): bool = all {
    HoldingPhases.contains(pc.get(p)),
    Awake(p),
    stallsLeft > 0,
    stallsLeft' = stallsLeft - 1,
    stalled' = stalled.set(p, true),
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started, crashesLeft' = crashesLeft,
    pc' = pc, held' = held, seenRev' = seenRev, renewAt' = renewAt,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Resume(p: str): bool = all {
    stalled.get(p),
    stalled' = stalled.set(p, false),
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    pc' = pc, held' = held, seenRev' = seenRev, renewAt' = renewAt,
    unchangedPlannerVars, unchangedNatsVars,
  }

  action Crash(p: str): bool = all {
    pc.get(p) != "idle",
    crashesLeft > 0,
    crashesLeft' = crashesLeft - 1,
    DropRun(p),
    stalled' = stalled.set(p, false),
    now' = now, offset' = offset, lease' = lease, copies' = copies, started' = started, stallsLeft' = stallsLeft,
    seenRev' = seenRev, renewAt' = renewAt,
    unchangedPlannerVars, unchangedNatsVars,
  }

  // why: a wipe loses the message of every task that isn't done, started or not. The recovery
  // campaign of ADR 023 replans those keys.
  action NatsWipe = all {
    wipesLeft > 0,
    wipesLeft' = wipesLeft - 1,
    lease' = { holder: NoPod, expiresAt: 0, rev: 0 },
    pending' = NoId,
    copies' = copies.foldl([], (acc, cp) => acc.append(LoseCopy(cp))),
    lostInWipe' = lostInWipe.union(Ids.filter(id => id < nextId and not(IsDone(id)))),
    now' = now, offset' = offset, nextId' = nextId, planner' = planner, submitted' = submitted,
    cover' = cover, started' = started, stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    duplicatesLeft' = duplicatesLeft, lossesLeft' = lossesLeft,
    unchangedPodVars,
  }

  action DuplicateCopy(c: int): bool = all {
    duplicatesLeft > 0,
    copies[c].state != "lost",
    duplicatesLeft' = duplicatesLeft - 1,
    copies' = copies.append({ id: copies[c].id, state: "ready", deadline: 0, pod: NoPod }),
    now' = now, offset' = offset, lease' = lease, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    wipesLeft' = wipesLeft, lossesLeft' = lossesLeft, lostInWipe' = lostInWipe,
    unchangedPodVars, unchangedPlannerVars,
  }

  action LoseMessage(c: int): bool = all {
    lossesLeft > 0,
    LiveStates.contains(copies[c].state),
    lossesLeft' = lossesLeft - 1,
    copies' = copies.replaceAt(c, copies[c].with("state", "lost")),
    now' = now, offset' = offset, lease' = lease, started' = started,
    stallsLeft' = stallsLeft, crashesLeft' = crashesLeft,
    wipesLeft' = wipesLeft, duplicatesLeft' = duplicatesLeft, lostInWipe' = lostInWipe,
    unchangedPodVars, unchangedPlannerVars,
  }

  action RedeliverAny = nondet c = oneOf(copies.indices()) Redeliver(c)
  action DuplicateAny = nondet c = oneOf(copies.indices()) DuplicateCopy(c)
  action LoseAny = nondet c = oneOf(copies.indices()) LoseMessage(c)

  action PodStep(p: str): bool = any {
    nondet c = oneOf(copies.indices()) Deliver(p, c),
    AcquireLease(p),
    GiveBack(p),
    StartTask(p),
    Renew(p),
    RenewFails(p),
    Work(p),
    Finish(p),
    Stall(p),
    Resume(p),
    Crash(p),
  }

  action step = any {
    Tick,
    Submit,
    Publish,
    PlannerCrash,
    NatsWipe,
    RedeliverAny,
    DuplicateAny,
    LoseAny,
    nondet p = oneOf(Pods) PodStep(p),
  }

  val vars = (
    now, offset, lease, pending, nextId, copies, planner, submitted, cover, started, pc, held,
    seenRev, renewAt, stalled, stallsLeft, crashesLeft, wipesLeft, duplicatesLeft, lossesLeft,
    lostInWipe,
  )

  val TypeOK = and {
    now >= 0 and now <= MaxTime,
    Pods.union(Set(NoPod)).contains(lease.holder),
    pending == NoId or Ids.contains(pending),
    started.subseteq(Ids),
    Pods.forall(p => Set("idle", "leasing").union(HoldingPhases).contains(pc.get(p))),
  }

  val NoOverlap = Pods.filter(p => RunPhases.contains(pc.get(p))).size() <= 1

  // why: after a wipe, the recovery campaign of ADR 023 owns every task from before the wipe. A
  // submit in flight during the wipe can leave one extra pending task, and the key runs once more.
  val PendingIds =
    Ids.filter(id => not(started.contains(id)) and not(lostInWipe.contains(id)) and HasLiveCopy(id))

  val AtMostOnePending = PendingIds.subseteq(Set(pending))

  def Covered(id: int): bool = or {
    started.contains(id),
    HasLiveCopy(id),
    planner == { phase: "publishing", id: id },
    lostInWipe.contains(id),
  }

  val EverySubmitCovered =
    1.to(MaxEvents).filter(e => e <= submitted).forall(e => Covered(cover.get(e)))
}
