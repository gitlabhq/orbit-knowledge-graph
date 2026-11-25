// why: leaseDedupeProtocol bounds time with MaxTime, so a lease or an ack wait could stay open forever
// once time stops. Here faults stop at some point, time jumps past every expiry and every ack wait,
// and engine steps are fair. Then every acked submit must end done, or lost in a wipe and owned by
// the recovery campaign.
module leaseDedupeLiveness {
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

  import leaseDedupeProtocol(
    Pods = Pods,
    NoPod = NoPod,
    Offsets = Offsets,
    Ttl = Ttl,
    RenewEvery = RenewEvery,
    SkewMargin = SkewMargin,
    AckWait = AckWait,
    GiveBackDelay = GiveBackDelay,
    MaxTime = MaxTime,
    MaxEvents = MaxEvents,
    MaxStalls = MaxStalls,
    MaxCrashes = MaxCrashes,
    MaxWipes = MaxWipes,
    MaxDuplicates = MaxDuplicates,
    MaxLosses = MaxLosses,
    StartClearsOnlyOwnPending = StartClearsOnlyOwnPending,
    SubmitRepublishesFoundPending = SubmitRepublishesFoundPending,
  ) as P from "./leaseDedupeProtocol"

  var quiet: bool

  val Horizon = MaxTime + 1 + Ttl + SkewMargin + AckWait

  val allVars = (
    P::now, P::offset, P::lease, P::pending, P::nextId, P::copies, P::planner, P::submitted,
    P::cover, P::started, P::pc, P::held, P::seenRev, P::renewAt, P::stalled, P::stallsLeft,
    P::crashesLeft, P::wipesLeft, P::duplicatesLeft, P::lossesLeft, P::lostInWipe, quiet,
  )

  action PodEngineStep(p: str): bool = any {
    nondet c = oneOf(P::copies.indices()) P::Deliver(p, c),
    P::AcquireLease(p),
    P::GiveBack(p),
    P::StartTask(p),
    P::Renew(p),
    P::Work(p),
    P::Finish(p),
    P::Resume(p),
  }

  action EngineStep = any {
    P::Tick,
    P::Submit,
    P::Publish,
    P::RedeliverAny,
    nondet p = oneOf(Pods) PodEngineStep(p),
  }

  action FaultStep = any {
    P::PlannerCrash,
    P::NatsWipe,
    P::DuplicateAny,
    P::LoseAny,
    nondet p = oneOf(Pods) any { P::RenewFails(p), P::Stall(p), P::Crash(p) },
  }

  action FaultsStop = all {
    not(quiet),
    quiet' = true,
    P::now' = Horizon,
    P::offset' = P::offset, P::lease' = P::lease, P::pending' = P::pending, P::nextId' = P::nextId,
    P::copies' = P::copies, P::planner' = P::planner, P::submitted' = P::submitted,
    P::cover' = P::cover, P::started' = P::started, P::stallsLeft' = P::stallsLeft,
    P::crashesLeft' = P::crashesLeft,
    P::pc' = P::pc, P::held' = P::held, P::seenRev' = P::seenRev,
    P::renewAt' = P::renewAt, P::stalled' = P::stalled, P::wipesLeft' = P::wipesLeft,
    P::duplicatesLeft' = P::duplicatesLeft, P::lossesLeft' = P::lossesLeft,
    P::lostInWipe' = P::lostInWipe,
  }

  action init = all { P::init, quiet' = false }

  action step = any {
    all { EngineStep, quiet' = quiet },
    all { not(quiet), FaultStep, quiet' = quiet },
    FaultsStop,
  }

  temporal Fairness = and {
    weakFair(FaultsStop, allVars),
    weakFair(all { P::Publish, quiet' = quiet }, allVars),
    weakFair(all { P::RedeliverAny, quiet' = quiet }, allVars),
    Pods.forall(p => and {
      weakFair(all { nondet c = oneOf(P::copies.indices()) P::Deliver(p, c), quiet' = quiet }, allVars),
      weakFair(all { P::AcquireLease(p), quiet' = quiet }, allVars),
      weakFair(all { P::GiveBack(p), quiet' = quiet }, allVars),
      weakFair(all { P::StartTask(p), quiet' = quiet }, allVars),
      weakFair(all { P::Renew(p), quiet' = quiet }, allVars),
      weakFair(all { P::Work(p), quiet' = quiet }, allVars),
      weakFair(all { P::Finish(p), quiet' = quiet }, allVars),
      weakFair(all { P::Resume(p), quiet' = quiet }, allVars),
    }),
  }

  def DoneOrOwnedByRecovery(id: int): bool = P::lostInWipe.contains(id) or P::IsDone(id)

  temporal EverySubmitIsDone =
    1.to(MaxEvents).forall(e => always(e <= P::submitted implies eventually(DoneOrOwnedByRecovery(P::cover.get(e)))))

  temporal PodsEndIdle = eventually(always(Pods.forall(p => P::pc.get(p) == "idle")))
}
