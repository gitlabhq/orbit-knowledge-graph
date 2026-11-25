---------------------------- MODULE LeaseDedupe ----------------------------
\* ADR 023 lease and pending-dedupe protocol: one pending task per key, and every acked submit runs.
\* why: NATS is the broker and the key-value store. It can wipe all its state, deliver a message twice, or lose one. Workers write rows versioned by their source, so a rare overlap of two runs of one key is harmless.
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Pods,
    NoPod,
    Offsets,
    Ttl,
    RenewEvery,
    SkewMargin,
    AckWait,
    MaxTime,
    MaxEvents,
    MaxStalls,
    MaxCrashes,
    MaxWipes,
    MaxDuplicates,
    MaxLosses,
    StartClearsOnlyOwnMarker,
    SubmitRepublishesFoundMarker

VARIABLES
    now,
    offset,
    lease,
    marker,
    nextId,
    copies,
    planner,
    submitted,
    cover,
    started,
    pc,
    held,
    seenRev,
    renewAt,
    stalled,
    stallsLeft,
    crashesLeft,
    wipesLeft,
    duplicatesLeft,
    lossesLeft,
    lostInWipe

vars == <<now, offset, lease, marker, nextId, copies, planner, submitted, cover,
          started, pc, held, seenRev, renewAt, stalled, stallsLeft, crashesLeft,
          wipesLeft, duplicatesLeft, lossesLeft, lostInWipe>>

podVars == <<pc, held, seenRev, renewAt, stalled>>
plannerVars == <<marker, nextId, planner, submitted, cover>>
natsVars == <<wipesLeft, duplicatesLeft, lossesLeft, lostInWipe>>

NoMarker == 0
MaxIds == MaxEvents + MaxCrashes
Ids == 1..MaxIds
HoldingPhases == {"leased", "running", "done"}
RunPhases == {"running", "done"}
LiveStates == {"ready", "inflight"}

Local(p) == now + offset[p]

HasLiveCopy(id) == \E c \in DOMAIN copies : copies[c].id = id /\ copies[c].state \in LiveStates

LeaseAcquirable(p) ==
    \/ lease.holder = NoPod
    \/ Local(p) >= lease.expiresAt + SkewMargin

HoldsCurrentLease(p) == lease.holder = p /\ lease.rev = seenRev[p]

Awake(p) == ~stalled[p]

DropRun(p) ==
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ held' = [held EXCEPT ![p] = 0]

PodSymmetry == Permutations(Pods)

Init ==
    /\ now = 0
    /\ offset \in [Pods -> Offsets]
    /\ lease = [holder |-> NoPod, expiresAt |-> 0, rev |-> 0]
    /\ marker = NoMarker
    /\ nextId = 1
    /\ copies = <<>>
    /\ planner = [phase |-> "idle", id |-> NoMarker]
    /\ submitted = 0
    /\ cover = [e \in 1..MaxEvents |-> NoMarker]
    /\ started = {}
    /\ pc = [p \in Pods |-> "idle"]
    /\ held = [p \in Pods |-> 0]
    /\ seenRev = [p \in Pods |-> 0]
    /\ renewAt = [p \in Pods |-> 0]
    /\ stalled = [p \in Pods |-> FALSE]
    /\ stallsLeft = MaxStalls
    /\ crashesLeft = MaxCrashes
    /\ wipesLeft = MaxWipes
    /\ duplicatesLeft = MaxDuplicates
    /\ lossesLeft = MaxLosses
    /\ lostInWipe = {}

Tick ==
    /\ now < MaxTime
    /\ \A p \in Pods : (pc[p] \in HoldingPhases /\ Awake(p)) => Local(p) < renewAt[p]
    /\ now' = now + 1
    /\ UNCHANGED <<offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<podVars, plannerVars>>
    /\ UNCHANGED natsVars

Submit ==
    /\ planner.phase = "idle"
    /\ submitted < MaxEvents
    /\ IF marker = NoMarker
         THEN /\ marker' = nextId
              /\ nextId' = nextId + 1
              /\ planner' = [phase |-> "publishing", id |-> nextId]
              /\ UNCHANGED <<submitted, cover>>
         ELSE IF SubmitRepublishesFoundMarker
           THEN /\ planner' = [phase |-> "publishing", id |-> marker]
                /\ UNCHANGED <<marker, nextId, submitted, cover>>
           ELSE /\ submitted' = submitted + 1
                /\ cover' = [cover EXCEPT ![submitted + 1] = marker]
                /\ UNCHANGED <<marker, nextId, planner>>
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED podVars
    /\ UNCHANGED natsVars

Publish ==
    /\ planner.phase = "publishing"
    /\ copies' = IF HasLiveCopy(planner.id)
                   THEN copies
                   ELSE Append(copies, [id |-> planner.id, state |-> "ready",
                                        deadline |-> 0, pod |-> NoPod])
    /\ submitted' = submitted + 1
    /\ cover' = [cover EXCEPT ![submitted + 1] = planner.id]
    /\ planner' = [phase |-> "idle", id |-> NoMarker]
    /\ UNCHANGED <<now, offset, lease, marker, nextId, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED podVars
    /\ UNCHANGED natsVars

PlannerCrash ==
    /\ planner.phase = "publishing"
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ planner' = [phase |-> "idle", id |-> NoMarker]
    /\ UNCHANGED <<now, offset, lease, marker, nextId, copies, submitted, cover, started, stallsLeft>>
    /\ UNCHANGED podVars
    /\ UNCHANGED natsVars

Deliver(p, c) ==
    /\ pc[p] = "idle"
    /\ Awake(p)
    /\ copies[c].state = "ready"
    /\ copies' = [copies EXCEPT ![c].state = "inflight", ![c].pod = p,
                                ![c].deadline = now + AckWait]
    /\ pc' = [pc EXCEPT ![p] = "leasing"]
    /\ held' = [held EXCEPT ![p] = c]
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<seenRev, renewAt, stalled, plannerVars>>
    /\ UNCHANGED natsVars

Redeliver(c) ==
    /\ copies[c].state = "inflight"
    /\ now >= copies[c].deadline
    /\ copies' = [copies EXCEPT ![c].state = "ready", ![c].pod = NoPod]
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<podVars, plannerVars>>
    /\ UNCHANGED natsVars

AcquireLease(p) ==
    /\ pc[p] = "leasing"
    /\ Awake(p)
    /\ LeaseAcquirable(p)
    /\ lease' = [holder |-> p, expiresAt |-> Local(p) + Ttl, rev |-> lease.rev + 1]
    /\ seenRev' = [seenRev EXCEPT ![p] = lease.rev + 1]
    /\ renewAt' = [renewAt EXCEPT ![p] = Local(p) + RenewEvery]
    /\ pc' = [pc EXCEPT ![p] = "leased"]
    /\ UNCHANGED <<now, offset, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, stalled, plannerVars>>
    /\ UNCHANGED natsVars

StartTask(p) ==
    LET id == copies[held[p]].id IN
    /\ pc[p] = "leased"
    /\ Awake(p)
    /\ started' = started \cup {id}
    /\ marker' = IF StartClearsOnlyOwnMarker /\ marker # id THEN marker ELSE NoMarker
    /\ pc' = [pc EXCEPT ![p] = "running"]
    /\ UNCHANGED <<now, offset, lease, nextId, copies, planner, submitted, cover, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, seenRev, renewAt, stalled>>
    /\ UNCHANGED natsVars

Renew(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ Local(p) >= renewAt[p]
    /\ IF HoldsCurrentLease(p)
         THEN /\ lease' = [holder |-> p, expiresAt |-> Local(p) + Ttl, rev |-> lease.rev + 1]
              /\ seenRev' = [seenRev EXCEPT ![p] = lease.rev + 1]
              /\ renewAt' = [renewAt EXCEPT ![p] = Local(p) + RenewEvery]
              /\ copies' = IF copies[held[p]].state = "inflight" /\ copies[held[p]].pod = p
                             THEN [copies EXCEPT ![held[p]].deadline = now + AckWait]
                             ELSE copies
              /\ UNCHANGED <<pc, held>>
         ELSE /\ DropRun(p)
              /\ UNCHANGED <<lease, seenRev, renewAt, copies>>
    /\ UNCHANGED <<now, offset, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<stalled, plannerVars>>
    /\ UNCHANGED natsVars

RenewFails(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ Local(p) >= renewAt[p]
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ DropRun(p)
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft>>
    /\ UNCHANGED <<seenRev, renewAt, stalled, plannerVars>>
    /\ UNCHANGED natsVars

Work(p) ==
    /\ pc[p] = "running"
    /\ Awake(p)
    /\ pc' = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, seenRev, renewAt, stalled, plannerVars>>
    /\ UNCHANGED natsVars

Finish(p) ==
    /\ pc[p] = "done"
    /\ Awake(p)
    /\ copies' = IF copies[held[p]].state = "lost"
                   THEN copies
                   ELSE [copies EXCEPT ![held[p]].state = "acked"]
    /\ lease' = IF HoldsCurrentLease(p)
                  THEN [holder |-> NoPod, expiresAt |-> 0, rev |-> lease.rev + 1]
                  ELSE lease
    /\ DropRun(p)
    /\ UNCHANGED <<now, offset, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<seenRev, renewAt, stalled, plannerVars>>
    /\ UNCHANGED natsVars

Stall(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ stallsLeft > 0
    /\ stallsLeft' = stallsLeft - 1
    /\ stalled' = [stalled EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<now, offset, lease, copies, started, crashesLeft>>
    /\ UNCHANGED <<pc, held, seenRev, renewAt, plannerVars>>
    /\ UNCHANGED natsVars

Resume(p) ==
    /\ stalled[p]
    /\ stalled' = [stalled EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<pc, held, seenRev, renewAt, plannerVars>>
    /\ UNCHANGED natsVars

Crash(p) ==
    /\ pc[p] # "idle"
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ DropRun(p)
    /\ stalled' = [stalled EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft>>
    /\ UNCHANGED <<seenRev, renewAt, plannerVars>>
    /\ UNCHANGED natsVars

\* why: a wipe loses the message of every task that isn't done, started or not. The recovery campaign of ADR 023 replans those keys.
IsDone(id) == \E c \in DOMAIN copies : copies[c].id = id /\ copies[c].state = "acked"

LoseCopy(c) == IF copies[c].state \in LiveStates THEN [copies[c] EXCEPT !.state = "lost"] ELSE copies[c]

NatsWipe ==
    /\ wipesLeft > 0
    /\ wipesLeft' = wipesLeft - 1
    /\ lease' = [holder |-> NoPod, expiresAt |-> 0, rev |-> 0]
    /\ marker' = NoMarker
    /\ copies' = [c \in DOMAIN copies |-> LoseCopy(c)]
    /\ lostInWipe' = lostInWipe \cup {id \in 1..(nextId - 1) : ~IsDone(id)}
    /\ UNCHANGED <<now, offset, nextId, planner, submitted, cover, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<duplicatesLeft, lossesLeft, podVars>>

DuplicateCopy(c) ==
    /\ duplicatesLeft > 0
    /\ copies[c].state # "lost"
    /\ duplicatesLeft' = duplicatesLeft - 1
    /\ copies' = Append(copies, [id |-> copies[c].id, state |-> "ready", deadline |-> 0, pod |-> NoPod])
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<wipesLeft, lossesLeft, lostInWipe, podVars, plannerVars>>

LoseMessage(c) ==
    /\ lossesLeft > 0
    /\ copies[c].state \in LiveStates
    /\ lossesLeft' = lossesLeft - 1
    /\ copies' = [copies EXCEPT ![c].state = "lost"]
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<wipesLeft, duplicatesLeft, lostInWipe, podVars, plannerVars>>

Next ==
    \/ Tick
    \/ Submit
    \/ Publish
    \/ PlannerCrash
    \/ NatsWipe
    \/ \E c \in DOMAIN copies : Redeliver(c)
    \/ \E c \in DOMAIN copies : DuplicateCopy(c)
    \/ \E c \in DOMAIN copies : LoseMessage(c)
    \/ \E p \in Pods :
        \/ \E c \in DOMAIN copies : Deliver(p, c)
        \/ AcquireLease(p)
        \/ StartTask(p)
        \/ Renew(p)
        \/ RenewFails(p)
        \/ Work(p)
        \/ Finish(p)
        \/ Stall(p)
        \/ Resume(p)
        \/ Crash(p)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ now \in 0..MaxTime
    /\ lease.holder \in Pods \cup {NoPod}
    /\ marker \in {NoMarker} \cup Ids
    /\ nextId \in 1..(MaxIds + 1)
    /\ Len(copies) <= MaxIds + MaxDuplicates
    /\ submitted \in 0..MaxEvents
    /\ started \subseteq Ids
    /\ pc \in [Pods -> {"idle", "leasing"} \cup HoldingPhases]

NoOverlap == Cardinality({p \in Pods : pc[p] \in RunPhases}) <= 1

\* why: after a wipe, the recovery campaign of ADR 023 owns every task from before the wipe. A submit in flight during the wipe can leave one extra pending task, and the key then runs once more.
PendingIds == {id \in Ids : id \notin started /\ id \notin lostInWipe /\ HasLiveCopy(id)}

AtMostOnePending == PendingIds \subseteq {marker}

Covered(id) ==
    \/ id \in started
    \/ HasLiveCopy(id)
    \/ planner = [phase |-> "publishing", id |-> id]
    \/ id \in lostInWipe

EverySubmitCovered == \A e \in 1..submitted : Covered(cover[e])
=============================================================================
