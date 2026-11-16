---------------------------- MODULE LeaseDedupe ----------------------------
\* ADR 023 lease and pending-dedupe protocol: no overlapping runs of one key, one pending task per key.
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
    StartClearsOnlyOwnMarker,
    SubmitRepublishesFoundMarker,
    HolderStopsBeforeExpiry,
    WritesFencedByLeaseRevision

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
    token,
    seenRev,
    expiry,
    renewAt,
    stalled,
    stallsLeft,
    crashesLeft,
    storeFence,
    staleWrite

vars == <<now, offset, lease, marker, nextId, copies, planner, submitted, cover,
          started, pc, held, token, seenRev, expiry, renewAt, stalled,
          stallsLeft, crashesLeft, storeFence, staleWrite>>

podVars == <<pc, held, token, seenRev, expiry, renewAt, stalled>>
plannerVars == <<marker, nextId, planner, submitted, cover>>
storeVars == <<storeFence, staleWrite>>

NoMarker == 0
MaxIds == MaxEvents + MaxCrashes
Ids == 1..MaxIds
HoldingPhases == {"leased", "running", "writing", "done"}
RunPhases == {"running", "writing", "done"}
LiveStates == {"ready", "inflight"}

Local(p) == now + offset[p]

Max(a, b) == IF a > b THEN a ELSE b

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
    /\ token = [p \in Pods |-> 0]
    /\ seenRev = [p \in Pods |-> 0]
    /\ expiry = [p \in Pods |-> 0]
    /\ renewAt = [p \in Pods |-> 0]
    /\ stalled = [p \in Pods |-> FALSE]
    /\ stallsLeft = MaxStalls
    /\ crashesLeft = MaxCrashes
    /\ storeFence = 0
    /\ staleWrite = FALSE

Tick ==
    /\ now < MaxTime
    /\ \A p \in Pods : (pc[p] \in HoldingPhases /\ Awake(p)) => Local(p) < renewAt[p]
    /\ now' = now + 1
    /\ UNCHANGED <<offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<podVars, plannerVars, storeVars>>

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
    /\ UNCHANGED <<podVars, storeVars>>

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
    /\ UNCHANGED <<podVars, storeVars>>

PlannerCrash ==
    /\ planner.phase = "publishing"
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ planner' = [phase |-> "idle", id |-> NoMarker]
    /\ UNCHANGED <<now, offset, lease, marker, nextId, copies, submitted, cover, started, stallsLeft>>
    /\ UNCHANGED <<podVars, storeVars>>

Deliver(p, c) ==
    /\ pc[p] = "idle"
    /\ Awake(p)
    /\ copies[c].state = "ready"
    /\ copies' = [copies EXCEPT ![c].state = "inflight", ![c].pod = p,
                                ![c].deadline = now + AckWait]
    /\ pc' = [pc EXCEPT ![p] = "leasing"]
    /\ held' = [held EXCEPT ![p] = c]
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<token, seenRev, expiry, renewAt, stalled, plannerVars, storeVars>>

Redeliver(c) ==
    /\ copies[c].state = "inflight"
    /\ now >= copies[c].deadline
    /\ copies' = [copies EXCEPT ![c].state = "ready", ![c].pod = NoPod]
    /\ UNCHANGED <<now, offset, lease, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<podVars, plannerVars, storeVars>>

AcquireLease(p) ==
    /\ pc[p] = "leasing"
    /\ Awake(p)
    /\ LeaseAcquirable(p)
    /\ lease' = [holder |-> p, expiresAt |-> Local(p) + Ttl, rev |-> lease.rev + 1]
    /\ token' = [token EXCEPT ![p] = lease.rev + 1]
    /\ seenRev' = [seenRev EXCEPT ![p] = lease.rev + 1]
    /\ expiry' = [expiry EXCEPT ![p] = Local(p) + Ttl]
    /\ renewAt' = [renewAt EXCEPT ![p] = Local(p) + RenewEvery]
    /\ pc' = [pc EXCEPT ![p] = "leased"]
    /\ UNCHANGED <<now, offset, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, stalled, plannerVars, storeVars>>

StartTask(p) ==
    LET id == copies[held[p]].id IN
    /\ pc[p] = "leased"
    /\ Awake(p)
    /\ started' = started \cup {id}
    /\ marker' = IF StartClearsOnlyOwnMarker /\ marker # id THEN marker ELSE NoMarker
    /\ pc' = [pc EXCEPT ![p] = "running"]
    /\ UNCHANGED <<now, offset, lease, nextId, copies, planner, submitted, cover, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, token, seenRev, expiry, renewAt, stalled, storeVars>>

Renew(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ Local(p) >= renewAt[p]
    /\ IF HoldsCurrentLease(p)
         THEN /\ lease' = [holder |-> p, expiresAt |-> Local(p) + Ttl, rev |-> lease.rev + 1]
              /\ seenRev' = [seenRev EXCEPT ![p] = lease.rev + 1]
              /\ expiry' = [expiry EXCEPT ![p] = Local(p) + Ttl]
              /\ renewAt' = [renewAt EXCEPT ![p] = Local(p) + RenewEvery]
              /\ copies' = IF copies[held[p]].state = "inflight" /\ copies[held[p]].pod = p
                             THEN [copies EXCEPT ![held[p]].deadline = now + AckWait]
                             ELSE copies
              /\ UNCHANGED <<pc, held>>
         ELSE /\ DropRun(p)
              /\ UNCHANGED <<lease, seenRev, expiry, renewAt, copies>>
    /\ UNCHANGED <<now, offset, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<token, stalled, plannerVars, storeVars>>

RenewFails(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ Local(p) >= renewAt[p]
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ DropRun(p)
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft>>
    /\ UNCHANGED <<token, seenRev, expiry, renewAt, stalled, plannerVars, storeVars>>

BeginWrite(p) ==
    /\ pc[p] = "running"
    /\ Awake(p)
    /\ HolderStopsBeforeExpiry => Local(p) < expiry[p] - SkewMargin
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<held, token, seenRev, expiry, renewAt, stalled, plannerVars, storeVars>>

CommitWrite(p) ==
    /\ pc[p] = "writing"
    /\ Awake(p)
    /\ IF WritesFencedByLeaseRevision /\ token[p] < storeFence
         THEN /\ DropRun(p)
              /\ UNCHANGED storeVars
         ELSE /\ staleWrite' = (staleWrite \/ token[p] < storeFence)
              /\ storeFence' = Max(storeFence, token[p])
              /\ pc' = [pc EXCEPT ![p] = "done"]
              /\ UNCHANGED held
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<token, seenRev, expiry, renewAt, stalled, plannerVars>>

Finish(p) ==
    /\ pc[p] = "done"
    /\ Awake(p)
    /\ copies' = [copies EXCEPT ![held[p]].state = "acked"]
    /\ lease' = IF HoldsCurrentLease(p)
                  THEN [holder |-> NoPod, expiresAt |-> 0, rev |-> lease.rev + 1]
                  ELSE lease
    /\ DropRun(p)
    /\ UNCHANGED <<now, offset, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<token, seenRev, expiry, renewAt, stalled, plannerVars, storeVars>>

Stall(p) ==
    /\ pc[p] \in HoldingPhases
    /\ Awake(p)
    /\ stallsLeft > 0
    /\ stallsLeft' = stallsLeft - 1
    /\ stalled' = [stalled EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<now, offset, lease, copies, started, crashesLeft>>
    /\ UNCHANGED <<pc, held, token, seenRev, expiry, renewAt, plannerVars, storeVars>>

Resume(p) ==
    /\ stalled[p]
    /\ stalled' = [stalled EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft, crashesLeft>>
    /\ UNCHANGED <<pc, held, token, seenRev, expiry, renewAt, plannerVars, storeVars>>

Crash(p) ==
    /\ pc[p] # "idle"
    /\ crashesLeft > 0
    /\ crashesLeft' = crashesLeft - 1
    /\ DropRun(p)
    /\ stalled' = [stalled EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<now, offset, lease, copies, started, stallsLeft>>
    /\ UNCHANGED <<token, seenRev, expiry, renewAt, plannerVars, storeVars>>

Next ==
    \/ Tick
    \/ Submit
    \/ Publish
    \/ PlannerCrash
    \/ \E c \in DOMAIN copies : Redeliver(c)
    \/ \E p \in Pods :
        \/ \E c \in DOMAIN copies : Deliver(p, c)
        \/ AcquireLease(p)
        \/ StartTask(p)
        \/ Renew(p)
        \/ RenewFails(p)
        \/ BeginWrite(p)
        \/ CommitWrite(p)
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
    /\ Len(copies) <= MaxIds
    /\ submitted \in 0..MaxEvents
    /\ started \subseteq Ids
    /\ pc \in [Pods -> {"idle", "leasing"} \cup HoldingPhases]

NoStaleWrite == ~staleWrite

NoOverlap == Cardinality({p \in Pods : pc[p] \in RunPhases}) <= 1

PendingIds == {id \in Ids : id \notin started /\ HasLiveCopy(id)}

AtMostOnePending == PendingIds \subseteq {marker}

Covered(id) ==
    \/ id \in started
    \/ HasLiveCopy(id)
    \/ planner = [phase |-> "publishing", id |-> id]

EverySubmitCovered == \A e \in 1..submitted : Covered(cover[e])
=============================================================================
