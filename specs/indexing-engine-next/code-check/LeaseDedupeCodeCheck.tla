------------------------- MODULE LeaseDedupeCodeCheck -------------------------
\* why: TLC follows each run of the Rust protocol code, which the cargo test writes to LeaseDedupeRuns. It deadlocks at the first step that LeaseDedupe can't take, and fails when the code allows a different set of actions than the spec.
EXTENDS LeaseDedupeRuns

VARIABLES run, step

Matches(s) ==
    /\ now = s.now
    /\ offset = s.offset
    /\ lease = s.lease
    /\ marker = s.marker
    /\ nextId = s.nextId
    /\ copies = s.copies
    /\ planner = s.planner
    /\ submitted = s.submitted
    /\ cover = s.cover
    /\ started = s.started
    /\ pc = s.pc
    /\ held = s.held
    /\ token = s.token
    /\ seenRev = s.seenRev
    /\ expiry = s.expiry
    /\ renewAt = s.renewAt
    /\ stalled = s.stalled
    /\ stallsLeft = s.stallsLeft
    /\ crashesLeft = s.crashesLeft
    /\ storeFence = s.storeFence
    /\ staleWrite = s.staleWrite

CheckInit ==
    /\ run \in DOMAIN Runs
    /\ step = 1
    /\ Matches(Runs[run][1])

FollowRun ==
    /\ step < Len(Runs[run])
    /\ step' = step + 1
    /\ UNCHANGED run
    /\ Next
    /\ Matches(Runs[run][step])'

EndOfRun ==
    /\ step = Len(Runs[run])
    /\ UNCHANGED <<vars, run, step>>

CheckSpec == CheckInit /\ [][FollowRun \/ EndOfRun]_<<vars, run, step>>

StartsAtInit == step = 1 => Init

PodAction(name, p) == name \o "(" \o ToString(p) \o ")"

SpecEnabled ==
    (IF ENABLED Tick THEN {"Tick"} ELSE {})
    \cup (IF ENABLED Submit THEN {"Submit"} ELSE {})
    \cup (IF ENABLED Publish THEN {"Publish"} ELSE {})
    \cup (IF ENABLED PlannerCrash THEN {"PlannerCrash"} ELSE {})
    \cup {"Redeliver(" \o ToString(c) \o ")" : c \in {c \in DOMAIN copies : ENABLED Redeliver(c)}}
    \cup {"Deliver(" \o ToString(pc2[1]) \o "," \o ToString(pc2[2]) \o ")" :
            pc2 \in {pc2 \in Pods \X DOMAIN copies : ENABLED Deliver(pc2[1], pc2[2])}}
    \cup {PodAction("AcquireLease", p) : p \in {p \in Pods : ENABLED AcquireLease(p)}}
    \cup {PodAction("StartTask", p) : p \in {p \in Pods : ENABLED StartTask(p)}}
    \cup {PodAction("Renew", p) : p \in {p \in Pods : ENABLED Renew(p)}}
    \cup {PodAction("RenewFails", p) : p \in {p \in Pods : ENABLED RenewFails(p)}}
    \cup {PodAction("BeginWrite", p) : p \in {p \in Pods : ENABLED BeginWrite(p)}}
    \cup {PodAction("CommitWrite", p) : p \in {p \in Pods : ENABLED CommitWrite(p)}}
    \cup {PodAction("Finish", p) : p \in {p \in Pods : ENABLED Finish(p)}}
    \cup {PodAction("Stall", p) : p \in {p \in Pods : ENABLED Stall(p)}}
    \cup {PodAction("Resume", p) : p \in {p \in Pods : ENABLED Resume(p)}}
    \cup {PodAction("Crash", p) : p \in {p \in Pods : ENABLED Crash(p)}}

CodeAllowsWhatSpecAllows == Runs[run][step].enabled = SpecEnabled
=============================================================================
