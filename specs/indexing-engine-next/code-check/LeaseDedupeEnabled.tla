-------------------------- MODULE LeaseDedupeEnabled --------------------------
\* why: the actions that LeaseDedupe enables in a state, named as the Rust cluster names them, so both code checks can compare them.
EXTENDS LeaseDedupe

PodAction(name, p) == name \o "(" \o ToString(p) \o ")"

SpecEnabled ==
    (IF ENABLED Tick THEN {"Tick"} ELSE {})
    \cup (IF ENABLED Submit THEN {"Submit"} ELSE {})
    \cup (IF ENABLED Publish THEN {"Publish"} ELSE {})
    \cup (IF ENABLED PlannerCrash THEN {"PlannerCrash"} ELSE {})
    \cup (IF ENABLED NatsWipe THEN {"NatsWipe"} ELSE {})
    \cup (IF ENABLED RecoverNats THEN {"RecoverNats"} ELSE {})
    \cup {"Redeliver(" \o ToString(c) \o ")" : c \in {c \in DOMAIN copies : ENABLED Redeliver(c)}}
    \cup {"DuplicateCopy(" \o ToString(c) \o ")" : c \in {c \in DOMAIN copies : ENABLED DuplicateCopy(c)}}
    \cup {"LoseMessage(" \o ToString(c) \o ")" : c \in {c \in DOMAIN copies : ENABLED LoseMessage(c)}}
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

SpecState == [now |-> now, offset |-> offset, lease |-> lease, marker |-> marker, nextId |-> nextId,
              copies |-> copies, planner |-> planner, submitted |-> submitted, cover |-> cover,
              started |-> started, pc |-> pc, held |-> held, token |-> token, seenRev |-> seenRev,
              expiry |-> expiry, renewAt |-> renewAt, stalled |-> stalled, stallsLeft |-> stallsLeft,
              crashesLeft |-> crashesLeft, storeFence |-> storeFence, staleWrite |-> staleWrite,
              rejectedCurrent |-> rejectedCurrent, generation |-> generation,
              natsGeneration |-> natsGeneration, wipesLeft |-> wipesLeft,
              duplicatesLeft |-> duplicatesLeft, lossesLeft |-> lossesLeft, lostInWipe |-> lostInWipe,
              enabled |-> SpecEnabled]
=============================================================================
