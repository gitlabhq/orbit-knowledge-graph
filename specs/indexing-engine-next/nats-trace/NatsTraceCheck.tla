---- MODULE NatsTraceCheck ----
EXTENDS leaseDedupeProtocol, NatsTrace, Sequences, TLC

VARIABLE seen

Actors == DOMAIN Observed

HeldId(p) == copies[held[p]].id

AtObservedTime(o) == o.t - 1 <= now /\ now <= o.t + 1

AfterObservedWipes(o) == MaxWipes - wipesLeft = o.w

Observe(o) ==
  AtObservedTime(o) /\ AfterObservedWipes(o) /\
  CASE o.a = "Submit" -> Submit /\ planner'.id = o.id
    [] o.a = "Publish" -> planner.id = o.id /\ Publish
    [] o.a = "Deliver" -> \E c \in 0..(Len(copies) - 1): copies[c + 1].id = o.id /\ Deliver(o.p, c)
    [] o.a = "AcquireLease" -> HeldId(o.p) = o.id /\ AcquireLease(o.p)
    [] o.a = "GiveBack" -> HeldId(o.p) = o.id /\ GiveBack(o.p)
    [] o.a = "StartTask" -> HeldId(o.p) = o.id /\ StartTask(o.p)
    [] o.a = "Renew" -> Renew(o.p) /\ pc'[o.p] = pc[o.p]
    [] o.a = "RenewLost" -> Renew(o.p) /\ pc'[o.p] = "idle"
    [] o.a = "RenewFails" -> RenewFails(o.p)
    [] o.a = "Finish" -> HeldId(o.p) = o.id /\ Finish(o.p)
    [] o.a = "NatsWipe" -> NatsWipe
    [] o.a = "Crash" -> Crash(o.p) \/ (pc[o.p] = "idle" /\ UNCHANGED vars)

Hidden ==
  \/ Tick
  \/ RedeliverAny
  \/ \E p \in Pods: Work(p)

TraceInit == q_init /\ seen = [actor \in Actors |-> 0]

TraceNext ==
  \/ \E actor \in Actors:
       /\ seen[actor] < Len(Observed[actor])
       /\ Observe(Observed[actor][seen[actor] + 1])
       /\ seen' = [seen EXCEPT ![actor] = @ + 1]
  \/ Hidden /\ UNCHANGED seen

NotEverySeen == \E actor \in Actors: seen[actor] < Len(Observed[actor])
====
