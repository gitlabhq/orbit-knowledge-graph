------------------------ MODULE LeaseDedupeTraceCheck ------------------------
\* why: TLC follows each trace that the simulation records from the real lane loop. A step that LeaseDedupe can't take leaves no next state, and TLC reports a deadlock at that step.
EXTENDS LeaseDedupeTraces

VARIABLES trace, step

traceVars == <<vars, trace, step>>

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
    /\ rejectedCurrent = s.rejectedCurrent
    /\ generation = s.generation
    /\ natsGeneration = s.natsGeneration
    /\ wipesLeft = s.wipesLeft
    /\ duplicatesLeft = s.duplicatesLeft
    /\ lossesLeft = s.lossesLeft
    /\ lostInWipe = s.lostInWipe

TraceInit ==
    /\ trace \in DOMAIN Traces
    /\ step = 1
    /\ Matches(Traces[trace][1])

FollowTrace ==
    /\ step < Len(Traces[trace])
    /\ step' = step + 1
    /\ UNCHANGED trace
    /\ Next
    /\ Matches(Traces[trace][step])'

EndOfTrace ==
    /\ step = Len(Traces[trace])
    /\ UNCHANGED traceVars

TraceSpec == TraceInit /\ [][FollowTrace \/ EndOfTrace]_traceVars

StartsAtInit == step = 1 => Init
=============================================================================
