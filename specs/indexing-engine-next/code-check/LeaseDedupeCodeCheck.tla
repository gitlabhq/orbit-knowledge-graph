------------------------- MODULE LeaseDedupeCodeCheck -------------------------
\* why: TLC follows each run of the Rust protocol code, which the cargo test writes to LeaseDedupeRuns. It deadlocks at the first step that LeaseDedupe can't take, and fails when the code allows a different set of actions than the spec.
EXTENDS LeaseDedupeRuns, LeaseDedupeEnabled

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
    /\ rejectedCurrent = s.rejectedCurrent
    /\ generation = s.generation
    /\ natsGeneration = s.natsGeneration
    /\ wipesLeft = s.wipesLeft
    /\ duplicatesLeft = s.duplicatesLeft
    /\ lossesLeft = s.lossesLeft
    /\ lostInWipe = s.lostInWipe

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

CodeAllowsWhatSpecAllows == Runs[run][step].enabled = SpecEnabled
=============================================================================
