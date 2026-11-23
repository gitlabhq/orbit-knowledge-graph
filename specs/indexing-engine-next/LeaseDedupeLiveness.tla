------------------------- MODULE LeaseDedupeLiveness -------------------------
\* why: LeaseDedupe bounds time with MaxTime, so a lease or an ack wait could stay open forever once time stops. Here faults stop at some point, time jumps past every expiry and every ack wait, and engine steps are fair. Then every acked submit must end done, or lost in a wipe and owned by the recovery campaign.
EXTENDS LeaseDedupe

VARIABLE quiet

allVars == <<vars, quiet>>

Horizon == MaxTime + 1 + Ttl + SkewMargin + AckWait

EngineStep ==
    \/ Tick
    \/ Submit
    \/ Publish
    \/ RecoverNats
    \/ \E c \in DOMAIN copies : Redeliver(c)
    \/ \E p \in Pods :
        \/ \E c \in DOMAIN copies : Deliver(p, c)
        \/ AcquireLease(p)
        \/ GiveBack(p)
        \/ StartTask(p)
        \/ Renew(p)
        \/ BeginWrite(p)
        \/ CommitWrite(p)
        \/ Finish(p)
        \/ Resume(p)

FaultStep ==
    \/ PlannerCrash
    \/ NatsWipe
    \/ \E c \in DOMAIN copies : DuplicateCopy(c)
    \/ \E c \in DOMAIN copies : LoseMessage(c)
    \/ \E p \in Pods : RenewFails(p) \/ Stall(p) \/ Crash(p)

FaultsStop ==
    /\ ~quiet
    /\ quiet' = TRUE
    /\ now' = Horizon
    /\ UNCHANGED <<offset, lease, pending, nextId, copies, planner, submitted, cover, started>>
    /\ UNCHANGED <<stallsLeft, crashesLeft, podVars, storeVars, natsVars>>

LiveInit == Init /\ quiet = FALSE

LiveNext ==
    \/ EngineStep /\ UNCHANGED quiet
    \/ ~quiet /\ FaultStep /\ UNCHANGED quiet
    \/ FaultsStop

LiveSpec ==
    /\ LiveInit
    /\ [][LiveNext]_allVars
    /\ WF_allVars(FaultsStop)
    /\ WF_allVars(Publish /\ UNCHANGED quiet)
    /\ WF_allVars(RecoverNats /\ UNCHANGED quiet)
    /\ WF_allVars(\E c \in DOMAIN copies : Redeliver(c) /\ UNCHANGED quiet)
    /\ \A p \in Pods :
        /\ WF_allVars(\E c \in DOMAIN copies : Deliver(p, c) /\ UNCHANGED quiet)
        /\ WF_allVars(AcquireLease(p) /\ UNCHANGED quiet)
        /\ WF_allVars(GiveBack(p) /\ UNCHANGED quiet)
        /\ WF_allVars(StartTask(p) /\ UNCHANGED quiet)
        /\ WF_allVars(Renew(p) /\ UNCHANGED quiet)
        /\ WF_allVars(BeginWrite(p) /\ UNCHANGED quiet)
        /\ WF_allVars(CommitWrite(p) /\ UNCHANGED quiet)
        /\ WF_allVars(Finish(p) /\ UNCHANGED quiet)
        /\ WF_allVars(Resume(p) /\ UNCHANGED quiet)

DoneOrOwnedByRecovery(id) == id \in lostInWipe \/ IsDone(id)

EverySubmitIsDone ==
    \A e \in 1..MaxEvents : [](e <= submitted => <>DoneOrOwnedByRecovery(cover[e]))

PodsEndIdle == <>[](\A p \in Pods : pc[p] = "idle")
=============================================================================
