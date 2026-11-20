# Indexing engine spec

## Summary

This folder holds a TLA+ spec of the lease and dedupe protocol of ADR 023. TLC checks every order of events in the spec, with pod faults and NATS faults. It also checks that every acked submit ends done once faults stop. A second check proves that the Rust code has the same state graph as the spec in 4 small clusters.

So far, TLC found 7 ways to break the rules and one gap in the recovery after a wipe. Four fixes close all but 2, and we keep those 2 open on purpose. The code matches the spec in 2.8 million states and 6 million steps.

## What we built

```text
                     ┌──────────────────────────────┐
                     │ LeaseDedupe.tla (the rules)  │
                     └──────────────┬───────────────┘
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                      ▼
   ┌────────────────────┐ ┌───────────────────┐ ┌─────────────────────────┐
   │ model check        │ │ counterexamples   │ │ code check              │
   │ every order of     │ │ 8 configs that    │ │ the code and the spec   │
   │ events, up to 45M  │ │ must break a rule │ │ have the same graph in  │
   │ states, + liveness │ │                   │ │ 4 small clusters        │
   └────────────────────┘ └───────────────────┘ └────────────▲────────────┘
                                                             │
                     ┌───────────────────────────────────────┴───┐
                     │ crates/indexing-engine-next               │
                     │  src/protocol/          the rules as code │
                     │  src/testkit/simulator/ clock, key-value  │
                     │                         store, broker, db │
                     │  tests/protocol/        simulated cluster │
                     └───────────────────────────────────────────┘
```

| Path | What it holds |
|---|---|
| `LeaseDedupe.tla` | The spec: the state, the actions and the rules |
| `LeaseDedupe.cfg`, `LeaseDedupe_ThreePods.cfg` | The model checks with 2 and 3 pods |
| `LeaseDedupeLiveness.tla`, `liveness/` | The liveness check and its 4 configs |
| `counterexamples/`, `liveness/counterexamples/` | 8 configs that must break a rule |
| `code-check/` | The 4 small clusters of the code check |
| `src/protocol/` in the crate | The rules as code with no IO. The NATS lane loop of 1b calls it. |
| `src/testkit/simulator/` in the crate | A simulated clock, key-value store, broker and database |
| `tests/protocol/` in the crate | A simulated cluster with one method for each spec action |

## Why

We want two rules to hold, whatever happens to pods and to NATS:

- One run of a key at a time writes to the database. This is the lease.
- At most one task of a key waits in the queue. This is the marker.

Pods stall, crash and see different clocks. NATS redelivers, delivers twice, loses messages and gets wiped. Tests that we write by hand rarely hit the bad orders of events. A model checker tries all of them.

A proof of the spec says nothing about our code, though. So the code check ties the two together. If the code drifts from the spec, CI fails and shows the states that differ.

## What it guarantees

| Check | What it proves |
|---|---|
| Model check | The rules hold in every reachable state of the spec, with 2 or 3 pods |
| Liveness check | Once faults stop, every acked submit ends done and every pod ends idle |
| Counterexamples | Each known race still breaks a rule, so the checks can fail |
| Code check | In 4 small clusters, the code and the spec have the same initial states, states and steps |
| Unit tests | Each rule of the protocol code, including one case that the model can't show |

Within the bounds of the 4 clusters, the code takes no step that the spec forbids. It also refuses no step that the spec allows. Beyond those bounds, we rely on the model check of the spec. Most protocol bugs show up with few pods.

What it doesn't prove:

- The NATS behaviors are assumptions. A probe against a local NATS 2.12.4 confirmed the 11 that the spec uses. The integration tests of 1b will check them in CI.
- A wipe and a duplicate together give 11M states or more, too many to compare as graphs.
- Each pod has one slot, so the model can't show one pod with two runs of a key. A unit test covers the holder check for that case.
- The liveness check needs faults to stop at some point. A cluster that keeps failing forever makes no progress, and no protocol can fix that.
- The recovery campaign, the sweeps and the reconcilers aren't in the spec. The spec hands their work to them and stops there.

## How it works

### The model

The model has one key and 2 or 3 pods with one slot each. One time unit is 20 s. The lease lasts 3 units, a pod renews it every unit, and the skew margin and the ack wait are 1 and 2 units. Each pod has a clock offset of 0 or 1.

A wipe empties the key-value store and the broker. The database survives it. After a wipe, a recovery reads the generation from the database, adds 1, and writes it to the key-value store.

### The liveness check

The spec bounds time, so a lease could stay open forever once time stops. `LeaseDedupeLiveness.tla` adds one step. Faults stop, and the clock jumps past every expiry and every ack wait. From then on, each engine step that stays possible happens in the end. TLC then checks that every acked submit ends done, or lost in a wipe and handed to the recovery campaign.

### The code check

```text
LeaseDedupe.tla + code-check/<cluster>.cfg          tests/protocol/cluster.rs
                │                                             │
       TLC explores every state                 Rust explores every state
       and dumps the graph                      and records every step
                │                                             │
                ▼                                             ▼
        initial states, states, steps   ==   initial states, states, steps
```

1. The cluster wires the simulator to the protocol code. Each spec action is one method with the same name. The simulator plays the environment, and the protocol code makes every decision about the lease, the marker and the fence.
2. Rust starts from every initial state and tries every action in every state. It prints each state the way TLC prints it.
3. TLC model-checks the spec with the same bounds and dumps its graph as a DOT file.
4. The test compares the initial states, the states and the steps. A state or a step that only one side has fails the test, with the states that differ.

Equal steps means every state has the same next states on both sides.

## What is tested

### Rules

TLC checks these rules in every state:

| Rule | Meaning |
|---|---|
| `NoStaleWrite` | The database never keeps a write of an older lease after a newer one |
| `NoRejectedCurrentHolder` | The database never rejects the write of the pod that holds the current lease |
| `AtMostOnePending` | Only the task in the marker waits in the queue |
| `EverySubmitCovered` | Every acked submit has a task that started, a live message, or a publish in progress |

The liveness check adds 2 properties:

| Property | Meaning |
|---|---|
| `EverySubmitIsDone` | Every acked submit ends done, or lost in a wipe and handed to the recovery campaign |
| `PodsEndIdle` | Every pod ends idle |

| Liveness config | Faults before they stop | States |
|---|---|---|
| `Liveness` | 1 stall, 1 crash | 149,282 |
| `Liveness_TwoTasks` | 2 tasks, 1 crash | 1,930,008 |
| `Liveness_NatsWipe` | 1 stall, 1 crash, 1 wipe | 848,760 |
| `Liveness_Duplicate` | 1 crash, 1 duplicate | 3,104,500 |

### Faults

| Fault | What happens |
|---|---|
| Stall | A pod freezes for any length of time, then resumes |
| Crash | A pod drops its run |
| Failed renewal | A renewal doesn't reach the key-value store |
| Planner crash | The planner stops between the marker and the publish |
| Clock skew | A pod's clock runs 1 unit ahead |
| Redelivery | The broker delivers a message again after the ack wait |
| NATS wipe | The key-value store and the broker lose all their state |
| Duplicate | The broker delivers a second copy of a message |
| Lost message | The broker loses a message |

### Small clusters of the code check

All 4 clusters have 2 pods:

| Cluster | Faults | States | Steps |
|---|---|---|---|
| `no_nats_faults` | 1 stall, 1 crash | 95,156 | 198,844 |
| `two_tasks` | 2 tasks, 1 crash | 865,748 | 1,551,818 |
| `nats_wipe` | 1 stall, 1 crash, 1 wipe | 829,510 | 2,019,408 |
| `duplicate` | 1 stall, 1 crash, 1 duplicate | 1,023,506 | 2,250,220 |

### Counterexamples

Each config switches one fix off, or adds one fault, and must break a rule:

| Config | Breaks | Trace |
|---|---|---|
| `AsWritten_StaleWrite` | `NoStaleWrite` | A pod stalls after it sends its write. Another pod takes the expired lease and writes. Then the old write lands. |
| `StopBeforeExpiry_StaleWrite` | `NoStaleWrite` | Same trace, with a pod that checks its expiry before the write. A check on the pod alone can't fix it. |
| `Fenced_RunsOverlapInTime` | `NoOverlap` | With the fence, two pods still run at once. Only their writes are safe. |
| `AsWritten_DoublePending` | `AtMostOnePending` | A redelivered task starts and clears the marker of a newer task. |
| `AsWritten_LostSubmit` | `EverySubmitCovered` | The planner crashes after the marker and before the publish. Every later submit of the key is dropped. |
| `NatsWipe_FenceWithoutGeneration` | `NoRejectedCurrentHolder` | A wipe restarts the revisions, so the database rejects the writes of the new holder. |
| `NatsLosesMessage` | `EverySubmitCovered` | NATS loses an acked message, and nothing brings it back. |
| `liveness/counterexamples/AsWritten_LostSubmit` | `EverySubmitIsDone` | Without F3, a stranded marker swallows a submit, and its task never runs. |

## Run it

| Command | What it does | Time on a laptop |
|---|---|---|
| `mise run spec:engine` | Model-checks the spec with both configs | about 3 min |
| `mise run spec:engine:liveness` | Checks that every acked submit ends done | about 4 min |
| `mise run spec:engine:counterexample` | Checks that each counterexample still breaks a rule | about 25 s |
| `mise run spec:engine:code` | Compares the code and the spec in all 4 clusters | about 6 min |
| `cargo test -p indexing-engine-next --test protocol` | Runs the rule tests and the invariants of the first cluster | about 10 s |

`ENGINE_SPEC_CLUSTER=two_tasks mise run spec:engine:code` checks one cluster. The mise tasks download a pinned `tla2tools.jar` into `target/` and install the Java version that they pin.

In CI, the jobs live in `.gitlab/ci/indexing-engine-spec.yml`. The model check, the liveness check and the counterexamples run when the spec changes. The code check runs one job for each cluster, in parallel, when the spec, the protocol code or the simulator changes. The `unit-test` job runs the tests of the crate on every pipeline, the protocol and simulator tests included.

## When you change something

- A rule in the spec: change the protocol code in the same MR. The code check fails until both match.
- A rule in the code: the code check fails and shows the states that differ. Change the spec too, or fix the code.
- A new variable in the spec: add it to `state_lines` in `tests/protocol/cluster/tlc.rs`.
- A new action in the spec: add it to the `Action` enum and to the cluster. Until then, the spec has steps that the code lacks, and the code check fails.
- A new fault: add a budget to `FaultBudget`, an action to the spec and to the cluster, and a counterexample if it breaks a rule.

## Results

### What TLC found

The rules as ADR 023 wrote them break in 5 ways, and NATS faults add 2 more. The counterexamples above show each trace. We changed the protocol in 4 ways:

```text
race                                   fix
────────────────────────────────────   ──────────────────────────────────────────
old write lands after a newer one   →  F1  fence: the lease revision at take time
start clears a newer task's marker  →  F2  the marker holds its task ID
crash strands the marker forever    →  F3  a submit republishes the found task
wipe sends revisions back to 1      →  F4  fence = (generation, revision)
```

1. F1, a fence. A pod writes with the lease revision from when it took the lease. The database keeps a write only if its fence is the highest it has seen.
2. F2, a marker that the task owns. The marker holds the task ID, and a start clears it only if it still holds that ID.
3. F3, a republish. A submit that finds a marker publishes that task again, with the task ID as the message ID.
4. F4, a generation in the fence. No pod takes a lease until the recovery wrote the new generation.

The liveness check found one more gap. A task that started before a wipe loses its message. If its pod then crashes, nothing runs it again. So a wipe hands every task that isn't done to the recovery campaign, not only the pending ones. The recovery campaign of ADR 023 must replan those keys.

Two cases stay open on purpose:

- A submit in flight during a wipe can leave one extra pending task. The recovery campaign of ADR 023 owns it, and the key runs once more.
- A lost acked message is an assumption about NATS. Jepsen found lost writes in NATS 2.12.1 under file corruption and power loss. So we run NATS with 3 replicas and `sync_interval` set to always.

### What the code check found

- We broke 12 rules in the protocol code one at a time. The code check catches 11, and a unit test catches the 12th.
- A first version had 1 task in each cluster, and it missed a broken marker rule. That's why `two_tasks` exists.
- 20,000 random runs found nothing. Each 1,000 runs added about 45,000 new states, so random runs never cover the space. We check every state of small clusters instead.

### What NATS confirmed

A probe against a local NATS 2.12.4 confirmed 11 behaviors. These matter most:

- A write with a stale revision is refused.
- Revisions grow for the whole bucket, not by 1 for each key.
- A wiped bucket starts its revisions again at 1.
- The same message ID after the 2 min dedupe window is stored again.
- A progress ack keeps a message from coming back. Without it, the message comes back after the ack wait.

## References

- [Validating Traces of Distributed Programs Against TLA+ Specifications](https://arxiv.org/abs/2404.16075)
- [Smart Casual Verification of the Confidential Consortium Framework](https://www.usenix.org/conference/nsdi25/presentation/howard)
- [Deterministic Simulation Testing](https://martinfowler.com/articles/deterministic-simulation-testing.html)
- [Jepsen: NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1)
- ADR 023 in the engineering docs
