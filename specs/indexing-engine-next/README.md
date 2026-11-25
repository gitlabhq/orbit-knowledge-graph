# Indexing engine spec

## Summary

This folder holds a TLA+ spec of the lease and dedupe protocol of ADR 023. TLC checks every order of events in the spec, with pod faults and NATS faults. It also checks that every acked submit ends done once faults stop. A second check proves that the Rust code has the same state graph as the spec in 4 small clusters.

TLC found 2 races in the dedupe rules, a NATS fault that loses work, and one gap after a wipe. Two fixes close the races, and we keep 2 cases open on purpose. The spec has no fence: workers write rows versioned by their source, so a rare overlap of two runs is harmless. The code matches the spec in 1.9 million states and 4.1 million steps.

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
                     ┌───────────────────────────────────────┴────┐
                     │ crates/indexing-engine-next                │
                     │  src/lease/rules.rs      the rules as code │
                     │  src/testkit/simulation/ clock, key-value  │
                     │                          store, broker, db │
                     │  tests/protocol/         simulated cluster │
                     └────────────────────────────────────────────┘
```

| Path | What it holds |
|---|---|
| `LeaseDedupe.tla` | The spec: the state, the actions and the rules |
| `LeaseDedupe.cfg`, `LeaseDedupe_ThreePods.cfg` | The model checks with 2 and 3 pods |
| `LeaseDedupeLiveness.tla`, `liveness/` | The liveness check and its 4 configs |
| `counterexamples/`, `liveness/counterexamples/` | 8 configs that must break a rule |
| `code-check/` | The 4 small clusters of the code check |
| `src/lease/rules.rs` in the crate | The rules as code with no IO. The runner calls them. |
| `src/testkit/simulation/` in the crate | A simulated clock, key-value store and broker |
| `tests/protocol/` in the crate | A simulated cluster with one method for each spec action |

## Why

We want two rules to hold, whatever happens to pods and to NATS:

- At most one task of a key waits in the queue. This is the marker.
- Every acked submit runs once faults stop. This is the liveness check.

The lease keeps two runs of one key apart most of the time. A stall or a wipe can still let two runs overlap. We accept that, because workers write rows versioned by their source: the newest data wins, whichever run writes last.

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
- A wipe and a duplicate together give too many states to compare as graphs.
- Each pod has one slot, so the model can't show one pod with two runs of a key. A unit test covers the holder check for that case, before and after a wipe.
- The spec doesn't check writes. A flow must write rows versioned by their source. A flow that versions rows by the time of the write can lose newer rows to a late run, until the next run of the key.
- The spec drops a republish while the task has a live copy. NATS drops a message ID only for 2 min after the first publish. A later republish stores a second copy, and the spec covers that only as its duplicate fault, at most once.
- The liveness check needs faults to stop at some point. A cluster that keeps failing forever makes no progress, and no protocol can fix that.
- The recovery campaign, the sweeps and the reconcilers aren't in the spec. The spec hands their work to them and stops there.

## How it works

### The model

The model has one key and 2 or 3 pods with one slot each. One time unit is 20 s. The lease lasts 3 units, a pod renews it every unit, and the skew margin and the ack wait are 1 and 2 units. Each pod has a clock offset of 0 or 1.

A wipe empties the key-value store and the broker, and the revisions start again at 1.

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

1. The cluster wires the simulator to the protocol code. Each spec action is one method with the same name. The simulator plays the environment, and the protocol code makes every decision about the lease and the marker.
2. Rust starts from every initial state and tries every action in every state. It prints each state the way TLC prints it.
3. TLC model-checks the spec with the same bounds and dumps its graph as a DOT file.
4. The test compares the initial states, the states and the steps. A state or a step that only one side has fails the test, with the states that differ.

Equal steps means every state has the same next states on both sides.

## What is tested

### Rules

TLC checks these rules in every state:

| Rule | Meaning |
|---|---|
| `AtMostOnePending` | Only the task in the marker waits in the queue |
| `EverySubmitCovered` | Every acked submit has a task that started, a live message, or a publish in progress |

The liveness check adds 2 properties:

| Property | Meaning |
|---|---|
| `EverySubmitIsDone` | Every acked submit ends done, or lost in a wipe and handed to the recovery campaign |
| `PodsEndIdle` | Every pod ends idle |

| Liveness config | Faults before they stop | States |
|---|---|---|
| `Liveness` | 1 stall, 1 crash | 101,786 |
| `Liveness_TwoTasks` | 2 tasks, 1 crash | 1,216,372 |
| `Liveness_NatsWipe` | 1 stall, 1 crash, 1 wipe | 523,360 |
| `Liveness_Duplicate` | 1 crash, 1 duplicate | 1,944,136 |

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
| `no_nats_faults` | 1 stall, 1 crash | 69,608 | 144,790 |
| `two_tasks` | 2 tasks, 1 crash | 605,578 | 1,108,128 |
| `nats_wipe` | 1 stall, 1 crash, 1 wipe | 491,372 | 1,159,160 |
| `duplicate` | 1 stall, 1 crash, 1 duplicate | 747,648 | 1,663,400 |

### Counterexamples

Each config switches one fix off, or adds one fault, and must break a rule:

| Config | Breaks | Trace |
|---|---|---|
| `AsWritten_DoublePending` | `AtMostOnePending` | A redelivered task starts and clears the marker of a newer task. |
| `AsWritten_LostSubmit` | `EverySubmitCovered` | The planner crashes after the marker and before the publish. Every later submit of the key is dropped. |
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

The counterexamples above show each trace. We changed the protocol in 2 ways:

```text
race                                   fix
────────────────────────────────────   ──────────────────────────────────────────
start clears a newer task's marker  →  F2  the marker holds its task ID
crash strands the marker forever    →  F3  a submit republishes the found task
```

1. F2, a marker that the task owns. The marker holds the task ID, and a start clears it only if it still holds that ID.
2. F3, a republish. A submit that finds a marker publishes that task again, with the task ID as the message ID.

TLC also found that a stalled pod can write after a newer run, and that a wipe sends the revisions back to 1. A fence fixes both (F1 and F4 in earlier versions), but it costs a generation in NATS and a rule for every flow. Rows versioned by their source make those late writes harmless, so we dropped the fence.

The liveness check found one more gap. A task that started before a wipe loses its message. If its pod then crashes, nothing runs it again. So a wipe hands every task that isn't done to the recovery campaign, not only the pending ones. The recovery campaign of ADR 023 must replan those keys.

Two cases stay open on purpose:

- A submit in flight during a wipe can leave one extra pending task. The recovery campaign of ADR 023 owns it, and the key runs once more.
- A lost acked message is an assumption about NATS. Jepsen found lost writes in NATS 2.12.1 under file corruption and power loss. So we run NATS with 3 replicas and `sync_interval` set to always.

### What the code check found

- In the version with fences, we broke 12 rules in the protocol code one at a time. The code check caught 11, and a unit test caught the 12th.
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
