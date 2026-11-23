# Indexing engine spec

## Summary

This folder holds a TLA+ spec of the lease and dedupe protocol of ADR 023. TLC checks every order of events in the spec, with pod faults and NATS faults. It also checks that every acked submit ends done once faults stop. A second check proves that the Rust code has the same state graph as the spec in 4 small clusters. A third check runs the real runner in a seeded simulator. TLC confirms that each step of each run is a step of the spec.

So far, TLC found 7 ways to break the rules and one gap in the recovery after a wipe. Four fixes close all but 2, and we keep those 2 open on purpose. The code matches the spec in 3.7 million states and 8.9 million steps.

## What we built

```text
                          ┌──────────────────────────────┐
                          │ LeaseDedupe.tla (the rules)  │
                          └──────────────┬───────────────┘
       ┌──────────────────────┬──────────┴───────────┬───────────────────────┐
       ▼                      ▼                      ▼                       ▼
┌───────────────┐ ┌───────────────────┐ ┌────────────────────┐ ┌──────────────────────┐
│ model check   │ │ counterexamples   │ │ code check         │ │ trace check          │
│ every order   │ │ 8 configs that    │ │ the protocol code  │ │ every step of 600    │
│ of events,    │ │ must break a rule │ │ and the spec have  │ │ simulated runs of    │
│ + liveness    │ │                   │ │ the same graph     │ │ the real runner      │
└───────────────┘ └───────────────────┘ └─────────▲──────────┘ └──────────▲───────────┘
                                                  │                       │
                  ┌───────────────────────────────┴───────────────────────┴───┐
                  │ crates/indexing-engine-next                               │
                  │  src/lease/rules.rs       the lease rules as code         │
                  │  src/runner/              the runner, on ports            │
                  │  src/testkit/simulation/  key-value store, broker, db,    │
                  │                           and the ports on a seeded world │
                  │  tests/protocol/          simulated cluster               │
                  └───────────────────────────────────────────────────────────┘
```

| Path | What it holds |
|---|---|
| `LeaseDedupe.tla` | The spec: the state, the actions and the rules |
| `LeaseDedupe.cfg`, `LeaseDedupe_ThreePods.cfg` | The model checks with 2 and 3 pods |
| `LeaseDedupeLiveness.tla`, `liveness/` | The liveness check and its 4 configs |
| `counterexamples/`, `liveness/counterexamples/` | 8 configs that must break a rule |
| `code-check/` | The 4 small clusters of the code check |
| `src/lease/rules.rs` in the crate | The lease rules as code with no IO. The runner calls them. |
| `src/testkit/simulation/` in the crate | A simulated tick clock, key-value store, broker and database, with the ports on one seeded world |
| `tests/protocol/` in the crate | A simulated cluster with one method for each spec action |
| `LeaseDedupeTraceCheck.tla` | The trace check: TLC follows each recorded run step by step |
| `src/ports.rs` in the crate | The ports that an adapter implements: clock, task queue, lease store, pending store, protocol log. Each port trait lives with its concept. |
| `src/runner/`, `src/lease/store.rs`, `src/pending.rs` in the crate | The runner and the submit path, written against the ports |
| `tests/simulation.rs` in the crate | The rules of the simulated parts, the seeds, the coverage of each fault, and the trace writer for TLC |

## Why

We want two rules to hold, whatever happens to pods and to NATS:

- One run of a key at a time writes to the database. This is the lease.
- At most one task of a key waits in the queue. This is the pending task.

Pods stall, crash and see different clocks. NATS redelivers, delivers twice, loses messages and gets wiped. Tests that we write by hand rarely hit the bad orders of events. A model checker tries all of them.

A proof of the spec says nothing about our code, though. So the code check ties the two together. If the code drifts from the spec, CI fails and shows the states that differ.

The code check covers the decisions in `src/lease/rules.rs`. It doesn't cover the runner that calls them. That loop sets the order of the reads and the writes. It renews the lease next to the worker, and it acks and releases. The trace check covers that code. It runs the real runner, and TLC checks each step that the loop logs.

## What it guarantees

| Check | What it proves |
|---|---|
| Model check | The rules hold in every reachable state of the spec, with 2 or 3 pods |
| Liveness check | Once faults stop, every acked submit ends done and every pod ends idle |
| Counterexamples | Each known race still breaks a rule, so the checks can fail |
| Code check | In 4 small clusters, the code and the spec have the same initial states, states and steps |
| Unit tests | Each rule of the protocol code, including one case that the model can't show |
| Trace check | Each step of 600 seeded runs of the real runner is a step of the spec, and the rules hold in each state |
| Simulation tests | In 1,200 seeded runs, the rules hold in each state, and every acked submit ends done once faults stop. Each fault happens in at least one seed. |

Within the bounds of the 4 clusters, the code takes no step that the spec forbids. It also refuses no step that the spec allows. Beyond those bounds, we rely on the model check of the spec. Most protocol bugs show up with few pods.

What it doesn't prove:

- The NATS behaviors are assumptions. A probe against a local NATS 2.12.4 confirmed 11 of them. The integration tests of 1b will check all of them in CI.
- The give-back adds 2 assumptions that the probe didn't cover: a negative ack with a delay brings the message back after the delay, and a negative ack of a message that NATS already delivered again frees that delivery too. The second one is the worst case, so the spec allows it.
- A wipe and a duplicate together give 11M states or more, too many to compare as graphs.
- Each pod has one slot, so the model can't show one pod with two runs of a key. A unit test covers the holder check for that case, before and after a wipe.
- A pod reads the generation and takes the lease in one step. In NATS, the generation and the lease are two key-value entries, so a wipe can come between the read and the write. Two runs can then get the same fence. 1b must make it one step: the recovery writes the generation into the lease value, or a run ID goes beside the fence.
- The spec drops a republish while the task has a live copy. NATS drops a message ID only for 2 min after the first publish. A later republish stores a second copy, and the spec covers that only as its duplicate fault, at most once.
- The liveness check needs faults to stop at some point. A cluster that keeps failing forever makes no progress, and no protocol can fix that.
- The recovery campaign, the sweeps and the reconcilers aren't in the spec. The spec hands their work to them and stops there.
- The trace check samples runs. It doesn't explore every order of events, as the code check does.
- In the simulator, each step of the runner is atomic in time. A stall can come between two steps, but not between the clock read and the write of one step. In production, a pod can stall there. Then the lease that it writes expires earlier than the spec says, and the fence keeps the writes safe.
- Interleaved runs let other steps run inside a step, so a CAS write can lose its race. They check the rules and the liveness in Rust, but TLC can't follow them.
- The simulator has one key, one lane, one slot for each process and one planner, the same shape as the spec. A worker that isn't exclusive skips the lease, and no check covers that path yet.
- The simulator replaces NATS with the simulated parts in `src/testkit/simulation/`. The NATS adapters of the ports come in the next MR, with the same scenarios on a real NATS server.

## How it works

### The model

The model has one key and 2 or 3 pods with one slot each. One time unit is 20 s. The lease lasts 3 units, a pod renews it every unit, and the skew margin and the ack wait are 1 and 2 units. Each pod has a clock offset of 0 or 1.

A pod that finds the lease of its key held doesn't wait for it. It gives the message back to the broker with a delay of 1 unit, so its slot is free for other work. The broker delivers the message again after the delay. The liveness configs use a delay of 0, because there the clock jumps past every timer once faults stop.

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

1. The cluster wires the simulator to the protocol code. Each spec action is one method with the same name. The simulator plays the environment, and the protocol code makes every decision about the lease, the pending task and the fence.
2. Rust starts from every initial state and tries every action in every state. It prints each state the way TLC prints it.
3. TLC model-checks the spec with the same bounds and dumps its graph as a DOT file.
4. The test compares the initial states, the states and the steps. A state or a step that only one side has fails the test, with the states that differ.

Equal steps means every state has the same next states on both sides.

### The trace check

```text
tests/simulation.rs                 src/testkit/simulation/             LeaseDedupeTraceCheck.tla
        │                                     │                                    │
 300 seeds for each scenario     real runners on simulated          TLC starts at each trace and
        │                        ports; after each step, the        follows it step by step
        ▼                        spec state of the world                           │
  run(seed) ───────────────────► is written in TLA+ ───────────────► a step that Next can't take
                                                                     is a deadlock: the check fails
```

1. The runner talks to the outside only through the ports in `src/ports.rs`. Each step reads the clock once at its start and logs one protocol event with that time.
2. The simulator implements the ports on one world: a simulated key-value store, broker and database. It runs 2 or 3 real runners in one process on a paused clock. A seed picks the order of the steps, the faults and their targets.
3. After each step, the world writes its state in the variables of the spec. The pod state comes from the protocol events, and the rest comes from the simulated stores.
4. TLC starts from the first state of each trace. For each next state, it looks for a step of `Next` that leads there. When it finds none, it stops with a deadlock and prints the trace and the step.

A time unit of the spec is one tick of 20 s, and each step happens on a tick. A process that crashes stops at once: its old tasks can't take another step. When a rule breaks, every process stops, so the timeline ends at the bug. Too many port calls in one tick also break a rule. So a busy loop fails the run and doesn't hang it.

## What is tested

### Rules

TLC checks these rules in every state:

| Rule | Meaning |
|---|---|
| `NoStaleWrite` | The database never keeps a write of an older lease after a newer one |
| `NoRejectedCurrentHolder` | The database never rejects the write of the pod that holds the current lease |
| `AtMostOnePending` | Only the pending task waits in the queue |
| `EverySubmitCovered` | Every acked submit has a task that started, a live message, or a publish in progress |

The liveness check adds 2 properties:

| Property | Meaning |
|---|---|
| `EverySubmitIsDone` | Every acked submit ends done, or lost in a wipe and handed to the recovery campaign |
| `PodsEndIdle` | Every pod ends idle |

| Liveness config | Faults before they stop | States |
|---|---|---|
| `Liveness` | 1 stall, 1 crash | 164,592 |
| `Liveness_TwoTasks` | 2 tasks, 1 crash | 2,638,648 |
| `Liveness_NatsWipe` | 1 stall, 1 crash, 1 wipe | 920,344 |
| `Liveness_Duplicate` | 1 crash, 1 duplicate | 4,369,530 |

### Faults

| Fault | What happens |
|---|---|
| Stall | A pod freezes for any length of time, then resumes |
| Crash | A pod drops its run |
| Failed renewal | A renewal doesn't reach the key-value store |
| Planner crash | The planner stops between the pending write and the publish |
| Clock skew | A pod's clock runs 1 unit ahead |
| Redelivery | The broker delivers a message again after the ack wait |
| NATS wipe | The key-value store and the broker lose all their state |
| Duplicate | The broker delivers a second copy of a message |
| Lost message | The broker loses a message |

### Small clusters of the code check

All 4 clusters have 2 pods:

| Cluster | Faults | States | Steps |
|---|---|---|---|
| `no_nats_faults` | 1 stall, 1 crash | 110,494 | 254,216 |
| `two_tasks` | 2 tasks, 1 crash | 1,189,216 | 2,475,690 |
| `nats_wipe` | 1 stall, 1 crash, 1 wipe | 934,822 | 2,366,246 |
| `duplicate` | 1 stall, 1 crash, 1 duplicate | 1,487,114 | 3,800,252 |

### Counterexamples

Each config switches one fix off, or adds one fault, and must break a rule:

| Config | Breaks | Trace |
|---|---|---|
| `AsWritten_StaleWrite` | `NoStaleWrite` | A pod stalls after it sends its write. Another pod takes the expired lease and writes. Then the old write lands. |
| `StopBeforeExpiry_StaleWrite` | `NoStaleWrite` | Same trace, with a pod that checks its expiry before the write. A check on the pod alone can't fix it. |
| `Fenced_RunsOverlapInTime` | `NoOverlap` | With the fence, two pods still run at once. Only their writes are safe. |
| `AsWritten_DoublePending` | `AtMostOnePending` | A redelivered task starts and clears a newer pending task. |
| `AsWritten_LostSubmit` | `EverySubmitCovered` | The planner crashes after the pending write and before the publish. Every later submit of the key is dropped. |
| `NatsWipe_FenceWithoutGeneration` | `NoRejectedCurrentHolder` | A wipe restarts the revisions, so the database rejects the writes of the new holder. |
| `NatsLosesMessage` | `EverySubmitCovered` | NATS loses an acked message, and nothing brings it back. |
| `liveness/counterexamples/AsWritten_LostSubmit` | `EverySubmitIsDone` | Without F3, a stranded pending task swallows a submit, and its task never runs. |

## Run it

| Command | What it does | Time on a laptop |
|---|---|---|
| `mise run spec:engine` | Model-checks the spec with both configs | about 7 min |
| `mise run spec:engine:liveness` | Checks that every acked submit ends done | about 14 min |
| `mise run spec:engine:counterexample` | Checks that each counterexample still breaks a rule | about 50 s |
| `mise run spec:engine:code` | Compares the code and the spec in all 4 clusters | about 8 min |
| `mise run spec:engine:trace` | Runs 600 seeds of the runner and checks each step with TLC | about 1 min |
| `cargo test -p indexing-engine-next --test simulation` | Runs 1,200 seeds and checks the rules, the liveness and the coverage of each fault | about 10 s |
| `cargo test -p indexing-engine-next --test protocol` | Runs the rule tests and the invariants of the first cluster | about 10 s |

`ENGINE_SPEC_CLUSTER=two_tasks mise run spec:engine:code` checks one cluster. The mise tasks download a pinned `tla2tools.jar` into `target/` and install the Java version that they pin.

In CI, the jobs live in `.gitlab/ci/indexing-engine-spec.yml`. The model check, the liveness check and the counterexamples run when the spec changes. The code check runs one job for each cluster, in parallel, when the spec, the protocol code or the simulator changes. The trace check runs when the spec or any code of the crate changes. The `unit-test` job runs the tests of the crate on every pipeline, the protocol and simulation tests included.

## When you change something

- A rule in the spec: change the protocol code in the same MR. The code check fails until both match.
- A rule in the code: the code check fails and shows the states that differ. Change the spec too, or fix the code.
- A new variable in the spec: add it to `state_lines` in `tests/protocol/cluster/tlc.rs`.
- A new action in the spec: add it to the `Action` enum and to the cluster. Until then, the spec has steps that the code lacks, and the code check fails.
- A new fault: add a budget to `FaultBudget`, an action to the spec and to the cluster, and a counterexample if it breaks a rule.
- A new step in the runner: read the clock at its start, and log one protocol event at its end. Then map the event to the spec state in `World::apply_protocol_event`. Until the spec has the step, the trace check fails.

## Results

### What TLC found

The rules as ADR 023 wrote them break in 5 ways, and NATS faults add 2 more. The counterexamples above show each trace. We changed the protocol in 4 ways:

```text
race                                   fix
────────────────────────────────────   ──────────────────────────────────────────
old write lands after a newer one   →  F1  fence: the lease revision at take time
start clears a newer pending task   →  F2  pending holds its task ID
crash strands the pending task      →  F3  a submit republishes the found task
wipe sends revisions back to 1      →  F4  fence = (generation, revision)
```

1. F1, a fence. A pod writes with the lease revision from when it took the lease. The database keeps a write only if its fence is the highest it has seen.
2. F2, a pending entry that the task owns. It holds the task ID, and a start clears it only if it still holds that ID.
3. F3, a republish. A submit that finds a pending task publishes that task again, with the task ID as the message ID.
4. F4, a generation in the fence. No pod takes a lease until the recovery wrote the new generation.

The liveness check found one more gap. A task that started before a wipe loses its message. If its pod then crashes, nothing runs it again. So a wipe hands every task that isn't done to the recovery campaign, not only the pending ones. The recovery campaign of ADR 023 must replan those keys.

Two cases stay open on purpose:

- A submit in flight during a wipe can leave one extra pending task. The recovery campaign of ADR 023 owns it, and the key runs once more.
- A lost acked message is an assumption about NATS. Jepsen found lost writes in NATS 2.12.1 under file corruption and power loss. So we run NATS with 3 replicas and `sync_interval` set to always.

### What the code check found

- We broke 12 rules in the protocol code one at a time. The code check catches 11, and a unit test catches the 12th.
- A first version had 1 task in each cluster, and it missed a broken pending rule. That's why `two_tasks` exists.
- 20,000 random runs found nothing. Each 1,000 runs added about 45,000 new states, so random runs never cover the space. We check every state of small clusters instead.

### What the trace check found

The trace check found a bug in the simulator before it passed. A crashed process could still take one step, because its old tasks stopped only later. TLC stopped at a write from a process that the spec had made idle. Now each port call of an old incarnation blocks for good.

The coverage test found 2 more gaps. After a stall, the renewal always ran before the write, so no seed reached a fence rejection. Now a process yields at random each time it wakes up, and half of the stalls wait for the next database write. A seed also hung when the loop made calls with no pause. Now too many port calls in one tick break a rule, so the run ends with a timeline and doesn't hang.

We broke the runner in 11 ways, one at a time:

| Broken rule in the runner | Rust rules | TLC trace check |
|---|---|---|
| A start clears any pending task, not only its own | `AtMostOnePending` | fails |
| A submit that finds a pending task doesn't publish again | `EverySubmitIsDone` | fails |
| A renewal skips the progress ack | passes | fails |
| A lost renewal takes the lease anyway | `NoRejectedCurrentHolder` | fails |
| The fence is the revision that the pod read, not the one that it wrote | `EverySubmitIsDone` | fails |
| A finished run releases a lease that it doesn't hold | passes | fails |
| The pending task clears at delivery, before the lease | passes | fails |
| A take of the lease ignores the lease rules | passes | fails |
| A give-back skips the negative ack | passes | fails |
| A renewal waits a fixed period from the start of the run, not until its renewal time | passes | fails |
| A failed run acks its task | passes | passes |

The checks catch 10 of 11, and only TLC catches 6 of them. The last one is a gap. The simulated worker fails only on a fence rejection. By then, a newer run has acked the same message, so the extra ack changes nothing. The error table of the next MR adds other worker errors, and then the spec needs a step for each one.

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
