# Indexing engine spec

## Summary

This folder holds the spec of the lease and dedupe protocol of ADR 023. We write it in Quint, a typed language that reads like code, and CI compiles it to TLA+. TLC checks every order of events in the spec, with pod faults and NATS faults. It also checks that every acked submit ends done once faults stop. A second check proves that the Rust code has the same state graph as the spec in 4 small clusters. A third check runs the real runner in a seeded simulator. Quint replays each run: every step must be the same action in the spec, and the state after it must match. A fourth check runs real pods on a NATS server through a crash and a wipe. For each key, TLC must find a behavior of the spec that explains every step at the time it happened.

TLC found 2 races in the dedupe rules, a NATS fault that loses work, and one gap after a wipe. Two fixes close the races, and we keep 2 cases open on purpose. The spec has no fence: workers write rows versioned by their source, so a rare overlap of two runs is harmless. The code matches the spec in 2.5 million states and 6 million steps.

## What we built

```text
                          ┌──────────────────────────────┐
                          │ leaseDedupeProtocol.qnt      │
                          │ the rules, compiled to TLA+  │
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
| `quint/leaseDedupeProtocol.qnt` | The spec: the state, the actions and the rules |
| `LeaseDedupe.cfg`, `LeaseDedupe_ThreePods.cfg` | The model checks with 2 and 3 pods |
| `quint/leaseDedupeLiveness.qnt`, `liveness/` | The liveness check and its 4 configs |
| `counterexamples/`, `liveness/counterexamples/` | 8 configs that must break a rule |
| `code-check/` | The 4 small clusters of the code check |
| `src/lease/rules.rs` in the crate | The lease rules as code with no IO. The runner calls them. |
| `src/testkit/simulation/` in the crate | A simulated tick clock, key-value store and broker, with the ports on one seeded world |
| `tests/protocol/` in the crate | A simulated cluster with one method for each spec action |
| `src/testkit/simulation/trace.rs` in the crate | The trace check: it writes each recorded run as a Quint test |
| `src/ports.rs` in the crate | The ports that an adapter implements: clock, task queue, lease store, pending store, protocol log. Each port trait lives with its concept. |
| `src/runner/`, `src/lease/store.rs`, `src/pending.rs` in the crate | The runner and the submit path, written against the ports |
| `tests/simulation.rs` in the crate | The rules of the simulated parts, the seeds, the coverage of each fault, and the trace writer for TLC |

## Why

We want two rules to hold, whatever happens to pods and to NATS:

- At most one task of a key waits in the queue. This is the pending task.
- Every acked submit runs once faults stop. This is the liveness check.

The lease keeps two runs of one key apart most of the time. A stall or a wipe can still let two runs overlap. We accept that, because workers write rows versioned by their source: the newest data wins, whichever run writes last.

Pods stall, crash and see different clocks. NATS redelivers, delivers twice, loses messages and gets wiped. Tests that we write by hand rarely hit the bad orders of events. A model checker tries all of them.

A proof of the spec says nothing about our code, though. So the code check ties the two together. If the code drifts from the spec, CI fails and shows the states that differ.

The code check covers the decisions in `src/lease/rules.rs`. It doesn't cover the runner that calls them. That loop sets the order of the reads and the writes. It renews the lease next to the worker, and it acks and releases. The trace check covers that code. It runs the real runner, and Quint replays each step that the loop logs.

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
| Port contracts | The simulated ports and the NATS adapters pass the same contracts: the CAS on a version, a write after a wipe, one copy for a live task ID, the ack, the ack wait, a negative ack with a delay and a progress ack |
| NATS trace check | Real pods on NATS go through a crash and a wipe. For each key, TLC finds a behavior of the spec that explains every step, within 1 unit of its real time, in the order of each pod and on the right side of the wipe |

Within the bounds of the 4 clusters, the code takes no step that the spec forbids. It also refuses no step that the spec allows. Beyond those bounds, we rely on the model check of the spec. Most protocol bugs show up with few pods.

What it doesn't prove:

- We trust the Quint compiler, its TypeScript evaluator, and the 2 Apalache modules. Before the switch, the Quint spec and the hand-written TLA+ spec gave the same state counts in all 18 configs.
- The port contracts check the NATS behaviors that the spec relies on, but not all of them. NATS must not lose an acked message, so we run it with 3 replicas. A negative ack of a message that NATS already delivered again is the worst case of the give-back, and the spec allows it without a contract.
- A wipe and a duplicate together give too many states to compare as graphs.
- Each pod has one slot, so the model can't show one pod with two runs of a key. A unit test covers the holder check for that case, before and after a wipe.
- The spec doesn't check writes. A flow must write rows versioned by their source. A flow that versions rows by the time of the write can lose newer rows to a late run, until the next run of the key.
- A pod reads the lease and writes it in one step. In NATS, a wipe can come between the read and the write, and the revisions start again at 1. A stale write can then take the lease beside another run. We accept that rare overlap for the same reason.
- The spec drops a republish while the task has a live copy. NATS drops a message ID only for 2 min after the first publish. A later republish stores a second copy, and the spec covers that only as its duplicate fault, at most once.
- The liveness check needs faults to stop at some point. A cluster that keeps failing forever makes no progress, and no protocol can fix that.
- The recovery campaign, the sweeps and the reconcilers aren't in the spec. The spec hands their work to them and stops there.
- The trace check samples runs. It doesn't explore every order of events, as the code check does. Between the first and last step of a run, it checks only the variables that changed, so a stray change in the spec shows up at a later step or at the end.
- In the simulator, each step of the runner is atomic in time. A stall can come between two steps, but not between the clock read and the write of one step. In production, a pod can stall there. Then the lease that it writes expires earlier than the spec says, and two runs can overlap.
- Interleaved runs let other steps run inside a step, so a CAS write can lose its race. They check the rules and the liveness in Rust, but TLC can't follow them.
- The simulator has one key, one lane, one slot for each process and one planner, the same shape as the spec. A worker that isn't exclusive skips the lease, and no check covers that path yet.
- The simulator replaces NATS with the simulated parts in `src/testkit/simulation/`. The NATS adapters live in `src/nats/`. The NATS trace check sees only the steps that the runner reports, one key at a time. TLC fills in the ticks, the redeliveries and the work of each run, and the order between pods is free as long as each pod keeps its own. One unit is 500 ms, and a step may sit 1 unit off its real time.

## How it works

### The model

The model has one key and 2 or 3 pods with one slot each. One time unit is 20 s. The lease lasts 3 units, a pod renews it every unit, and the skew margin and the ack wait are 1 and 2 units. Each pod has a clock offset of 0 or 1.

A pod that finds the lease of its key held doesn't wait for it. It gives the message back to the broker with a delay of 1 unit, so its slot is free for other work. The broker delivers the message again after the delay. The liveness configs use a delay of 0, because there the clock jumps past every timer once faults stop.

A wipe empties the key-value store and the broker, and the revisions start again at 1. In NATS, a pod whose task stream is gone stops, and a restart creates the stream and the bucket again.

### The liveness check

The spec bounds time, so a lease could stay open forever once time stops. `quint/leaseDedupeLiveness.qnt` adds one step. Faults stop, and the clock jumps past every expiry and every ack wait. From then on, each engine step that stays possible happens in the end. TLC then checks that every acked submit ends done, or lost in a wipe and handed to the recovery campaign.

### The code check

```text
compiled spec + code-check/<cluster>.cfg            tests/protocol/cluster.rs
                │                                             │
       TLC explores every state                 Rust explores every state
       and dumps the graph                      and records every step
                │                                             │
                ▼                                             ▼
        initial states, states, steps   ==   initial states, states, steps
```

1. The cluster wires the simulator to the protocol code. Each spec action is one method with the same name. The simulator plays the environment, and the protocol code makes every decision about the lease and the pending task.
2. Rust starts from every initial state and tries every action in every state. It prints each state the way TLC prints it.
3. TLC model-checks the spec with the same bounds and dumps its graph as a DOT file.
4. The test compares the initial states, the states and the steps. A state or a step that only one side has fails the test, with the states that differ.

Equal steps means every state has the same next states on both sides.

### The trace check

```text
tests/simulation.rs                 src/testkit/simulation/             quint test
        │                                     │                                    │
 300 seeds for each scenario     real runners on simulated          each run starts at initWith,
        │                        ports; each step and the           then takes each action and
        ▼                        state after it are written         checks the state after it
  run(seed) ───────────────────► as a Quint run ──────────────────► a disabled action or a wrong
                                                                     state fails the run
```

1. The runner talks to the outside only through the ports in `src/ports.rs`. Each step reads the clock once at its start and logs one protocol event with that time.
2. The simulator implements the ports on one world: a simulated key-value store and broker. It runs 2 or 3 real runners in one process on a paused clock. A seed picks the order of the steps, the faults and their targets.
3. After each step, the world records the spec action with its arguments, for example `Deliver("p1", 0)`, and the state in the variables of the spec. The pod state comes from the protocol events, and the rest comes from the simulated stores.
4. The trace writer turns each run into a Quint test. A run checks the full state at its first and last step, and the changed variables after each step in between. `quint test` fails a run when an action isn't enabled or a state differs.

A time unit of the spec is one tick of 20 s, and each step happens on a tick. A process that crashes stops at once: its old tasks can't take another step. When a rule breaks, every process stops, so the timeline ends at the bug. Too many port calls in one tick also break a rule. So a busy loop fails the run and doesn't hang it.

## What is tested

### Rules

TLC checks these rules in every state:

| Rule | Meaning |
|---|---|
| `AtMostOnePending` | Only the pending task waits in the queue |
| `EverySubmitCovered` | Every acked submit has a task that started, a live message, or a publish in progress |

The liveness check adds 2 properties:

| Property | Meaning |
|---|---|
| `EverySubmitIsDone` | Every acked submit ends done, or lost in a wipe and handed to the recovery campaign |
| `PodsEndIdle` | Every pod ends idle |

| Liveness config | Faults before they stop | States |
|---|---|---|
| `Liveness` | 1 stall, 1 crash | 111,752 |
| `Liveness_TwoTasks` | 2 tasks, 1 crash | 1,632,294 |
| `Liveness_NatsWipe` | 1 stall, 1 crash, 1 wipe | 559,872 |
| `Liveness_Duplicate` | 1 crash, 1 duplicate | 2,701,442 |

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
| `no_nats_faults` | 1 stall, 1 crash | 80,708 | 184,608 |
| `two_tasks` | 2 tasks, 1 crash | 820,990 | 1,708,614 |
| `nats_wipe` | 1 stall, 1 crash, 1 wipe | 540,782 | 1,328,456 |
| `duplicate` | 1 stall, 1 crash, 1 duplicate | 1,087,588 | 2,789,916 |

### Counterexamples

Each config switches one fix off, or adds one fault, and must break a rule:

| Config | Breaks | Trace |
|---|---|---|
| `AsWritten_DoublePending` | `AtMostOnePending` | A redelivered task starts and clears a newer pending task. |
| `AsWritten_LostSubmit` | `EverySubmitCovered` | The planner crashes after the pending write and before the publish. Every later submit of the key is dropped. |
| `NatsLosesMessage` | `EverySubmitCovered` | NATS loses an acked message, and nothing brings it back. |
| `liveness/counterexamples/AsWritten_LostSubmit` | `EverySubmitIsDone` | Without F3, a stranded pending task swallows a submit, and its task never runs. |

## Run it

| Command | What it does | Time on a laptop |
|---|---|---|
| `mise run spec:engine:compile` | Compiles the Quint spec to TLA+ in `target/indexing-engine-spec`. Every task below runs it first. | about 10 s |
| `mise run spec:engine` | Model-checks the spec with both configs | about 7 min |
| `mise run spec:engine:liveness` | Checks that every acked submit ends done | about 14 min |
| `mise run spec:engine:counterexample` | Checks that each counterexample still breaks a rule | about 50 s |
| `mise run spec:engine:code` | Compares the code and the spec in all 4 clusters | about 10 min |
| `mise run spec:engine:trace` | Runs 600 seeds of the runner and replays each step with Quint, one file of 25 seeds for each CPU core | about 2 min |
| `cargo test -p indexing-engine-next --test simulation` | Runs 1,200 seeds and checks the rules, the liveness and the coverage of each fault | about 10 s |
| `cargo test -p indexing-engine-next --test protocol` | Runs the rule tests and the invariants of the first cluster | about 10 s |
| `mise run test:engine:nats` | Runs the port contracts and the crash and wipe scenario on a NATS container, or on `ENGINE_TEST_NATS_URL` | about 30 s |
| `mise run spec:engine:nats-trace` | Runs the crash and wipe scenario on NATS, then asks TLC to explain the steps of each key | about 20 s |

`ENGINE_SPEC_CLUSTER=two_tasks mise run spec:engine:code` checks one cluster. The mise tasks download a pinned `tla2tools.jar` into `target/` and install the Java version that they pin. The compile task runs Quint 0.34.0 with `bunx`. On its first run, Quint downloads Apalache into `~/.quint`, and the task copies the 2 TLA+ modules that the compiled spec needs from it.

In CI, the jobs live in `.gitlab/ci/indexing-engine-spec.yml`. The model check, the liveness check and the counterexamples run when the spec changes. The code check runs one job for each cluster, in parallel, when the spec, the protocol code or the simulator changes. The trace check runs when the spec or any code of the crate changes. The `unit-test` job runs the tests of the crate on every pipeline, the protocol and simulation tests included.

## When you change something

- A rule in the spec: change `quint/leaseDedupeProtocol.qnt` and the protocol code in the same MR. The code check fails until both match.
- Never edit the compiled TLA+. It lives in `target/`, and the next compile replaces it.
- Quint lists start at 0. A TLA+ module that picks a copy by index uses `0..(Len(copies) - 1)`, as the liveness module does.
- A rule in the code: the code check fails and shows the states that differ. Change the spec too, or fix the code.
- A new variable in the spec: add it to `state_lines` in `tests/protocol/cluster/tlc.rs`.
- A new action in the spec: add it to the `Action` enum and to the cluster. Until then, the spec has steps that the code lacks, and the code check fails.
- A new fault: add a budget to `FaultBudget`, an action to the spec and to the cluster, and a counterexample if it breaks a rule.
- A new step in the runner: read the clock at its start, and log one protocol event at its end. Then map the event to the spec state in `World::apply_protocol_event`. Until the spec has the step, the trace check fails.

## Results

### What TLC found

The counterexamples above show each trace. We changed the protocol in 2 ways:

```text
race                                   fix
────────────────────────────────────   ──────────────────────────────────────────
start clears a newer task's pending →  F2  the pending entry holds its task ID
crash strands the pending entry     →  F3  a submit republishes the found task
```

1. F2, a pending entry that the task owns. The entry holds the task ID, and a start clears it only if it still holds that ID.
2. F3, a republish. A submit that finds a pending entry publishes that task again, with the task ID as the message ID.

TLC also found that a stalled pod can write after a newer run, and that a wipe sends the revisions back to 1. A fence fixes both (F1 and F4 in earlier versions), but it costs a generation in NATS and a rule for every flow. Rows versioned by their source make those late writes harmless, so we dropped the fence.

The liveness check found one more gap. A task that started before a wipe loses its message. If its pod then crashes, nothing runs it again. So a wipe hands every task that isn't done to the recovery campaign, not only the pending ones. The recovery campaign of ADR 023 must replan those keys.

Two cases stay open on purpose:

- A submit in flight during a wipe can leave one extra pending task. The recovery campaign of ADR 023 owns it, and the key runs once more.
- A lost acked message is an assumption about NATS. Jepsen found lost writes in NATS 2.12.1 under file corruption and power loss. So we run NATS with 3 replicas and `sync_interval` set to always.

### What the code check found

- In the version with fences, we broke 12 rules in the protocol code one at a time. The code check caught 11, and a unit test caught the 12th.
- A first version had 1 task in each cluster, and it missed a broken pending rule. That's why `two_tasks` exists.
- 20,000 random runs found nothing. Each 1,000 runs added about 45,000 new states, so random runs never cover the space. We check every state of small clusters instead.

### What the trace check found

The first trace check, in TLC, found a bug in the simulator before it passed. A crashed process could still take one step, because its old tasks stopped only later. TLC stopped at a write from a process that the spec had made idle. Now each port call of an old incarnation blocks for good.

The coverage test found 2 more gaps. After a stall, the renewal always ran before the write, so no seed reached the late write of a stalled run. Now a process yields at random each time it wakes up, and half of the stalls wait for the end of the work. A seed also hung when the loop made calls with no pause. Now too many port calls in one tick break a rule, so the run ends with a timeline and doesn't hang.

In the version with fences, we broke the runner in 11 ways, one at a time:

| Broken rule in the runner | Rust rules | Trace check |
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

The checks catch 10 of 11, and only the trace check catches 6 of them. The 2 give-back rows ran against the Quint trace check. The other rows ran against the earlier TLC trace check. That check accepts any step of the spec, while the Quint check needs the exact action. But the Quint check compares the variables that didn't change only at the first and last step of a run. The last one is a gap. Without fences, the simulated worker never fails. The error table of a later MR adds worker errors, and then the spec needs a step for each one.

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
