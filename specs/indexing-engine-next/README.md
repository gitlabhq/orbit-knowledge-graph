# Indexing engine spec

This folder holds a TLA+ model of the lease and dedupe protocol of ADR 023. It also holds the configs of the check that ties the Rust code to the model.

We want two rules to hold, whatever happens to pods and to NATS:

- One run of a key at a time writes to the database. This is the lease.
- At most one task of a key waits in the queue. This is the marker.

Pods stall, crash and see different clocks. NATS redelivers, delivers twice, loses messages and gets wiped. Tests that we write by hand rarely hit the bad orders of events. A model checker tries all of them.

## What is here

| Path | What it holds |
|---|---|
| `LeaseDedupe.tla` | The spec: the state, the actions, and the rules that must hold |
| `LeaseDedupe.cfg` | The model check with 2 pods, 3.8M states |
| `LeaseDedupe_ThreePods.cfg` | The model check with 3 pods, 45.2M states |
| `counterexamples/` | 7 configs that must break a rule |
| `code-check/` | 4 small clusters that the code check compares |

The Rust side lives in the `indexing-engine-next` crate:

| Path | What it holds |
|---|---|
| `src/protocol/` | The rules as code with no IO |
| `src/testkit/simulator/` | The simulated clock, key-value store, broker and database |
| `tests/protocol/cluster.rs` | A simulated cluster with one method for each spec action |
| `tests/protocol/cluster/tlc.rs` | The state in the format of TLC, and the reader of the TLC graph |

## The model

The model has one key and 2 or 3 pods with one slot each. One time unit is 20 s. The lease lasts 3 units, a pod renews it every unit, and the skew margin and the ack wait are 1 and 2 units. Each pod has a clock offset of 0 or 1.

Pod faults are a stall of any length, a crash, a failed renewal and a planner crash. NATS faults are a wipe, a duplicate delivery and a lost message. A wipe empties the key-value store and the broker. The database survives it. After a wipe, a recovery reads the generation from the database, adds 1, and writes it to the key-value store.

TLC checks these rules in every state:

| Rule | Meaning |
|---|---|
| `NoStaleWrite` | The database never keeps a write of an older lease after a newer one |
| `NoRejectedCurrentHolder` | The database never rejects the write of the pod that holds the current lease |
| `AtMostOnePending` | Only the task in the marker waits in the queue |
| `EverySubmitCovered` | Every acked submit has a task that started, a live message, or a publish in progress |

## The fixes

The rules as ADR 023 wrote them break in 5 ways. The configs in `counterexamples/` show each trace, and 2 more show the NATS cases. Four fixes make the model pass:

1. A fence. A pod writes with the lease revision from when it took the lease. The database keeps a write only if its fence is the highest it has seen.
2. A marker that the task owns. The marker holds the task ID, and a start clears it only if it still holds that ID.
3. A republish. A submit that finds a marker publishes that task again, with the task ID as the message ID.
4. A generation in the fence. A wipe restarts the revisions at 1, so the fence is the pair of the generation and the revision. No pod takes a lease until the recovery wrote the new generation.

Two cases stay open on purpose. A submit in flight during a wipe can leave one extra pending task. The recovery campaign of ADR 023 owns it, and the key runs once more. A lost acked message is an assumption about NATS. Jepsen found lost writes in NATS 2.12.1 under file corruption and power loss. So we run NATS with 3 replicas and `sync_interval` set to always.

## How the code check works

A proof of the spec says nothing about our code. So we check that the code has the same state graph as the spec.

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

Equal steps means every state has the same next states on both sides. So the code takes no step that the spec forbids, and it refuses no step that the spec allows.

The 4 clusters have 2 pods each:

| Cluster | Faults | States | Steps |
|---|---|---|---|
| `no_nats_faults` | 1 stall, 1 crash | 95,156 | 198,844 |
| `two_tasks` | 2 tasks, 1 crash | 865,748 | 1,551,818 |
| `nats_wipe` | 1 stall, 1 crash, 1 wipe | 1,073,592 | 2,686,400 |
| `duplicate` | 1 stall, 1 crash, 1 duplicate | 1,023,506 | 2,250,220 |

## Run it

| Command | What it does | Time on a laptop |
|---|---|---|
| `mise run spec:engine` | Model-checks the spec with both configs | about 3 min |
| `mise run spec:engine:counterexample` | Checks that each counterexample still breaks a rule | about 25 s |
| `mise run spec:engine:code` | Compares the code and the spec in all 4 clusters | about 6 min |
| `cargo test -p indexing-engine-next --test protocol` | Runs the rule tests and the invariants of the first cluster | about 10 s |

`ENGINE_SPEC_CLUSTER=two_tasks mise run spec:engine:code` checks one cluster. The mise tasks download a pinned `tla2tools.jar` into `target/` and install the Java version that they pin.

## CI

The jobs live in `.gitlab/ci/indexing-engine-spec.yml`. The model check and the counterexamples run when the spec changes. The graph check runs one job for each cluster, in parallel, when the spec, the protocol code or the simulator changes. The slowest job takes about 6 min.

## When you change something

- A rule in the spec: change the protocol code in the same MR. The graph check fails until both match.
- A rule in the code: the graph check fails and shows the states that differ. Change the spec too, or fix the code.
- A new variable in the spec: add it to `state_lines` in `tests/protocol/cluster/tlc.rs`.
- A new action in the spec: add it to the `Action` enum and to the cluster. Until then, the spec has steps that the code lacks, and the graph check fails.
- A new fault: add a budget to `FaultBudget`, an action to the spec and to the cluster, and a counterexample if it breaks a rule.

## What it proves and what it doesn't

Within the bounds of the 4 clusters, the code and the spec make exactly the same moves. TLC proves that the rules hold in every state. Beyond those bounds, we rely on the model check of the spec. Most protocol bugs show up with few pods.

These stay outside the check:

- The NATS behaviors are assumptions. A probe against a local NATS 2.12.4 confirmed the 11 that the spec uses. The integration tests of 1b will check them in CI.
- A wipe and a duplicate together give 11M states or more, too many to compare as graphs.
- Each pod has one slot, so the model can't show one pod with two runs of a key. A unit test covers the holder check for that case.
- The model checks safety only. It doesn't check that every task runs in the end.

## Further reading

- [Validating Traces of Distributed Programs Against TLA+ Specifications](https://arxiv.org/abs/2404.16075)
- [Smart Casual Verification of the Confidential Consortium Framework](https://www.usenix.org/conference/nsdi25/presentation/howard)
- [Jepsen: NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1)
