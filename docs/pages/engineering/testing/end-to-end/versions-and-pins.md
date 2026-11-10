---
title: Versions and pins
description: How the end-to-end suites cover each GitLab version, keep component versions fixed, and survive ClickHouse replica failures.
order: 4
authors:
  - bohdanpk
  - michaelangeloio
  - jgdoyon1
---

## GitLab version lanes

GitLab.com runs the latest GitLab code, and GitLab Self-Managed and GitLab Dedicated run released versions.
**The same Orbit release must work with all of them.**

```mermaid
flowchart LR
    accTitle: One Orbit commit against each GitLab version
    accDescr: One Orbit commit deploys one stack for each GitLab version lane. The same suites run on each stack, and the commit must pass on every version.
    Commit[Orbit commit] --> Default[Stack with the GitLab default branch]
    Commit --> Lane1[Stack with a released GitLab]
    Commit --> Lane2[Stack with an older released GitLab]
    Default --> Suites[Same suites]
    Lane1 --> Suites
    Lane2 --> Suites
```

The main job tests against the GitLab default branch, which matches GitLab.com.
A second job runs the same suites against each supported released version, which we call a lane.
Each lane has its own GitLab chart, commit, and image digests, and Siphon replicates the tables that its GitLab version defines.
To support a new GitLab version, we add a lane.

## Pins

The stack pins each component that Orbit does not build: GitLab, Siphon, ClickHouse, the message broker, and the databases.
Then a failure cannot come from a change that nobody made in Orbit.
GitLab images are pinned to a digest, because a moving tag can let two GitLab pods run different code.
No run deploys a pinned Orbit image. A CI run tests the Orbit image of its own commit.

```mermaid
flowchart LR
    accTitle: The pin bump
    accDescr: A schedule starts the pin bump. The job finds the newest builds and pushes the new pins to one rolling merge request. The end-to-end tests run on that merge request and block it until they pass.
    Schedule --> Bump[Find the newest builds] --> MR[Rolling merge request] --> E2E[End-to-end tests] --> Merge
```

A scheduled job moves the pins forward.
It finds the newest GitLab, Siphon, and Orbit builds, and publishes the replicated tables for the new GitLab commit.
Then it pushes the pins to one rolling merge request.
The main end-to-end job **blocks that merge request until it passes**, so a GitLab change that breaks Orbit shows up before a release.

A lane can be pinned by hand, for example to images from a GitLab stable branch that holds an Orbit backport.
The scheduled job skips a hand-pinned lane. Today every lane is hand-pinned, so the job moves only the default-branch pins.

A CI check compares the redaction settings in the ontology with the GitLab permission policies of the pinned commit.
A pin that renames or removes an ability fails the check before it merges.

## ClickHouse HA variant

Production ClickHouse runs with replicas.
The ClickHouse HA variant runs the same suites on a replicated ClickHouse cluster and stops replicas during the run:

```mermaid
sequenceDiagram
    accTitle: Replica failures during the ClickHouse HA run
    accDescr: At the end of setup, the run stops the first replica and waits until it catches up. After the smoke suite passes, the run stops the second replica while the parallel suites write data. Every test must still pass.
    participant S as Setup and suites
    participant R1 as Replica 1
    participant R2 as Replica 2
    S->>R1: Stop at the end of setup
    R1-->>S: Restarts and catches up
    S->>S: Smoke suite passes, parallel suites start
    S->>R2: Stop during the parallel suites
    R2-->>S: Restarts and catches up
```

**Every test must still pass, and each replica must catch up in a time limit.**

Replicated writes are slower, so this variant multiplies each wait budget.
