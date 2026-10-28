---
title: "GKG ADR 009: Separate code indexer service"
creation-date: "2026-04-22"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-04-22

## Context

The indexer process runs three kinds of handlers in one engine.
The handlers do software development lifecycle (SDLC) indexing, code indexing, and namespace deletion.
They share one NATS consumer loop, one worker pool, and one set of resource limits.

Code indexing and SDLC indexing use resources in different ways:

| Characteristic | SDLC indexing | Code indexing |
|---|---|---|
| CPU | Low, SQL transforms on ClickHouse data | High, source parsing and data-flow analysis |
| Memory | Moderate, columnar batches | High, full repository archives and syntax trees in memory |
| Disk | None | Large scratch space for archive extraction |
| Network | ClickHouse queries | Large archive downloads from Rails, ClickHouse writes |
| Concurrency pattern | Many small fast tasks | Few large slow tasks |
| External dependencies | ClickHouse data lake and graph | GitLab Rails API and ClickHouse graph |
| Failure blast radius | A slow namespace blocks its concurrency group | A large repository runs the pod out of memory, and the SDLC handlers stop with it |

The Helm chart sizes the indexer pod for the worst case, which is code indexing.
SDLC indexing alone needs a fraction of those resources.

This shared process causes four problems:

- Scaling is coupled.
  More code indexing capacity means more SDLC pods too, and the SDLC side wastes resources.
- Faults are not isolated.
  When code indexing runs out of memory, the pod stops, and in-flight SDLC work stops with it.
  NATS redelivers both, but the disruption has no purpose.
- Deployments are coupled.
  A parser change, such as a new language, needs a redeploy of the same binary that runs SDLC indexing.
  A parser regression then affects SDLC availability.
- Tuning is coupled.
  One configuration holds all concurrency groups.
  A change to code indexing parallelism needs a redeploy of the combined indexer.

## Decision

**We split code indexing into a dedicated code indexer service.**

The code indexer is a new run mode of the same server binary.
The existing indexer mode keeps only SDLC indexing and namespace deletion.

The code indexer connects to NATS, to the ClickHouse graph database, and to the GitLab Rails API for archive downloads.
It does not connect to the data lake.
It runs the same schema migration check as the indexer, and it registers only the code handlers.
It has its own engine configuration, with concurrency groups sized for code work.

The indexer stops registering the code handlers.
Operators can deploy the indexer before the code indexer exists.
For this transition, a configuration flag can keep code indexing in the indexer until the migration is complete.
We remove the flag after the migration.

The Helm chart gets a separate code indexer deployment with these properties:

- Resource limits for code workloads, with high CPU, high memory, and scratch storage for archives
- A replica count that is independent of the indexer
- Its own concurrency groups, with fewer workers and more resources for each worker
- The scratch volume, which moves from the indexer
- The same secret mounts as the current indexer

The indexer deployment then drops the scratch volume and lowers its resource limits.
It also loses the Rails configuration and the Rails signing key.
SDLC indexing does not call Rails.
The health-check service also monitors the new deployment, together with the indexer and the dispatcher.

These parts do not change:

- Dispatch.
  The dispatcher already publishes code tasks independently of the consumer.
- NATS subjects.
  Code tasks and SDLC requests already use separate subjects, so routing does not change.
- Namespace deletion.
  Deletion removes rows from all ontology tables, including code tables.
  It is a light ClickHouse delete and does not need code resources, so it stays in the indexer.
- Schema migration coordination.
  Both services check the schema version on startup in the same way.
  The dispatcher triggers code backfill during a migration, and the code indexer consumes those messages.
- The code indexing module.
  The module is already self-contained, so it needs no structural change.

## Consequences

Code and SDLC indexing scale independently.
Operators can run many code indexer replicas and few SDLC indexer replicas, based on the workload.
When code indexing runs out of memory, SDLC work continues.
Each workload gets resource limits that fit it.
SDLC pods drop to a fraction of the old memory limit, and code pods keep the high limits.
Parser changes deploy and test on the code indexer without an effect on SDLC indexing.
Monitoring and alerts can separate code indexing health from SDLC indexing health at the pod level.

The change also has costs:

- Operators manage one more deployment, and the Helm values grow by one top-level section.
- During the transition, operators deploy the updated indexer and the new code indexer.
  The configuration flag on the indexer reduces this risk.
- The health-check targets need an update.
- Three handler groups in two deployments share one NATS stream.
  NATS consumer groups already support this, but operators must make sure that consumer names do not collide.

## Why not the alternatives

### Feature flag on the existing indexer

A configuration list of modules could let operators choose the handlers for each pod.
This works, but it puts deployment topology into application configuration.
A separate run mode keeps the deployment intent explicit.
It also matches the existing pattern of one mode for each of the webserver, indexer, dispatcher, and health check.

### Separate binary

A separate binary gives the strongest isolation.
The code indexer shares the engine, the handler contract, the NATS consumer, the ClickHouse client, schema migration, and metrics with the SDLC indexer.
To duplicate that, or to move it into a shared library, adds build complexity for a small isolation gain.
A new mode on the same binary is consistent with the existing architecture.

### Move code indexing to a different crate

This choice is independent of the service split.
Crate boundaries are a compile-time concern, and mode selection is a runtime concern.
The code module is already isolated inside the indexer crate.
If compile times become a problem, we can move the module to its own crate later.

## References

- [Code indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/code_indexing.md)
- [SDLC indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/sdlc_indexing.md)
