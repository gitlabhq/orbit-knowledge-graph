---
title: Performance
description: How Orbit measures query speed, indexing speed, and scale.
order: 6
authors:
  - vishal.s.patel
---

## What performance tests prove

Performance tests show how fast Orbit answers, and how much work the graph store does for it.
**Performance results report, and they do not block a merge.**

Shared CI machines do not give stable timings, so a timing threshold would fail at random.
The merge gates check results and query plans instead, at the [integration layers](../integration/README.md).

## The performance tools

| Tool | Question | Where it runs |
| ---- | -------- | ------------- |
| [Load test](load-test.md) | How fast does each query shape answer under concurrent load? | CI, after each merge to main and on merge requests that change the query path |
| Query profiler | Why is one query slow? It reports rows and bytes read, time for each stage, and the query plan. | On demand |
| Code-indexing benchmark | How fast does code indexing run on pinned revisions of real repositories? | On demand |
| Reference architecture bench | Does a hardware size meet the service level objectives with production-sized data and real indexing? | On demand, on its own cluster |
| Production rollouts | How does Orbit behave on the real workload? | Each rollout that rebuilds the graph |

Every load test scenario also compiles against the ontology in the integration tests, so a schema change cannot leave a stale scenario.

## Production rollouts

A schema change that rebuilds the graph reindexes the real workload, so each such rollout is a load test.
Each rollout records the migration start and end, the reindex time, the write throughput, and the time to full coverage in the [production rollout issue](https://gitlab.com/gitlab-com/gl-infra/production/-/work_items/21860).
Some failures occur only at production scale, such as disk evictions, merge backpressure, memory limits on joins, and queue growth.

The commands for each tool are in [Testing commands](../../development/testing-commands.md).
