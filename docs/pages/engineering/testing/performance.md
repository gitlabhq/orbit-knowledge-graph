---
title: Performance
description: How Orbit measures speed and scale.
order: 6
authors:
  - michaelangeloio
---

Performance tests load a synthetic graph and send concurrent
queries. Benchmarks measure code indexing on real repositories. Production
rollouts are the load test at full scale.

## Contracts

- Shared CI machines do not give stable timings. Benchmark and load-test
  timings report results, and they do not block a merge.
- The synthetic load test runs on the main branch. A merge request from this
  project that changes the query path gets a report as a comment.
- Every performance scenario must compile in the integration tests, so a
  broken scenario fails a merge.
- Code-indexing benchmarks use pinned revisions of real repositories and run
  outside CI.
- The query profiler compiles a query the way production does. It reports
  rows and bytes read, time for each stage, and the query plan.
- The reference architecture bench deploys Orbit on its own cluster and
  replays a production data dump. It checks a hardware size against the
  service level objectives, outside CI.

## Production rollouts

- A schema change that rebuilds the graph re-indexes the real workload, so
  each such rollout is a load test.
- Each rollout records the same measurements: migration start and end,
  re-index time, write throughput, and time to full coverage. The
  [production rollout issue](https://gitlab.com/gitlab-com/gl-infra/production/-/work_items/21860)
  holds the reports. Compare a rollout with the last report to find a
  regression.
- Some failures occur only at production scale, such as disk evictions,
  merge backpressure, memory limits on joins, and queue growth.
