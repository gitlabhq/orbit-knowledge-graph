# OrbitDispatchTaskStopped

## Overview

A dispatcher task that runs every few minutes did not complete once in 30 minutes. The
alert watches these tasks:

- `dispatch.sdlc.namespace`: sends SDLC indexing requests for namespaces.
- `dispatch.sdlc.global`: sends SDLC indexing requests for global entities.
- `dispatch.siphon`: turns datalake changes into code indexing tasks.
- `namespace_freshness`: records the SDLC freshness samples.

When one of them stops, indexing requests or freshness samples stop, and graph data goes
stale. No paging, posts to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

`gkg_scheduler_task_runs_total{task, outcome}` from the dispatcher pods. The alert fires
when a task has no `outcome="success"` run in 30 minutes. A task that fails on every run
also fires it. When the dispatcher stops exporting at all, the generated
`OrbitServiceGkgDispatcherTrafficAbsent` alert fires instead.

## Verification

```promql
sum by (task, outcome) (increase(gkg_scheduler_task_runs_total{env="orbit-prd"}[30m]))
```

- Logs: [Kibana, orbit index](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter
  `json.mode: dispatch-indexing` and the message `task failed` or `cadence lock check failed`.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Runs with `outcome="error"` | The task query or publish fails (datalake, graph ClickHouse or NATS) | Read the `task failed` log lines for the error |
| No runs at all, pods are running | Cadence lock in NATS KV is stuck or unreachable | Check NATS, then restart the dispatcher |
| No runs, pods are not running | Dispatcher down | Check the `orbit-dispatcher` pod ([Order of checks](../troubleshooting.md#order-of-checks)). If it is OOM killed, give it more memory ([Capacity](../maintenance.md#capacity)) |

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
