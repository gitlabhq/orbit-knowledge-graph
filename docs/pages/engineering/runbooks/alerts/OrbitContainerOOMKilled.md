# OrbitContainerOOMKilled

An Orbit container was OOM killed; its memory limit is too small for the
workload. Fires after `1m`.

## Overview

The kernel OOM killer terminated an Orbit container because it exceeded its
memory limit. One OOM kill loses the work of that pod: running queries on the
webserver, or a running indexing job on an indexer. Repeated kills reduce
throughput. The indexers are the usual suspects:
indexing a very large repository can spike memory well above the
steady-state usage the limits were tuned for.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

The firing expression counts OOM kill events from cAdvisor
(`container_oom_events_total`, exposed by the kubelet on every cluster):

```promql
sum by (env, environment, stage, container) (
  increase(container_oom_events_total{container=~"orbit-.*"}[10m])
) > 0
```

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D) filtered on the alertname.
- A single OOM kill fires the alert and it auto-resolves roughly 10 minutes
  after the event. Sustained kills keep it firing continuously.
- The `container` label identifies which workload was killed
  (`orbit-indexer`, `orbit-webserver`, `orbit-dispatcher`, ...; `gkg-*` before the
  container rename).

## Severities

- No paging. A one-off kill is disruptive but self-healing; sustained
  kills degrade indexing throughput or query availability and will surface
  through the SLO alerts.

## Verification

Find the affected pods and the kill rate:

```promql
sum by (environment, pod) (increase(container_oom_events_total{container=~"orbit-.*"}[1h])) > 0
```

Cross-check with kube-state-metrics which pods most recently terminated
with an OOM:

```promql
kube_pod_container_status_last_terminated_reason{container=~"orbit-.*", reason="OOMKilled"} > 0
```

Compare memory usage against the limit to see how close to the ceiling the
workload runs:

```promql
container_memory_rss:labeled{type="orbit", container!="", container!="POD"}
/ on (environment, cluster, pod, container) group_left ()
kube_pod_container_resource_limits:labeled{type="orbit", resource="memory"}
```

Cross-check the `kube_container_rss_limit` saturation panel on the
[orbit service dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd).

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged) touching `services/gkg` (a merge is a deploy).
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| One indexer pod killed once, then stable | A single very large repository spiked memory | Usually self-healing; check the indexer picked the job back up |
| The same container killed repeatedly | Memory limit below actual demand | Raise the limit in the Helm values via argocd-apps ([Capacity](../maintenance.md#capacity)) |
| Kills started right after a deploy | New version uses more memory | Roll back the image tag ([Rollback](../maintenance.md#rollback)), then investigate the regression |
| Webserver containers killed under load | Query workload exceeding the limit | Raise the limit or find the offending query pattern in the logs |

## Possible Resolutions

- Raise the container memory limit for the affected deployment in the
  [argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps) values for
  `services/gkg`.
- If the kills start with a fresh deploy, roll back the image tag. Report the
  memory growth as a regression in the
  [knowledge-graph](https://gitlab.com/gitlab-org/orbit/knowledge-graph) repo.
- If a single pathological repository keeps OOMing an indexer, exclude or
  defer it and file an issue against the indexer.

## Dependencies

- cAdvisor metrics via the kubelet (metric source)
- No GKG application dependency: the signal is entirely Kubernetes-side.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`).

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/orbit-cause-alerts.yml) (hand-maintained; keep the expr above in sync when tuning it).

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
