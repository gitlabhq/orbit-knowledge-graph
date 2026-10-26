# Monitoring

Back to the [runbook](README.md).

## Dashboards

| Dashboard | Use it for |
| --- | --- |
| [Webserver](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver) | Query rate, errors and latency |
| [Indexer](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) | Freshness, indexing volume, saturation, fleet size, schema migration |
| [NATS](https://dashboards.gitlab.net/d/orbit-orbit-nats) | Queue depth, messages in flight, JetStream disk |
| [All Orbit dashboards](https://dashboards.gitlab.net/dashboards/f/orbit/orbit) | SLI overview, Siphon |

Panels to know:

| Panel | What it tells you |
| --- | --- |
| Indexer, **Fleet** row | Indexer replicas per pool. |
| Indexer, **ETL: worker permits in flight** | A flat line at its top means that all indexer pods are full. |
| Indexer, **Schema migration** row | "migrating-version age" above zero means a reindex runs. Coverage shows the progress. |
| Indexer, **Deploys** markers | A reindex shows as a large increase in indexed rows and projects right after a marker. |
| NATS, `nats_consumer_num_ack_pending` | Flat at the `max_ack_pending` limit: more SDLC pods do not help. See [capacity](maintenance.md#capacity). |

ClickHouse: in the ClickHouse Cloud console, open the service, then Monitoring > Service
health for CPU and memory per replica.

Logs: [Kibana](https://log.gprd.gitlab.net/app/r/s/DhOL5), index `pubsub-orbit-inf-gprd`.

## SLOs

The objectives are in [`orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
in the runbooks repository.

| SLI | What it measures |
| --- | --- |
| `orbit_query` | Graph queries: server faults and latency. Caller errors do not count. |
| `sdlc_freshness` | How far SDLC data in the graph is behind the datalake |
| `gkg_indexer_sdlc` | SDLC indexing errors |
| `gkg_indexer_code` | Code indexing errors |
| `gkg_dispatcher` | Scheduler task errors |
| `nats_server`, `nats_jetstream` | NATS in the Orbit clusters |

Code data has no freshness SLO yet.

## Alert routing

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Where alerts post | `orbit-prd`: `#f_orbit_alerts` and `#alerts`. `orbit-stg`: `#f_orbit_alerts_stg` | `#dedicated-prod-alerts` and incident.io |
| Paging | No Orbit alert pages | incident.io sends Orbit alerts to the Orbit on-call |

Dedicated has no SLO alerts. It sends the three cause alerts in the table below.

## Alerts

| Alert | Type | Playbook |
| --- | --- | --- |
| `OrbitServiceOrbitQueryApdexSLOViolation`, `OrbitServiceOrbitQueryErrorSLOViolation` | SLO | [orbit-query](alerts/orbit-query.md) |
| `OrbitServiceGkgIndexerSdlcErrorSLOViolation` | SLO | [gkg-indexer-sdlc](alerts/gkg-indexer-sdlc.md) |
| `OrbitServiceGkgIndexerCodeErrorSLOViolation` | SLO | [gkg-indexer-code](alerts/gkg-indexer-code.md) |
| `OrbitServiceGkgDispatcherErrorSLOViolation` | SLO | [gkg-dispatcher](alerts/gkg-dispatcher.md) |
| `OrbitServiceNatsServerErrorSLOViolation` | SLO | [nats-server](alerts/nats-server.md) |
| `OrbitServiceNatsJetstreamErrorSLOViolation` | SLO | [nats-jetstream](alerts/nats-jetstream.md) |
| `OrbitService<SLI>TrafficCessation`, `OrbitService<SLI>TrafficAbsent` | SLO | The playbook of that SLI |
| [`OrbitSdlcFreshnessSLOViolation`](alerts/sdlc-freshness.md) | Freshness | Same |
| [`OrbitSdlcDataOlderThan4h`](alerts/OrbitSdlcDataOlderThan4h.md) | Freshness | Same |
| [`OrbitQueryLatencyDegraded`](alerts/OrbitQueryLatencyDegraded.md) | Latency | Same |
| [`OrbitDispatchTaskStopped`](alerts/OrbitDispatchTaskStopped.md) | Heartbeat | Same |
| [`OrbitAuthFilterMissing`](alerts/OrbitAuthFilterMissing.md) | Cause, also on Dedicated | Same |
| [`OrbitPipelineInvariantViolated`](alerts/OrbitPipelineInvariantViolated.md) | Cause, also on Dedicated | Same |
| [`OrbitContainerOOMKilled`](alerts/OrbitContainerOOMKilled.md) | Cause, also on Dedicated | Same |
| `nats_disk_space` | Saturation | [NATS operations](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md) |
| `open_fds`, `kube_container_*`, `kube_horizontalpodautoscaler_desired_replicas` | Saturation | Generic Kubernetes saturation |
| `KubeContainersWaitingInError` | Kubernetes, owned by SRE | Orbit containers cannot start |

Rule sources: [`orbit-alerts.libsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet)
(freshness, latency, heartbeat) and
[`orbit-cause-alerts.yml`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/orbit-cause-alerts.yml)
(cause alerts).

No alert watches certificate expiry yet
([#1424](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1424)).
