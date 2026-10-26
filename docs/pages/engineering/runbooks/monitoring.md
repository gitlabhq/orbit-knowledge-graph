# Monitoring

Back to the [runbook](README.md). Dashboard links and alert routing are on the card.

## Panels to know

| Panel | What it tells you |
| --- | --- |
| Indexer, **Fleet** row | Indexer replicas per pool. |
| Indexer, **ETL: worker permits in flight** | A flat line at its top: all indexer pods are full. |
| Indexer, **Schema migration** row | "migrating-version age" above zero: a reindex runs. Coverage shows the progress. |
| Indexer, **Deploys** markers | A reindex shows as a large increase in indexed rows right after a marker. |
| [NATS](https://dashboards.gitlab.net/d/orbit-orbit-nats), `nats_consumer_num_ack_pending` | Flat at the `max_ack_pending` limit: more SDLC pods do not help. See [capacity](maintenance.md#capacity). |
| ClickHouse Cloud console, Monitoring > Service health | CPU and memory per replica of the graph service. |

SLOs are defined in [`orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet).
SLO status per SLI: [Orbit overview](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?var-PROMETHEUS_DS=mimir-analytics-eventsdot&var-environment=orbit-prd).
The [error budget](https://dashboards.gitlab.net/d/product-orbit_error_budget) shows only the
GitLab Rails part, not the Orbit service SLIs.

## Which page for which alert

The alert links still open the pages in the runbooks repository. Seven older cause alerts,
for example `OrbitCircuitBreakerOpen`, have pages only there until
[runbooks!11925](https://gitlab.com/gitlab-com/runbooks/-/merge_requests/11925) removes them.

| Alert | Page |
| --- | --- |
| `OrbitServiceOrbitQuery*`, `OrbitQueryLatencyDegraded` | [orbit-query](alerts/orbit-query.md) |
| `OrbitServiceSdlcFreshness*`, `OrbitSdlcFreshnessSLOViolation`, `OrbitSdlcDataOlderThan4h` | [sdlc-freshness](alerts/sdlc-freshness.md) |
| `OrbitServiceGkgIndexerSdlc*` | [gkg-indexer-sdlc](alerts/gkg-indexer-sdlc.md) |
| `OrbitServiceGkgIndexerCode*` | [gkg-indexer-code](alerts/gkg-indexer-code.md) |
| `OrbitServiceGkgDispatcher*`, `OrbitDispatchTaskStopped` | [gkg-dispatcher](alerts/gkg-dispatcher.md) |
| `OrbitServiceNatsServer*`, `OrbitServiceNatsJetstream*` | [nats-slo](alerts/nats-slo.md) |
| `OrbitAuthFilterMissing` | [OrbitAuthFilterMissing](alerts/OrbitAuthFilterMissing.md) |
| `OrbitPipelineInvariantViolated` | [OrbitPipelineInvariantViolated](alerts/OrbitPipelineInvariantViolated.md) |
| `OrbitContainerOOMKilled` | [OrbitContainerOOMKilled](alerts/OrbitContainerOOMKilled.md) |
| `nats_disk_space` | [NATS operations](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md) |
| `open_fds` | An Orbit process leaks file descriptors. Find the pod and page Orbit tier-2. |
| `kube_horizontalpodautoscaler_desired_replicas` | A pool is at its KEDA maximum, see [capacity](maintenance.md#capacity) |
| `KubeContainersWaitingInError` | Orbit containers cannot start: check the pods and the image, see [troubleshooting](troubleshooting.md) |
