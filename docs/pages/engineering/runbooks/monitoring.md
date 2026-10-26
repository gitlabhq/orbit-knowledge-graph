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

## Which page for which alert

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
| `KubeContainersWaitingInError` | Orbit containers cannot start: check the pods and the image, see [troubleshooting](troubleshooting.md) |
