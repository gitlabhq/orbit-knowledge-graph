# Orbit NATS JetStream SLO alerts

Covers `OrbitServiceNatsJetstreamErrorSLOViolation`, `OrbitServiceNatsJetstreamTrafficCessation`
and `OrbitServiceNatsJetstreamTrafficAbsent`.

## Overview

JetStream is the persistence layer on the NATS cluster: it stores the indexing job
streams and the KV buckets (locks).

These alerts cover only the NATS that runs in the Orbit clusters (orbit-prd, orbit-stg) for
Orbit and Siphon. Other NATS deployments belong to the `nats` service. These alerts do
not page and post to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- [NATS runbook](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/README.md)
- Owner team: `context_systems`, for the NATS in the Orbit clusters and the Orbit
  consumers. Siphon, the other producer, is owned by `platform_insights`.

## Alerts

### OrbitServiceNatsJetstreamErrorSLOViolation

Message redeliveries are rising. A redelivery means a consumer took a message but did
not acknowledge it in time, so JetStream handed it out again. This is how code indexing
tasks retry, but a rising rate means work is failing repeatedly.

### OrbitServiceNatsJetstreamTrafficCessation / TrafficAbsent

No JetStream messages are moving. `TrafficCessation` means traffic recently dropped to
zero, `TrafficAbsent` means it has been missing for longer.

## Metrics

The `nats_jetstream` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
uses JetStream stream and consumer metrics (total messages, redeliveries) from the
Prometheus exporter.

A low background redelivery rate is normal during deploys and pod restarts. Sustained
growth is not.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- Redeliveries spike during indexer rollouts (in-flight messages get redelivered). A
  spike that lines up with a deploy and settles within minutes is expected.

## Severities

- No paging. User impact is indexing delay. A redelivery loop on one subject can
  also starve other work on the same consumer.

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Error ratio
gitlab_component_errors:ratio_1h{type="orbit", component="nats_jetstream", monitor="global"}

# Request rate (traffic cessation fires when this drops to zero)
gitlab_component_ops:rate_1h{type="orbit", component="nats_jetstream", monitor="global"}
```

- [NATS dashboard](https://dashboards.gitlab.net/d/orbit-orbit-nats), JetStream panels:
  redeliveries, ack pending, consumer lag.
- Per-stream and per-consumer state from nats-box:
  [Check NATS stream health](../code_indexing.md#check-nats-stream-health).
- Cross-check which handler is failing on the
  [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer); redeliveries
  are the flip side of handler errors.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  for services `nats` and `gkg`. Stream, consumer or subject config changes are the
  usual suspects.
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Redeliveries concentrated on one subject | Poison message: a task that fails on every attempt | Find the failing handler in the logs. Code tasks go to the [dead letter queue](../code_indexing.md#dead-letter-queue) after the last attempt. Do not purge NATS. Escalate to Orbit tier-2 if it continues |
| Redeliveries across all subjects | Consumers too slow: ClickHouse insert latency or undersized pools | Check ClickHouse first, then scale the indexer pools ([Capacity](../maintenance.md#capacity)) |
| Consumer stopped consuming after a deploy or config change | Orbit cannot create or update its stream or consumer | Read the indexer logs. Do not delete streams or KV buckets: each release has its own streams, and Siphon uses the same NATS. Escalate to Orbit tier-2 ([Escalation](../README.md#escalation)) |
| Ack pending grows, disk follows | Consumers lagging, persisted messages accumulate | Scale indexers; disk has its own alert (`nats_disk_space`) with runbook [docs/nats/operations.md](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md) |

Dead letter queue procedures (inspect, replay) are in
[code indexing](../code_indexing.md#dead-letter-queue).

## Possible Resolutions

- Fix the cause of the poison message, then replay its dead letter.
- Scale the consuming pool ([Capacity](../maintenance.md#capacity)) or relieve ClickHouse
  pressure ([Pause and resume indexing](../maintenance.md#pause-and-resume-indexing)).

## Dependencies

- Consumers: indexer pools. Producers: dispatcher, Siphon. Storage: JetStream file
  storage on the NATS PVCs, backed up by the Velero schedule `velero-nats-backup`.

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`). For Siphon producer
  problems, ask in `#g_analytics_platform_insights`.
- Generic SLO playbooks: [ErrorSLOViolation](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/ErrorSLOViolation.md),
  [TrafficAbsent](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/TrafficAbsent.md).

## Definitions

- [Alert rules](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/autogenerated-analytics-eventsdot-orbit-service-level-alerts.yml)
  (generated, do not edit).
- [Tune the SLI](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
