---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prerequisites, installation order, and shared configuration values for GitLab Orbit on GitLab Self-Managed.
title: Get started with GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

GitLab Orbit depends on ClickHouse, Kubernetes, and NATS, and installs none of them.
Set up all three before you install GitLab Orbit.

## Prerequisites

- GitLab 19.2.2 or later.
- Administrator access to the GitLab instance and to its PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- [ClickHouse](clickhouse.md), set up for GitLab.
- Kubernetes 1.36 or later. For earlier versions, see [Image volumes](#image-volumes).
- NATS with JetStream turned on.
- The latest [GitLab Orbit release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases) and [Helm chart](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/releases).

## Installation order

Each step depends on the step before it:

1. [Set up ClickHouse](clickhouse.md) and run the GitLab ClickHouse migrations.
1. Install NATS on the cluster with JetStream turned on.
1. [Set up data replication](data-replication.md) to copy GitLab rows into the ClickHouse data lake.
1. [Set up GitLab Orbit](orbit-setup.md) and turn on indexing for a top-level group.

You can verify steps 1 to 3 before you install GitLab Orbit.

## Image volumes

Siphon mounts the `gitlab-siphon-tables` image as an
[image volume](https://kubernetes.io/docs/tasks/configure-pod-container/image-volumes/).
Image volumes are generally available in Kubernetes 1.36, and on by default in Kubernetes 1.35.
On Kubernetes 1.33 and 1.34, turn on the `ImageVolume` feature gate in the API server and on every node.
The container runtime must also support image volumes.
Some managed services, such as Amazon EKS, do not let you turn on the gate.
When the gate is off, Kubernetes drops the volume without an error, and the Siphon pods fail with
`no such file` errors under `/etc/tables`.

If your cluster cannot use image volumes, set `splitTables.volumeMode: initCopy` in `siphon-values.yaml`.
For more information, see
[Split mode without ImageVolume](https://gitlab.com/gitlab-org/analytics-section/platform-insights/siphon-helm-charts#split-mode-without-imagevolume).

## NATS

Siphon publishes every changed row to a NATS JetStream stream, and both Siphon and GitLab Orbit read from it.
NATS needs JetStream, a persistent volume, and a `max_payload` of 64 MB, because some GitLab rows are larger
than the 1 MB default.

With the [NATS Helm chart](https://github.com/nats-io/k8s/tree/main/helm/charts/nats), set these values:

```yaml
config:
  jetstream:
    enabled: true
    fileStore:
      pvc:
        size: <volume_size>
  merge:
    max_payload: << 64MB >>
```

Keep the `<<` and `>>` marks around `64MB`, so the chart writes the value without quotes.

## Object storage (optional)

Siphon stores snapshots, and rows too large for the stream, in a NATS JetStream object store by default.
To keep that data off the JetStream volume, use an S3-compatible or Google Cloud Storage bucket instead.
For more information, see [Object storage for large rows](data-replication.md#object-storage-for-large-rows).

GitLab Orbit uses no object storage.
The indexer uses node-local disk for repository checkouts.

## Shared configuration values

Use the same value in every component that reads it.
No component checks these values against the others, so a mismatch stops data without an error.

| Value | Used by | Example |
|-------|---------|---------|
| NATS stream name | Siphon and GitLab Orbit | `siphon_stream_main_db` |
| Data lake database | GitLab, Siphon, and GitLab Orbit | `gitlab_clickhouse_main_production` |
| Graph database | GitLab Orbit | `orbit` |
| PostgreSQL host and port | Siphon | `postgres.example.com:5432` |
| GitLab URL reachable from the cluster | GitLab Orbit | `https://gitlab.example.com` |
| GitLab Orbit gRPC endpoint reachable from GitLab | GitLab | `tls://orbit.example.com:50054` |

To use a stream name other than `siphon_stream_main_db`, set `stream_name` in `siphon-values.yaml` and
`schedule.tasks.siphon.events_stream_name` in `orbit-values.yaml`.

## Next step

- [Set up data replication](data-replication.md)
