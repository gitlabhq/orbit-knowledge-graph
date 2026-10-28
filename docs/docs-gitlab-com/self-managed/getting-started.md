---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prerequisites, installation order, and shared configuration values for GitLab Orbit on GitLab Self-Managed.
title: Get started with GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

GitLab Orbit depends on Kubernetes, NATS, object storage, and ClickHouse, and installs none of them.
Set up all four before you install GitLab Orbit.

## Prerequisites

- One of these GitLab versions, which accept GQL queries by default:
  - 19.4.4
  - 19.5.2
  - 19.6.0 or later
- Administrator access to the GitLab instance and to its PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- [ClickHouse](clickhouse.md), set up for GitLab.
- Kubernetes 1.36 or later. For Kubernetes 1.33 to 1.35, see [Image volumes](#image-volumes).
- NATS with JetStream turned on.
- An S3-compatible or Google Cloud Storage bucket for Siphon.
- The latest [GitLab Orbit release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases) and [Helm chart](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/releases).

## Installation order

Each step depends on the step before it:

1. [Set up ClickHouse](clickhouse.md) and run the GitLab ClickHouse migrations.
1. Install NATS on the cluster with JetStream turned on.
1. [Set up data replication](data-replication.md) to copy GitLab rows into the ClickHouse data lake.
1. [Set up GitLab Orbit](orbit-setup.md) and turn on indexing for a top-level group.

You can verify steps 1 to 3 before you install GitLab Orbit.

## Sizing

For the pod, node, NATS, Siphon, and ClickHouse sizes, see [GitLab Orbit sizing](sizing.md).
For the chart defaults, see [`values.yaml`](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/blob/main/chart/values.yaml).

## High availability

For ClickHouse high availability, see [High availability](clickhouse.md#high-availability).
For a NATS cluster, see [Configure a NATS cluster](sizing.md#configure-a-nats-cluster).

## Image volumes

Siphon mounts the `gitlab-siphon-tables` image as an [image volume](https://kubernetes.io/docs/tasks/configure-pod-container/image-volumes/).
Image volumes need Kubernetes 1.35 or later, or the `ImageVolume` feature gate on Kubernetes 1.33 and 1.34, and a container runtime that supports them.
Some managed services, such as Amazon EKS, do not let you turn on the gate.
When the gate is off, Kubernetes drops the volume without an error, and the Siphon pods fail with `no such file` errors under `/etc/tables`.

If your cluster cannot use image volumes, set `splitTables.volumeMode: initCopy` in `siphon-values.yaml`.
For more information, see [Split mode without ImageVolume](https://gitlab.com/gitlab-org/analytics-section/platform-insights/siphon-helm-charts#split-mode-without-imagevolume).

## Configure NATS

Siphon publishes every changed row to a NATS JetStream stream, and Siphon and GitLab Orbit read from it.
NATS needs JetStream, a persistent volume, and a `max_payload` of 64 MB, because some GitLab rows are larger than the 1 MB default.

To configure NATS, set these values in the [NATS Helm chart](https://github.com/nats-io/k8s/tree/main/helm/charts/nats):

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

Replace the following values:

- `<volume_size>`: the size of the JetStream volume, from [NATS](sizing.md#nats).

Keep the `<<` and `>>` marks around `64MB`, so the chart writes the value without quotes.

## Object storage

Siphon writes snapshots and oversize rows to an S3-compatible or Google Cloud Storage bucket.
For the credentials, see [Object storage for large rows](data-replication.md#object-storage-for-large-rows).

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

To use a stream name other than `siphon_stream_main_db`, set both `stream_name` values in `siphon-values.yaml` and `schedule.tasks.siphon.events_stream_name` in `orbit-values.yaml`.
