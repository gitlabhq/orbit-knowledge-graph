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

{{< /details >}}

GitLab Orbit depends on Kubernetes, NATS, object storage, and ClickHouse, and installs none of them.
Set up all four before you install GitLab Orbit.

## Prerequisites

<!-- Merge after the v19.4.4-ee and v19.5.2-ee tags exist. If Product picks 19.5 as the minimum, delete the 19.4 line. -->

- One of these GitLab versions:
  - 19.4.4 or a later 19.4 patch release
  - 19.5.2 or a later 19.5 patch release
  - 19.6.0 or later
- Administrator access to the GitLab instance and to its PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- [ClickHouse](clickhouse.md), set up for GitLab.
- Kubernetes 1.36 or later. For earlier versions, see [Image volumes](#image-volumes).
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

## Reference architectures

GitLab Dedicated sizes GitLab Orbit for these tiers.
Pod cells give the pod count and the limits for one pod, and node cells give the node count and size.
Repository count and push rate drive the indexer load more than user count.

| GitLab users | Webserver | `sdlc` indexer | `code` indexer | NATS volume | Kubernetes nodes |
|--------------|-----------|----------------|----------------|-------------|------------------|
| 2,000 | 1, 2 CPU, 4 GiB | 1, 2 CPU, 16 GiB | 1, 4 CPU, 10 GiB, 30 GiB disk | 20 GiB | 1 to 3, 8 vCPU, 32 GiB |
| 5,000 | 2, 2 CPU, 6 GiB | 2, 4 CPU, 24 GiB | 1 to 3, 8 CPU, 18 GiB, 60 GiB disk | 100 GiB | 2 to 5, 16 vCPU, 64 GiB |
| 10,000 | 2, 2 CPU, 6 GiB | 2, 4 CPU, 24 GiB | 1 to 3, 12 CPU, 26 GiB, 60 GiB disk | 200 GiB | 2 to 6, 16 vCPU, 64 GiB |
| 25,000 | 3, 4 CPU, 8 GiB | 3, 8 CPU, 32 GiB | 2 to 3, 12 CPU, 34 GiB, 65 GiB disk | 3 x 400 GiB | 3 to 8, 16 vCPU, 64 GiB |
| 50,000 | 3, 4 CPU, 8 GiB | 3, 8 CPU, 32 GiB | 2 to 3, 16 CPU, 34 GiB, 65 GiB disk | 3 x 500 GiB | 3 to 10, 32 vCPU, 128 GiB |

Each Siphon deployment runs one pod with up to 2 CPU and 2 to 10 GiB of memory.
For ClickHouse, see [Memory and disk](clickhouse.md#memory-and-disk).
For the chart defaults, see [`values.yaml`](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/blob/main/chart/values.yaml).

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

- `<volume_size>`: the size of the JetStream volume, from [Reference architectures](#reference-architectures).

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

To use a stream name other than `siphon_stream_main_db`, set `stream_name` in `siphon-values.yaml` and `schedule.tasks.siphon.events_stream_name` in `orbit-values.yaml`.
