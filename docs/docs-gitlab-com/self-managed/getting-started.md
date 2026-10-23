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

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.2.2.

{{< /history >}}

> [!note]
> GitLab Orbit on GitLab Self-Managed is in
> [beta](https://docs.gitlab.com/policy/development_stages_support/#beta).
> This feature is available for testing, but not ready for production use.

GitLab Orbit depends on three systems that it does not install: ClickHouse, Kubernetes, and NATS.
The remaining setup steps require all three systems to exist and be reachable.

## Prerequisites

- GitLab 19.2.2 or later.
- Administrator access to the GitLab instance and to its PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- ClickHouse, set up for GitLab. For the supported releases, see [ClickHouse](#clickhouse).
- Kubernetes 1.24 or later. For the Siphon table image, see [Kubernetes](#kubernetes).
- NATS with JetStream turned on.
- A GitLab Orbit release that supports your GitLab version. For more information, see
  [Version compatibility](#version-compatibility).

## Version compatibility

GitLab does not check the GitLab Orbit version. A release that is too old fails at query time.

| GitLab version | Minimum GitLab Orbit version |
|----------------|------------------------------|
| 19.2 to 19.4   | 0.128.0 |
| 19.5           | 0.131.0 |

0.128.0 is the oldest release that the `orbit` chart 4.x installs. GitLab 19.3 and 19.4 also need 0.100.0
or later, and GitLab 19.5 calls services that 0.131.0 added.

For a later GitLab version, use the latest GitLab Orbit release.

## Installation order

Each step depends on the step before it. Install the components in this order:

1. Set up ClickHouse for GitLab and run the GitLab ClickHouse migrations.
1. Install NATS on the cluster with JetStream turned on.
1. [Set up data replication](data-replication.md): prepare PostgreSQL, then install Siphon. After this
   step, GitLab rows arrive in the ClickHouse data lake.
1. [Set up GitLab Orbit](orbit-setup.md): create the ClickHouse identities, install the chart, point
   GitLab at GitLab Orbit, and turn on indexing for a top-level group.

Steps 1 to 3 do not depend on GitLab Orbit. You can verify each one on its own.

## Kubernetes

The GitLab Orbit chart requires Kubernetes 1.24 or later.

Siphon in split mode mounts the `gitlab-siphon-tables` image as an
[image volume](https://kubernetes.io/docs/tasks/configure-pod-container/image-volumes/) by default. Image
volumes need `containerd` 2.1 or later, or CRI-O 1.31 or later, and the `ImageVolume` feature gate:

| Kubernetes version | `ImageVolume` feature gate |
|--------------------|----------------------------|
| 1.33 and 1.34      | Off by default. Some managed services, such as Amazon EKS, do not let you turn it on. |
| 1.35               | On by default. |
| 1.36 and later     | Generally available. |

When the gate is off, Kubernetes drops the image volume with no error. If your cluster cannot use image
volumes, set
`splitTables.volumeMode: initCopy` in `siphon-values.yaml`. Init containers then copy the table definitions
into the pod. For more information, see
[Split mode without ImageVolume](https://gitlab.com/gitlab-org/analytics-section/platform-insights/siphon-helm-charts#split-mode-without-imagevolume).

GitLab Orbit can run on a separate cluster from GitLab. That cluster must reach GitLab, PostgreSQL,
ClickHouse, and NATS, and GitLab must reach that cluster.

## NATS

Siphon publishes every changed row to a NATS JetStream stream, and both Siphon and GitLab Orbit read from
that stream. NATS requires JetStream turned on and a persistent volume.

Set the NATS server `max_payload` to 64 MB. The default of 1 MB is smaller than some GitLab rows. Siphon
reads the server value to decide when a row is too large to send.

With the [NATS Helm chart](https://github.com/nats-io/k8s/tree/main/helm/charts/nats), set these values:

```yaml
config:
  jetstream:
    enabled: true
    fileStore:
      pvc:
        size: <volume_size>
  merge:
    # Keep the << >> marks. The chart then writes 64MB without quotes.
    max_payload: << 64MB >>
```

## Object storage (optional)

Siphon stores snapshot events in an object store, along with any row that is still too large after you
raise `max_payload`. By default, Siphon uses a NATS JetStream object store bucket, so no additional service
is required. To keep that traffic off the JetStream volume, or to apply a separate retention policy,
configure an S3-compatible or Google Cloud Storage bucket instead.

GitLab Orbit uses no object storage. The graph is stored in ClickHouse and can be rebuilt by indexing
again. The indexer requires node-local disk for repository checkouts. The chart sizes this disk with
ephemeral storage requests.

## Shared configuration values

Choose each of these values once, and use the same value in every component that reads it. No component
validates these values against the others. If they do not match, the affected component starts, but some or
all data does not arrive.

| Value | Used by | Example |
|-------|---------|---------|
| NATS stream name | Siphon and GitLab Orbit | `siphon_stream_main_db` |
| Data lake database | GitLab, Siphon, and GitLab Orbit | `gitlab_clickhouse_main_production` |
| Graph database | GitLab Orbit | `orbit` |
| PostgreSQL host and port | Siphon | `postgres.example.com:5432` |
| GitLab URL reachable from the cluster | GitLab Orbit | `https://gitlab.example.com` |
| GitLab Orbit gRPC endpoint reachable from GitLab | GitLab | `tls://orbit.example.com:50054` |

GitLab Orbit expects the stream name `siphon_stream_main_db` by default. To use a different name, set
`stream_name` in `siphon-values.yaml` and `schedule.tasks.siphon.events_stream_name` in
`orbit-values.yaml`.

## Next step

- [Set up data replication](data-replication.md)
- [Troubleshooting](troubleshooting.md)
