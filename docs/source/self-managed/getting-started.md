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
- ClickHouse 26.4 to 26.6, set up for GitLab.
- Kubernetes 1.24 or later. Siphon has a higher requirement. For more information, see [Kubernetes](#kubernetes).
- NATS with JetStream turned on.
- A GitLab Orbit release that supports your GitLab version. For more information, see
  [Version compatibility](#version-compatibility).

## Version compatibility

GitLab does not check the GitLab Orbit version. A release that is too old fails at query time.

| GitLab version | Minimum GitLab Orbit version |
|----------------|------------------------------|
| 19.2 to 19.4   | 0.128.0, the oldest release that the `orbit` chart 4.x installs |
| 19.5           | 0.131.0 |

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

## ClickHouse

GitLab Orbit requires ClickHouse 26.4 or later. GitLab supports ClickHouse releases up to 26.6, so use 26.4,
26.5, or 26.6. GitLab Orbit does not check the version at startup. On an older release, some queries fail.
To use ClickHouse 26.7 or later, follow
[ClickHouse 26.7 and later](https://docs.gitlab.com/integration/clickhouse/#clickhouse-267-and-later), which
sets `compatibility` to `26.6`.

Set up ClickHouse for GitLab first. For more information, see
[ClickHouse](https://docs.gitlab.com/integration/clickhouse/). Complete every step on that page, including
[Run ClickHouse migrations](https://docs.gitlab.com/integration/clickhouse/#run-clickhouse-migrations) and
[Enable ClickHouse for analytics](https://docs.gitlab.com/integration/clickhouse/#enable-clickhouse-for-analytics).
Those migrations create the data lake tables that replication writes into. Without these tables, Siphon has
no target to write to and replication fails.

GitLab Orbit uses two databases:

| Database | Written by | Read by |
|----------|------------|---------|
| `gitlab_clickhouse_main_production` | GitLab, Siphon | GitLab, the GitLab Orbit indexer and dispatcher |
| `orbit` | The GitLab Orbit dispatcher (schema) and indexer (data) | All three GitLab Orbit components |

The two databases can be on separate ClickHouse instances. If they are, give GitLab, Siphon, and
GitLab Orbit credentials for the instance that holds the database each one uses.

The `gitlab_clickhouse_main_production` database exists after ClickHouse setup is complete. You create the
`orbit` database when you set up GitLab Orbit. GitLab Orbit does not create it at startup.

### High availability

GitLab Orbit can run on a ClickHouse cluster with several replicas behind one load balancer.
The cluster needs ClickHouse Keeper with a quorum of three nodes and at least two ClickHouse replicas.
An HTTP load balancer sits in front of the replicas.
For the ClickHouse side, see
[HA ClickHouse for GitLab Self-Managed](https://docs.gitlab.com/integration/clickhouse/?tab=HA+ClickHouse+for+GitLab+Self-Managed).

Create both databases with the `Replicated` database engine, on the cluster:

```sql
CREATE DATABASE orbit ON CLUSTER '{cluster}'
ENGINE = Replicated('/clickhouse/databases/orbit', '{shard}', '{replica}');
```

Then tell each writer that the cluster is replicated:

| Component | Setting |
|-----------|---------|
| GitLab | None. GitLab detects the `Replicated` database engine and creates replicated tables. |
| Siphon | `connection_settings` with `insert_quorum: "auto"`, `insert_quorum_parallel: "0"`, and `async_insert: "0"`. See [Install Siphon](data-replication.md#install-siphon). |
| GitLab Orbit | `clickhouse.ha.enabled: true` in the Helm values. When only one database sits on the cluster, set `replicated: true` on that connection instead. |

With the switch on, GitLab Orbit creates `Replicated*MergeTree` tables, writes with a majority quorum, reads
with sequential consistency, and retries the transient quorum and Keeper errors.
One replica can leave the cluster while indexing and queries continue.
Leave the switch off on a single node and on ClickHouse Cloud.

### Sizing and settings

Provision at least 8 CPU and 32 GiB of memory for ClickHouse. ClickHouse saturates 8 cores while the graph
builds.

Provision at least as much ClickHouse storage as the size of the GitLab PostgreSQL database.

ClickHouse spills a large sort or `GROUP BY` to disk when it uses half of the memory limit. To set a fixed
memory ceiling and fixed thresholds, put them in the `default` profile. The following file suits a 32 GiB
instance, with 8 GiB for each threshold and 20 GiB for the memory ceiling.

Save the file in `/etc/clickhouse-server/users.d/`, for example as `users.d/orbit.xml`. ClickHouse ignores a
profile in `config.d/` and reports no error.

```xml
<clickhouse>
  <profiles>
    <default>
      <max_bytes_before_external_sort>8589934592</max_bytes_before_external_sort>
      <max_bytes_before_external_group_by>8589934592</max_bytes_before_external_group_by>
      <max_memory_usage>21474836480</max_memory_usage>
    </default>
  </profiles>
</clickhouse>
```

### System log retention

The stock ClickHouse configuration keeps `query_log`, `text_log`, `trace_log`, and the other system log tables
with no time limit. On one installation, these tables used most of the ClickHouse disk. To delete old rows, add
a `ttl` for each log to a file in `/etc/clickhouse-server/config.d/`, for example
`config.d/system_log_ttl.xml`:

```xml
<clickhouse>
  <query_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></query_log>
  <text_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></text_log>
  <trace_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></trace_log>
  <metric_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></metric_log>
  <asynchronous_metric_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></asynchronous_metric_log>
  <part_log><ttl>event_date + INTERVAL 30 DAY DELETE</ttl></part_log>
</clickhouse>
```

After the restart, ClickHouse renames each old table, for example to `system.query_log_0`, and keeps its rows.
Drop the renamed tables to free the disk.

## Kubernetes

The GitLab Orbit chart requires Kubernetes 1.24 or later.

Siphon in split mode mounts the `gitlab-siphon-tables` image as an
[image volume](https://kubernetes.io/docs/tasks/configure-pod-container/image-volumes/) by default. Image
volumes need `containerd` 2.1 or later, or CRI-O 1.31 or later, and the `ImageVolume` feature gate:

| Kubernetes version | `ImageVolume` feature gate |
|--------------------|----------------------------|
| 1.33 and 1.34      | Off by default. Managed services, such as Amazon EKS, do not let you turn it on. |
| 1.35               | On by default. |
| 1.36 and later     | Generally available. |

When the gate is off, Kubernetes drops the image volume with no error, and the Siphon pods fail with
`no such file` errors under `/etc/tables`. If your cluster cannot use image volumes, set
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
validates these values against the others. If they do not match, the affected component starts with no
error, but some or all data does not arrive.

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
