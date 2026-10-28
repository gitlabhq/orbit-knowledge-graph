---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Sizing recommendations for GitLab Orbit components at each GitLab reference architecture size.
title: GitLab Orbit sizing
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

Size GitLab Orbit next to your [GitLab reference architecture](https://docs.gitlab.com/administration/reference_architectures/).
Find the size of your reference architecture, then add the GitLab Orbit components for that size to your environment.

GitLab Orbit load depends more on the groups that you index, your Gitaly nodes, and your PostgreSQL change rate than on user count.
After you choose a size, read [Adjust for your workload](#adjust-for-your-workload).

The values are starting points, and they can be more than your workload needs.
Monitor the signals in [Monitor and scale](#monitor-and-scale), then decrease the values.

## Choose a size

Use the size that matches your GitLab reference architecture.
For fewer than 2,000 users, use the 2,000 users size.

| Size | GitLab reference architecture | Cloud Native Hybrid size |
|------|-------------------------------|--------------------------|
| 2,000 users | [Up to 40 RPS or 2,000 users](https://docs.gitlab.com/administration/reference_architectures/2k_users/) | None |
| 3,000 users | [Up to 60 RPS or 3,000 users](https://docs.gitlab.com/administration/reference_architectures/3k_users/) | None |
| 5,000 users | [Up to 100 RPS or 5,000 users](https://docs.gitlab.com/administration/reference_architectures/5k_users/) | Small |
| 10,000 users | [Up to 200 RPS or 10,000 users](https://docs.gitlab.com/administration/reference_architectures/10k_users/) | Medium |
| 25,000 users | [Up to 500 RPS or 25,000 users](https://docs.gitlab.com/administration/reference_architectures/25k_users/) | Large |
| 50,000 users | [Up to 1000 RPS or 50,000 users](https://docs.gitlab.com/administration/reference_architectures/50k_users/) | Extra large |

## Components

| Component | Runs on | Purpose |
|-----------|---------|---------|
| Webserver | Kubernetes | Serves GitLab Orbit queries. |
| SDLC indexer | Kubernetes | Builds the graph from the GitLab data in the ClickHouse data lake. |
| Code indexer | Kubernetes | Builds the code graph from repository archives that it fetches through the GitLab internal API. |
| Dispatcher | Kubernetes | Schedules indexing tasks. Use the default resources of the chart. |
| Health check | Kubernetes | Serves component health to the webserver, and the queue depth that autoscaling reads. Turn it on with `healthCheck.enabled: true`. Use the default resources of the chart. |
| NATS | Kubernetes | Holds the stream of PostgreSQL changes and the indexing tasks. |
| Siphon | Kubernetes | Reads PostgreSQL changes and writes them to NATS and ClickHouse. |
| ClickHouse | ClickHouse Cloud or VMs | Stores the data lake and the graph. |

## Kubernetes

### Node pools

Put the GitLab Orbit pods on their own node pool, and NATS and Siphon on a second node pool.
The maximum node counts leave room for the extra pods that a chart upgrade starts.

| Size | GitLab Orbit node pool | Node disk | NATS and Siphon node pool |
|------|------------------------|-----------|---------------------------|
| 2,000 users | 1 to 3 nodes, 8 vCPU, 32 GB memory | 200 GB | 1 to 2 nodes, 4 vCPU, 16 GB memory |
| 3,000 users | 1 to 6 nodes, 8 vCPU, 32 GB memory | 200 GB | 1 to 2 nodes, 4 vCPU, 16 GB memory |
| 5,000 users | 2 to 5 nodes, 16 vCPU, 64 GB memory | 300 GB | 1 to 2 nodes, 4 vCPU, 16 GB memory |
| 10,000 users | 2 to 6 nodes, 16 vCPU, 64 GB memory | 400 GB | 1 to 2 nodes, 4 vCPU, 16 GB memory |
| 25,000 users | 3 to 10 nodes, 16 vCPU, 64 GB memory | 500 GB | 3 to 4 nodes, 4 vCPU, 16 GB memory |
| 50,000 users | 3 to 10 nodes, 32 vCPU, 128 GB memory | 600 GB | 3 to 4 nodes, 8 vCPU, 32 GB memory |

| Node size | GCP example | AWS example | Azure example |
|-----------|-------------|-------------|---------------|
| 4 vCPU, 16 GB memory | `n2-standard-4` | `m6i.xlarge` | `Standard_D4s_v5` |
| 8 vCPU, 32 GB memory | `n2-standard-8` | `m6i.2xlarge` | `Standard_D8s_v5` |
| 16 vCPU, 64 GB memory | `n2-standard-16` | `m6i.4xlarge` | `Standard_D16s_v5` |
| 32 vCPU, 128 GB memory | `n2-standard-32` | `m6i.8xlarge` | `Standard_D32s_v5` |

Machine type examples are for illustration.
Other machine types that meet the requirements are supported, including Arm types.
Do not use burstable instance types, because their CPU performance is not consistent.

### Pods

The following tables give the pod count, and the request and the limit for one pod.
The indexers use most of their memory limit during backfill, so their memory request equals the limit.
GitLab Orbit sets CPU limits on the code indexer, because the code indexer sets its indexing lanes from them.

Webserver:

| Size | Pods | Request | Limit |
|------|------|---------|-------|
| 2,000 users | 1 | 250m vCPU, 1 GiB | 2 vCPU, 4 GiB |
| 3,000 users | 2 | 250m vCPU, 1 GiB | 2 vCPU, 4 GiB |
| 5,000 users | 2 | 500m vCPU, 2 GiB | 2 vCPU, 6 GiB |
| 10,000 users | 2 | 500m vCPU, 2 GiB | 2 vCPU, 6 GiB |
| 25,000 users | 3 | 1 vCPU, 4 GiB | 4 vCPU, 8 GiB |
| 50,000 users | 3 | 1 vCPU, 4 GiB | 4 vCPU, 8 GiB |

SDLC indexer:

| Size | Pods | Request | Limit | Workers per pod |
|------|------|---------|-------|-----------------|
| 2,000 users | 1 | 250m vCPU, 16 GiB | 2 vCPU, 16 GiB | 4 |
| 3,000 users | 1 | 250m vCPU, 16 GiB | 2 vCPU, 16 GiB | 4 |
| 5,000 users | 2 | 500m vCPU, 24 GiB | 4 vCPU, 24 GiB | 8 |
| 10,000 users | 2 | 500m vCPU, 24 GiB | 4 vCPU, 24 GiB | 8 |
| 25,000 users | 3 | 1 vCPU, 32 GiB | 8 vCPU, 32 GiB | 12 |
| 50,000 users | 3 | 1 vCPU, 32 GiB | 8 vCPU, 32 GiB | 16 |

Code indexer:

| Size | Pods | Request | Limit | Indexing lanes per pod | Ephemeral disk (request, limit) | `/tmp` size | Archive slots per Gitaly storage |
|------|------|---------|-------|------------------------|---------------------------------|-------------|----------------------------------|
| 2,000 users | 1 | 2 vCPU, 10 GiB | 4 vCPU, 10 GiB | 2 | 20 GiB, 30 GiB | 26 GiB | 1 |
| 3,000 users | 1 to 3 | 3 vCPU, 16 GiB | 6 vCPU, 16 GiB | 3 | 20 GiB, 30 GiB | 26 GiB | 1 |
| 5,000 users | 1 to 3 | 4 vCPU, 20 GiB | 8 vCPU, 20 GiB | 4 | 30 GiB, 60 GiB | 54 GiB | 2 |
| 10,000 users | 1 to 3 | 6 vCPU, 26 GiB | 12 vCPU, 26 GiB | 5 | 30 GiB, 60 GiB | 54 GiB | 4 |
| 25,000 users | 2 to 3 | 6 vCPU, 34 GiB | 12 vCPU, 34 GiB | 6 | 50 GiB, 65 GiB | 59 GiB | 8 |
| 50,000 users | 2 to 3 | 8 vCPU, 40 GiB | 16 vCPU, 40 GiB | 8 | 65 GiB, 65 GiB | 59 GiB | 16 |

Each indexing lane needs about 2 vCPU and 5 GiB of memory.
Keep the CPU limit below the usable vCPU of one node.

### Configure the pools

The chart defaults do not match these tables.
To configure the pools, set the values in `orbit-values.yaml`, then upgrade the chart.
For example, for 10,000 users:

```yaml
webserver:
  replicas: 2
  resources:
    requests:
      cpu: 500m
      memory: 2Gi
    limits:
      cpu: "2"
      memory: 6Gi
indexer:
  pools:
    sdlc:
      replicas: 2
      resources:
        requests:
          cpu: 500m
          memory: 24Gi
        limits:
          cpu: "4"
          memory: 24Gi
      engine:
        max_concurrent_workers: 8
        concurrency_groups:
          sdlc: 8
    code:
      replicas: 3
      tmpSizeLimit: 54Gi
      resources:
        requests:
          cpu: "6"
          memory: 26Gi
          ephemeral-storage: 30Gi
        limits:
          cpu: "12"
          memory: 26Gi
          ephemeral-storage: 60Gi
engine:
  handlers:
    code-indexing-task:
      pipeline:
        archive_storage_budget: 4
        archive_tenant_budget: 4
```

- For the SDLC indexer, set `max_concurrent_workers` and `concurrency_groups.sdlc` to the same value.
- Keep `tmpSizeLimit` below the ephemeral storage limit, because logs and the container layer use the same disk.
- Without autoscaling, set the code indexer `replicas` to the maximum of the pod range.
- Before you turn on indexing for groups, apply the rules in [Limit the archive load on Gitaly](#limit-the-archive-load-on-gitaly) to `archive_storage_budget` and `archive_tenant_budget`.
- To put the pods on the node pool, set `nodeSelector` and `tolerations` for the webserver, the dispatcher, the health check, and each indexer pool.

### Autoscale the code indexer

Prerequisites:

- The [KEDA](https://keda.sh/) operator runs in the cluster.

To autoscale the code indexer on the count of pending code indexing tasks:

1. Add these values to `orbit-values.yaml`:

   ```yaml
   healthCheck:
     enabled: true
   indexer:
     pools:
       code:
         keda:
           enabled: true
           minReplicaCount: <code_min_pods>
           maxReplicaCount: <code_max_pods>
   ```

   Replace the following values:

   - `<code_min_pods>`: the minimum of the code indexer pod range in [Pods](#pods).
   - `<code_max_pods>`: the maximum of the code indexer pod range. The chart default is 4.

1. Upgrade the chart.

## ClickHouse

The sizes are for the ClickHouse instance that holds the GitLab Orbit data lake and graph.
If GitLab Orbit shares the ClickHouse instance of GitLab, use these sizes for the shared instance.
They are larger than the sizes for GitLab ClickHouse features, because the data lake holds a copy of the GitLab database.

| Size | Deployment | ClickHouse nodes | ClickHouse Keeper nodes |
|------|------------|------------------|-------------------------|
| 2,000 users | ClickHouse Cloud or 1 node | 1, 8 vCPU, 32 GB memory | None |
| 3,000 users | ClickHouse Cloud or 1 node | 1, 8 vCPU, 32 GB memory | None |
| 5,000 users | ClickHouse Cloud or 1 node | 1, 16 vCPU, 64 GB memory | None |
| 10,000 users | ClickHouse Cloud or 1 node, with optional high availability | 1, 16 vCPU, 64 GB memory | 3, 2 vCPU, 8 GB memory, for high availability |
| 25,000 users | ClickHouse Cloud, 1 node, or high availability | 1, 32 vCPU, 128 GB memory, or 3, 16 vCPU, 64 GB memory | 3, 2 vCPU, 8 GB memory, for high availability |
| 50,000 users | ClickHouse Cloud or high availability | 3, 16 vCPU, 64 GB memory | 3, 2 vCPU, 8 GB memory |

| Node size | GCP example | AWS example | Azure example |
|-----------|-------------|-------------|---------------|
| 2 vCPU, 8 GB memory | `c4a-standard-2` | `m8g.large` | `Standard_D2ps_v6` |
| 8 vCPU, 32 GB memory | `c4a-standard-8` | `m8g.2xlarge` | `Standard_D8ps_v6` |
| 16 vCPU, 64 GB memory | `c4a-standard-16` | `m8g.4xlarge` | `Standard_D16ps_v6` |
| 32 vCPU, 128 GB memory | `c4a-standard-32` | `m8g.8xlarge` | `Standard_D32ps_v6` |

The examples are Arm machine types. x86 machine types of the same size are also supported.

For the high availability setup and its load balancer, see [High availability](clickhouse.md#high-availability).
On ClickHouse Cloud, choose a service with at least the memory of one node in the table.

If you run ClickHouse on VMs:

- Give each node this much disk:

  ```plaintext
  disk = 2 x (GitLab PostgreSQL database size + 1 MiB x indexed projects)
  ```

  The factor of 2 keeps free space for merges.
- Set the memory and external sort settings in [Memory and disk](clickhouse.md#memory-and-disk).
  Without them, large sorts fail instead of spilling to disk.
  The example there is for 32 GiB of memory.
  For other sizes, set `max_memory_usage` to about 62% of the memory, and the two external limits to 25%.
  On a 64 GiB node, use 42949672960 for `max_memory_usage`, and 17179869184 for the external limits.
- Set a TTL on the system log tables, as in [System log retention](clickhouse.md#system-log-retention).

## NATS

| Size | Replicas | Request per pod | Limit per pod | Minimum volume per replica |
|------|----------|-----------------|---------------|----------------------------|
| 2,000 users | 1 | 250m vCPU, 512 MiB | 1 vCPU, 1 GiB | 20 GiB |
| 3,000 users | 1 | 250m vCPU, 512 MiB | 1 vCPU, 1 GiB | 30 GiB |
| 5,000 users | 1 | 500m vCPU, 1 GiB | 2 vCPU, 2 GiB | 100 GiB |
| 10,000 users | 1 | 500m vCPU, 1 GiB | 2 vCPU, 2 GiB | 200 GiB |
| 25,000 users | 3 | 1 vCPU, 2 GiB | 2 vCPU, 4 GiB | 400 GiB |
| 50,000 users | 3 | 1 vCPU, 2 GiB | 4 vCPU, 8 GiB | 500 GiB |

For fewer than 25,000 users, run NATS as one node.
One node has no redundancy.
If you lose its volume, Siphon needs a fresh snapshot, and GitLab Orbit can stop with [Error: `ontology archive vNN is missing`](troubleshooting.md#error-ontology-archive-vnn-is-missing).

For 25,000 users or more, run NATS as a 3-node JetStream cluster.
For the steps, see [Configure a NATS cluster](#configure-a-nats-cluster).

Calculate the volume with this formula. Use the larger of the result and the table value.

```plaintext
volume = retention_seconds x peak_changes_per_second x 300 bytes
```

- `retention_seconds` is the Siphon `max_age_seconds`.
  The example in [Install Siphon](data-replication.md#install-siphon) keeps 15 days, which is 1296000 seconds.
- `peak_changes_per_second` is the PostgreSQL change rate at peak.
  To measure it, run this query twice during peak, then divide the difference by the seconds between the two runs:

  ```sql
  SELECT sum(n_tup_ins + n_tup_upd + n_tup_del) FROM pg_stat_user_tables;
  ```

### Configure a NATS cluster

Prerequisites:

- Three nodes in the NATS and Siphon node pool.

To run NATS as a 3-node JetStream cluster:

1. Add the cluster settings to the NATS chart values, then upgrade the NATS release:

   ```yaml
   config:
     cluster:
       enabled: true
       replicas: 3
   ```

1. Set `nats_config.replicas: 3` in `siphon-values.yaml`, as in [Install Siphon](data-replication.md#install-siphon), then upgrade the Siphon release.

The cluster protects the Siphon stream only.
GitLab Orbit creates its own streams and key-value buckets, including the schema archive catalog, with one replica.
If you lose the node that holds them, GitLab Orbit can stop with [Error: `ontology archive vNN is missing`](troubleshooting.md#error-ontology-archive-vnn-is-missing).

## Siphon

Each Siphon deployment runs one pod.

| Size | Memory request | Memory limit |
|------|----------------|--------------|
| 2,000 users | 256 MiB | 2 GiB |
| 3,000 users | 256 MiB | 2 GiB |
| 5,000 users | 512 MiB | 4 GiB |
| 10,000 users | 1 GiB | 6 GiB |
| 25,000 users | 1 GiB | 8 GiB |
| 50,000 users | 2 GiB | 10 GiB |

For CPU, give each pod a request of 250m vCPU and a limit of 2 vCPU.

### PostgreSQL disk for the replication slot

Siphon reads changes through a PostgreSQL logical replication slot.
While Siphon is stopped, the slot holds write-ahead log (WAL) on the GitLab PostgreSQL disk.

Keep enough free disk for the longest Siphon outage that you accept, and at least 24 hours:

```plaintext
free disk = WAL bytes per hour x outage hours
```

To measure the WAL rate:

1. Run this query, and note the result:

   ```sql
   SELECT pg_current_wal_lsn();
   ```

1. One hour later, run the same query again.
1. Calculate the WAL bytes for the hour:

   ```sql
   SELECT pg_wal_lsn_diff('<second_lsn>', '<first_lsn>');
   ```

   Replace the following values:

   - `<first_lsn>`: the result of the first query.
   - `<second_lsn>`: the result of the second query.

To protect the disk, you can set `max_slot_wal_keep_size` in PostgreSQL to less than the free disk.
When a slot exceeds this limit, PostgreSQL invalidates it, and Siphon needs a fresh snapshot.

> [!warning]
> `max_slot_wal_keep_size` applies to every replication slot on the server, including the slots of standby replicas and GitLab Geo.
> Set it larger than the lag that your standby replicas can reach.

For more information, see [Verify replication](data-replication.md#verify-replication).

## Adjust for your workload

The first code backfill puts the most load on GitLab Orbit and on Gitaly.
Estimate its length, and limit its load on Gitaly before you turn on indexing for groups.

### Estimate the backfill time

To estimate the time for the first code backfill, use this formula:

```plaintext
parallel_archives = min(archive slots x Gitaly storages, code indexer pods x indexing lanes per pod)
backfill_hours = indexed projects x 30 seconds / parallel_archives / 3600
```

For example, 100,000 projects with 4 parallel archives take about 9 days.
The formula assumes that the repositories are spread evenly across the storages.
If one storage holds most of them, that storage sets the pace.
Large repositories take longer.

The smaller term in `parallel_archives` sets the pace:

- If archive slots x Gitaly storages is smaller, the archive slots set the pace.
  Add slots only as the rules in [Limit the archive load on Gitaly](#limit-the-archive-load-on-gitaly) allow.
  More code indexer pods do not help.
- If code indexer pods x indexing lanes per pod is smaller, add code indexer pods.

### Limit the archive load on Gitaly

Gitaly builds an archive of every repository for the code backfill.
The load goes to the Gitaly node that holds the repositories.

Before you turn on indexing for groups:

1. Turn on the [GitLab rate limits](https://docs.gitlab.com/security/rate_limits/).
1. Balance your repositories across your Gitaly nodes as evenly as you can.
1. Find the vCPU count of the Gitaly node that holds the most repositories.
   For Gitaly Cluster (Praefect), use the smallest node in the cluster.
1. Calculate the archive slots: 1 for each 4 vCPU of that node, up to 16.
   If the node has more than one storage, divide by the number of storages on the node.
1. Set `archive_storage_budget` and `archive_tenant_budget` to the smaller of this value and the archive slots in [Pods](#pods), then upgrade the chart.

`archive_storage_budget` limits the archive downloads in flight for each Gitaly storage, across all code indexer pods.
`archive_tenant_budget` is the same limit for projects whose Gitaly storage GitLab Orbit cannot find.

During the backfill:

1. Turn on indexing for a few groups at a time.
1. Monitor the CPU of the Gitaly node with the most repositories.
   If its CPU is more than 70%, or its 1-minute load average is more than its vCPU count, pause code indexing until the load drops.
   To pause it, set `indexer.pools.code.replicas: 0` and upgrade the chart, or with autoscaling, set the `autoscaling.keda.sh/paused-replicas: "0"` annotation on the code indexer `ScaledObject`.
1. Monitor the p95 of `grpc_server_handling_seconds` on Gitaly for `GetArchive`, which is the backfill load.
   `PackObjectsHookWithSidechannel`, `PostUploadPackWithSidechannel`, and `FindCommit` show the effect on your users.
   Gitaly exports these histograms only when you set `grpc_latency_buckets` in its Prometheus configuration.

## Monitor and scale

After the first backfill, monitor these signals and adjust the values.
For the metrics and dashboards, see [Monitor GitLab Orbit](observability.md).

| Signal | Action |
|--------|--------|
| OOM kills on any GitLab Orbit pod | Increase the memory request and limit of that pod. |
| CPU throttling on the code or SDLC indexer | Increase the CPU limit, or add pods. |
| `code_pending` in the health check grows and does not decrease | Use the smaller term in [Estimate the backfill time](#estimate-the-backfill-time) to choose between archive slots and pods. |
| `gkg_indexer_sdlc_watermark_lag_seconds` grows | Increase the SDLC indexer CPU limit or workers. |
| Gitaly node CPU is more than 70% during backfill | Decrease the archive slots, or pause code indexing. |
| NATS volume is more than 80% full | Increase the volume, or decrease the Siphon `max_age_seconds`. |
| ClickHouse disk is more than 50% full | Increase the disk, so merges have space. |
| Free disk on PostgreSQL is less than one day of WAL | Find why Siphon is behind, or increase the disk. |
| Usage is far below the request for 30 days | Decrease the request. |

## Related topics

- [GitLab reference architectures](https://docs.gitlab.com/administration/reference_architectures/)
- [GitLab rate limits](https://docs.gitlab.com/security/rate_limits/)
- [Set up GitLab Orbit](orbit-setup.md)
- [ClickHouse for GitLab Orbit](clickhouse.md)
- [Monitor GitLab Orbit](observability.md)
