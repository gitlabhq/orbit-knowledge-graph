---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Resource sizes for GitLab Orbit components at each GitLab reference architecture size.
title: GitLab Orbit reference architectures
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

Use this page to size GitLab Orbit next to your GitLab reference architecture.
Find the row for your reference architecture size, then add the GitLab Orbit components to your environment.
For fewer than 2,000 users, use the 2,000 row.

GitLab Orbit load depends more on the projects that you enable, your Gitaly nodes, and your PostgreSQL change rate than on user count.
After you choose a size, read [Adjust for your workload](#adjust-for-your-workload).

The values are starting points, and they can be more than your workload needs.
Monitor the signals in [Monitor and scale](#monitor-and-scale), then decrease the values.

## Components

| Component | Runs on | Purpose |
|---|---|---|
| Webserver | Kubernetes | Serves GitLab Orbit queries. |
| SDLC indexer | Kubernetes | Builds the graph from the GitLab data in the ClickHouse data lake. |
| Code indexer | Kubernetes | Builds the code graph from repository archives that it fetches through the GitLab internal API. |
| Dispatcher | Kubernetes | Schedules indexing tasks. Use the default resources of the chart. |
| Health check | Kubernetes | Serves component health to the webserver, and the queue depth that autoscaling reads. Turn it on with `healthCheck.enabled: true`. Use the default resources of the chart. |
| NATS | Kubernetes | Holds the stream of PostgreSQL changes and the indexing tasks. |
| Siphon | Kubernetes | Reads PostgreSQL changes and writes them to NATS and ClickHouse. |
| ClickHouse | ClickHouse Cloud, or VMs | Stores the data lake and the graph. |

## Kubernetes pools

Each cell gives the pod count, then the request and the limit for one pod.

| Size | Webserver | SDLC indexer | Code indexer |
|---|---|---|---|
| 2,000 | 1 pod. Request 0.25 vCPU, 1 GiB. Limit 2 vCPU, 4 GiB. | 1 pod. Request 0.25 vCPU, 16 GiB. Limit 2 vCPU, 16 GiB. | 1 pod. Request 2 vCPU, 10 GiB. Limit 4 vCPU, 10 GiB. |
| 3,000 | 2 pods. Request 0.25 vCPU, 1 GiB. Limit 2 vCPU, 4 GiB. | 1 pod. Request 0.25 vCPU, 16 GiB. Limit 2 vCPU, 16 GiB. | 1 to 3 pods. Request 3 vCPU, 16 GiB. Limit 6 vCPU, 16 GiB. |
| 5,000 | 2 pods. Request 0.5 vCPU, 2 GiB. Limit 2 vCPU, 6 GiB. | 2 pods. Request 0.5 vCPU, 24 GiB. Limit 4 vCPU, 24 GiB. | 1 to 3 pods. Request 4 vCPU, 20 GiB. Limit 8 vCPU, 20 GiB. |
| 10,000 | 2 pods. Request 0.5 vCPU, 2 GiB. Limit 2 vCPU, 6 GiB. | 2 pods. Request 0.5 vCPU, 24 GiB. Limit 4 vCPU, 24 GiB. | 1 to 3 pods. Request 6 vCPU, 26 GiB. Limit 12 vCPU, 26 GiB. |
| 25,000 | 3 pods. Request 1 vCPU, 4 GiB. Limit 4 vCPU, 8 GiB. | 3 pods. Request 1 vCPU, 32 GiB. Limit 8 vCPU, 32 GiB. | 2 to 3 pods. Request 6 vCPU, 34 GiB. Limit 12 vCPU, 34 GiB. |
| 50,000 | 3 pods. Request 1 vCPU, 4 GiB. Limit 4 vCPU, 8 GiB. | 3 pods. Request 1 vCPU, 32 GiB. Limit 8 vCPU, 32 GiB. | 2 to 3 pods. Request 8 vCPU, 40 GiB. Limit 16 vCPU, 40 GiB. |

The indexers use most of their memory limit during backfill, so their memory request equals the limit.

Workers, lanes, disk, and archive slots for the indexers:

| Size | SDLC workers per pod | Code workers per pod | Code indexing lanes per pod | Code ephemeral disk per pod (request, limit) | Code `/tmp` size | Archive slots per Gitaly storage |
|---|---|---|---|---|---|---|
| 2,000 | 4 | 2 | 2 | 20 GiB, 30 GiB | 26 GiB | 1 |
| 3,000 | 4 | 3 | 3 | 20 GiB, 30 GiB | 26 GiB | 1 |
| 5,000 | 8 | 4 | 4 | 30 GiB, 60 GiB | 54 GiB | 2 |
| 10,000 | 8 | 6 | 5 | 30 GiB, 60 GiB | 54 GiB | 4 |
| 25,000 | 12 | 6 | 6 | 50 GiB, 65 GiB | 59 GiB | 8 |
| 50,000 | 16 | 8 | 8 | 65 GiB, 65 GiB | 59 GiB | 16 |

The code indexer sets its indexing lanes from the pod limits.
Each lane needs about 2 vCPU and 5 GiB of memory.
To index faster, add pods, not bigger pods.
Keep the CPU limit below the usable vCPU of one node.

### Configure the pools

The chart defaults do not match these tables.
Set the values in `orbit-values.yaml`.
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
        max_concurrent_workers: 6
        concurrency_groups:
          code: 6
engine:
  handlers:
    code-indexing-task:
      pipeline:
        archive_storage_budget: 4
        archive_tenant_budget: 4
```

- Set `max_concurrent_workers` and the matching `concurrency_groups` entry to the same value.
- Keep `tmpSizeLimit` below the ephemeral storage limit, because logs and the container layer use the same disk.
- Without autoscaling, set the code indexer `replicas` to the maximum of the pod range.
- Before you enable groups, apply the rules in [Unbalanced Gitaly nodes](#unbalanced-gitaly-nodes) to `archive_storage_budget` and `archive_tenant_budget`.
- To put the pods on the node pool, set `nodeSelector` and `tolerations` for the webserver, the dispatcher, the health check, and each indexer pool.

### Autoscale the code indexer

To autoscale the code indexer on the count of pending code indexing tasks:

1. Install the [KEDA](https://keda.sh/) operator in the cluster.
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

   Replace `<code_min_pods>` and `<code_max_pods>` with the code indexer pod range from the table.
   The chart default maximum is 4.

### Node pools

Put the GitLab Orbit pods on their own node pool, and NATS and Siphon on a second node pool.
The maximum node counts leave room for the extra pods that a chart upgrade starts.

| Size | GitLab Orbit node pool | Node disk | NATS and Siphon node pool |
|---|---|---|---|
| 2,000 | 1 to 3 nodes, 8 vCPU, 32 GiB | 200 GiB | 1 to 2 nodes, 4 vCPU, 16 GiB |
| 3,000 | 1 to 6 nodes, 8 vCPU, 32 GiB | 200 GiB | 1 to 2 nodes, 4 vCPU, 16 GiB |
| 5,000 | 2 to 5 nodes, 16 vCPU, 64 GiB | 300 GiB | 1 to 2 nodes, 4 vCPU, 16 GiB |
| 10,000 | 2 to 6 nodes, 16 vCPU, 64 GiB | 400 GiB | 1 to 2 nodes, 4 vCPU, 16 GiB |
| 25,000 | 3 to 10 nodes, 16 vCPU, 64 GiB | 500 GiB | 3 to 4 nodes, 4 vCPU, 16 GiB |
| 50,000 | 3 to 10 nodes, 32 vCPU, 128 GiB | 600 GiB | 3 to 4 nodes, 8 vCPU, 32 GiB |

Machine type examples:

| Node size | GCP | AWS | Azure |
|---|---|---|---|
| 4 vCPU, 16 GiB | `n2-standard-4` | `m6i.xlarge` | `Standard_D4s_v5` |
| 8 vCPU, 32 GiB | `n2-standard-8` | `m6i.2xlarge` | `Standard_D8s_v5` |
| 16 vCPU, 64 GiB | `n2-standard-16` | `m6i.4xlarge` | `Standard_D16s_v5` |
| 32 vCPU, 128 GiB | `n2-standard-32` | `m6i.8xlarge` | `Standard_D32s_v5` |

## ClickHouse

GitLab Orbit can share the ClickHouse instance of GitLab, or use a separate one.
The sizes are for the instance that holds the GitLab Orbit data lake and graph.

| Size | Deployment | Configuration |
|---|---|---|
| 2,000 | ClickHouse Cloud, or 1 node | 8 vCPU, 32 GiB |
| 3,000 | ClickHouse Cloud, or 1 node | 8 vCPU, 32 GiB |
| 5,000 | ClickHouse Cloud, or 1 node | 16 vCPU, 64 GiB |
| 10,000 | ClickHouse Cloud, or 1 node. HA is optional. | 16 vCPU, 64 GiB |
| 25,000 | ClickHouse Cloud, or HA | 3 nodes, 16 vCPU, 64 GiB each. 3 ClickHouse Keeper nodes, 2 vCPU, 8 GiB each. |
| 50,000 | ClickHouse Cloud, or HA | 3 nodes, 16 vCPU, 64 GiB each. 3 ClickHouse Keeper nodes, 2 vCPU, 8 GiB each. |

For the HA setup and its load balancer, see [High availability](clickhouse.md#high-availability).
On ClickHouse Cloud, choose a service with at least the memory of one node in the table.

Machine type examples:

| Node size | GCP | AWS | Azure |
|---|---|---|---|
| 2 vCPU, 8 GiB | `c4a-standard-2` | `m8g.large` | `Standard_D2ps_v6` |
| 8 vCPU, 32 GiB | `c4a-standard-8` | `m8g.2xlarge` | `Standard_D8ps_v6` |
| 16 vCPU, 64 GiB | `c4a-standard-16` | `m8g.4xlarge` | `Standard_D16ps_v6` |

If you run ClickHouse on VMs:

- Give each node this much disk:

  ```plaintext
  disk = 2 x (GitLab PostgreSQL database size + 1 MiB x enabled projects)
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
|---|---|---|---|---|
| 2,000 | 1 | 0.25 vCPU, 512 MiB | 1 vCPU, 1 GiB | 20 GiB |
| 3,000 | 1 | 0.25 vCPU, 512 MiB | 1 vCPU, 1 GiB | 30 GiB |
| 5,000 | 1 | 0.5 vCPU, 1 GiB | 2 vCPU, 2 GiB | 100 GiB |
| 10,000 | 1 | 0.5 vCPU, 1 GiB | 2 vCPU, 2 GiB | 200 GiB |
| 25,000 | 3 | 1 vCPU, 2 GiB | 2 vCPU, 4 GiB | 400 GiB |
| 50,000 | 3 | 1 vCPU, 2 GiB | 4 vCPU, 8 GiB | 500 GiB |

Below 25,000 users, run NATS as one node.
One node has no redundancy.
If you lose its volume, Siphon needs a fresh snapshot, and GitLab Orbit can stop with [Error: `ontology archive vNN is missing`](troubleshooting.md#error-ontology-archive-vnn-is-missing).

At 25,000 users and more, run NATS as a 3-node JetStream cluster:

1. In the NATS chart values, add the cluster settings:

   ```yaml
   config:
     cluster:
       enabled: true
       replicas: 3
   ```

1. Set `nats_config.replicas: 3` in the Siphon values, as in [Install Siphon](data-replication.md#install-siphon).

GitLab Orbit creates its own streams, which hold the indexing tasks, with one replica.
The cluster does not replicate these streams.

Calculate the volume, and use the larger of the calculated value and the table value:

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

## Siphon

Each Siphon deployment runs one pod.

| Size | Memory request | Memory limit |
|---|---|---|
| 2,000 | 256 MiB | 2 GiB |
| 3,000 | 256 MiB | 2 GiB |
| 5,000 | 512 MiB | 4 GiB |
| 10,000 | 1 GiB | 6 GiB |
| 25,000 | 1 GiB | 8 GiB |
| 50,000 | 2 GiB | 10 GiB |

For CPU, give each pod a request of 0.25 vCPU and a limit of 2 vCPU.

### PostgreSQL disk for the replication slot

Siphon reads changes through a PostgreSQL logical replication slot.
While Siphon is stopped, the slot holds write-ahead log (WAL) on the GitLab PostgreSQL disk.

Keep enough free disk for the longest Siphon outage that you accept, and at least 24 hours:

```plaintext
free disk = WAL bytes per hour x outage hours
```

To measure the WAL rate, run `SELECT pg_current_wal_lsn();` twice, one hour apart.
Then run `SELECT pg_wal_lsn_diff('<second_lsn>', '<first_lsn>');`.

To protect the disk, you can set `max_slot_wal_keep_size` in PostgreSQL to less than the free disk.
When a slot exceeds this limit, PostgreSQL invalidates it, and Siphon needs a fresh snapshot.

> [!warning]
> `max_slot_wal_keep_size` applies to every replication slot on the server, including the slots of standby replicas and GitLab Geo.
> Set it larger than the lag that your standby replicas can reach.

For more information, see [Verify replication](data-replication.md#verify-replication).

## Adjust for your workload

### Projects to enable

If the backfill estimate below is longer than you accept, increase the maximum code indexer pods.
First, apply the rules in [Unbalanced Gitaly nodes](#unbalanced-gitaly-nodes).

To estimate the time for the first code backfill:

```plaintext
parallel_archives = min(archive slots x Gitaly storages, code indexer pods x indexing lanes per pod)
backfill_hours = enabled projects x 30 seconds / parallel_archives / 3,600
```

For example, 100,000 projects with 4 parallel archives take about 9 days.
The formula assumes that the repositories are spread evenly across the storages.
If one storage holds most of them, that storage sets the pace.
Large repositories take longer.

### Unbalanced Gitaly nodes

The code backfill fetches an archive of every repository through the GitLab internal API.
Gitaly builds each archive, so the load goes to the Gitaly node that holds the repositories.
If one Gitaly node holds most of your repositories, the backfill can use all of its CPU and make Git operations slow for your users.

`archive_storage_budget` is the maximum number of archive downloads in flight for each Gitaly storage, across all code indexer pods.
`archive_tenant_budget` is the same limit for projects whose Gitaly storage GitLab Orbit cannot find.

Before you enable groups:

1. Find the vCPU count of the Gitaly node that holds the most repositories.
   If you use Gitaly Cluster (Praefect), use the vCPU count of the smallest node in the cluster.
1. Calculate the budget for that node: 1 archive slot for each 4 vCPU, up to 16.
1. If the node has more than one storage, divide the budget by the number of storages on the node.
1. Set `archive_storage_budget` and `archive_tenant_budget` to the smaller of this value and the table value.

During the backfill:

1. Enable groups a few at a time, not all at once.
1. Monitor the CPU of the Gitaly node with the most repositories.
   Pause code indexing if its CPU is more than 70%, or if its 1-minute load average is more than its vCPU count.
1. Monitor the p95 of `grpc_server_handling_seconds` on Gitaly:
   - `GetArchive` shows the load that the backfill adds.
   - `PackObjectsHookWithSidechannel`, `PostUploadPackWithSidechannel`, and `FindCommit` show the effect on your users.

   Gitaly exports these latency histograms only when you set `grpc_latency_buckets` in its Prometheus configuration.

### Pause code indexing

The commands use the release name `orbit` and the namespace `gitlab-orbit` from [Set up GitLab Orbit](orbit-setup.md).

Without autoscaling:

1. Set `indexer.pools.code.replicas: 0` in `orbit-values.yaml`, then upgrade the chart.
   If you only scale the deployment with `kubectl`, the next chart upgrade starts the pods again.
1. To resume, set `replicas` back to the table value, then upgrade the chart.

With autoscaling:

1. To pause, run:

   ```shell
   kubectl -n gitlab-orbit annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0"
   ```

1. To resume, run:

   ```shell
   kubectl -n gitlab-orbit annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

## Monitor and scale

After the first backfill, monitor these signals and adjust the values.
For the metrics and dashboards, see [Monitor GitLab Orbit](observability.md).

| Signal | Action |
|---|---|
| OOM kills on any GitLab Orbit pod | Increase the memory request and limit of that pod. |
| CPU throttling on the code or SDLC indexer | Increase the CPU limit, or add pods. |
| Pending code indexing tasks grow and do not decrease | Add code indexer pods, if Gitaly has capacity. |
| SDLC watermark lag grows | Increase the SDLC indexer CPU limit or workers. |
| Gitaly node CPU is more than 70% during backfill | Decrease the archive slots, or pause code indexing. |
| NATS volume is more than 80% full | Increase the volume, or decrease the Siphon `max_age_seconds`. |
| ClickHouse disk is more than 50% full | Increase the disk, so merges have space. |
| Free disk on PostgreSQL is less than one day of WAL | Find why Siphon is behind, or increase the disk. |
| Usage is far below the request for 30 days | Decrease the request. |
