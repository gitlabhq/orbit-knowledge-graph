---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Set up ClickHouse for GitLab Orbit on GitLab Self-Managed.
title: ClickHouse for GitLab Orbit
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

ClickHouse spills a large sort or `GROUP BY` to disk at half of the query memory limit. To set a fixed
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
with no time limit, so they can fill the disk. To delete old rows, add
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

Restart ClickHouse to apply the file. After the restart, ClickHouse renames each old table, for example to `system.query_log_0`, and keeps its rows.
Drop the renamed tables to free the disk.

## Create the ClickHouse database and identities

GitLab Orbit requires its own graph database and three users:

| User | Role | Access |
|------|------|--------|
| `gkg_writer` | `gkg_app` | Read and write the graph database |
| `gkg_reader` | `gkg_reader_app` | Read the graph database |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | Read the data lake |

The statements that create the database and the users ship inside the GitLab Orbit image, so the statements
always match the version you run. The statements are idempotent. GitLab Orbit does not create the graph
database at startup, and the dispatcher only runs schema migrations against a database that already exists.

To create the database and the identities:

1. Read the statements:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.149.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. Substitute the graph database name, the data lake database name, and a password for each user. Use
   `orbit` as the graph database name.

1. Run the result against ClickHouse as an administrator. The statements create the graph database and
   grant the `gkg_app` role the privileges it needs on that database.

GitLab Orbit reaches ClickHouse over the HTTP interface on port 8123, or port 8443 with TLS. Siphon uses the
native protocol on port 9000, so both the HTTP port and port 9000 must be reachable from the cluster.
