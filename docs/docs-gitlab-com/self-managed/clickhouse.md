---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Set up ClickHouse for GitLab Orbit on GitLab Self-Managed.
title: ClickHouse for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

GitLab Orbit stores the data lake and the graph in ClickHouse.
GitLab Orbit connects over HTTP on port 8123, or port 8443 with TLS.
Siphon connects over the native protocol on port 9000.

## Supported versions

GitLab Orbit requires ClickHouse 26.4 or later, in a release that GitLab supports.
For the releases that GitLab supports, see [Supported ClickHouse versions](https://docs.gitlab.com/integration/clickhouse/#supported-clickhouse-versions).

## Databases

[Set up ClickHouse for GitLab](https://docs.gitlab.com/integration/clickhouse/) first, including the GitLab ClickHouse migrations.
The migrations create the data lake tables that Siphon writes into.

| Database | Written by | Read by |
|----------|------------|---------|
| `gitlab_clickhouse_main_production` | GitLab and Siphon | GitLab and GitLab Orbit |
| `orbit` | GitLab Orbit | GitLab Orbit |

The two databases can be on separate ClickHouse instances.

## High availability

GitLab Orbit can run on a ClickHouse cluster with ClickHouse Keeper and at least two replicas behind an HTTP load balancer.
For the cluster setup, see [HA ClickHouse for GitLab Self-Managed](https://docs.gitlab.com/integration/clickhouse/?tab=HA+ClickHouse+for+GitLab+Self-Managed).

To set up high availability:

1. Create the graph database with the `Replicated` engine:

   ```sql
   CREATE DATABASE orbit ON CLUSTER '{cluster}'
   ENGINE = Replicated('/clickhouse/databases/orbit', '{shard}', '{replica}');
   ```

1. Store users in ClickHouse Keeper, so every replica has the GitLab Orbit users.
   Add this block to the server configuration on every replica:

   ```xml
   <user_directories>
     <users_xml>
       <path>users.xml</path>
     </users_xml>
     <replicated>
       <zookeeper_path>/clickhouse/access</zookeeper_path>
     </replicated>
   </user_directories>
   ```

1. Turn on replication in each writer:

   | Component | Setting |
   |-----------|---------|
   | GitLab | None. GitLab detects the `Replicated` engine. |
   | Siphon | `insert_quorum: "auto"`, `insert_quorum_parallel: "0"`, and `async_insert: "0"` in `connection_settings`. |
   | GitLab Orbit | `clickhouse.ha.enabled: true`. If only one database is on the cluster, set `replicated: true` on that connection. |

Leave these settings off on a single node and on ClickHouse Cloud.

## Memory and disk

Give ClickHouse at least 8 CPU, 32 GiB of memory, and as much storage as the GitLab PostgreSQL database.

To cap memory and spill large queries to disk, put the limits in the `default` profile in `/etc/clickhouse-server/users.d/`.
ClickHouse ignores a profile in `config.d/` and reports no error.
These values suit a 32 GiB instance:

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

## System log retention

The ClickHouse system log tables have no time limit by default and can fill the disk.
To delete old rows, add a TTL for these logs to `/etc/clickhouse-server/config.d/system_log_ttl.xml`, then restart ClickHouse:

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

After the restart, ClickHouse keeps the old rows in renamed tables, such as `system.query_log_0`.
Drop those tables to free the disk.

## Users

GitLab Orbit uses three ClickHouse users:

| User | Access |
|------|--------|
| `gkg_writer` | Read and write the graph database |
| `gkg_reader` | Read the graph database |
| `gkg_siphon_reader` | Read the data lake |

Before every install and upgrade, the chart setup hook signs in to the graph instance as a ClickHouse administrator, and creates the graph database, the users, their roles, and their grants.
For the grants of an administrator other than `default`, see [ClickHouse setup hook](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts#clickhouse-setup-hook).
Create the users manually when the data lake is on a separate instance, or when the chart cannot use an administrator account.

### Create the users manually

To create the users manually:

1. Save the setup statements from the GitLab Orbit image:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:<orbit_version> \
     /usr/share/gkg/clickhouse-setup.sql > clickhouse-setup.sql
   ```

   Replace the following values:

   - `<orbit_version>`: The GitLab Orbit release that you deploy.

1. In `clickhouse-setup.sql`, replace `${GRAPH_DB}` with `orbit` and `${DATALAKE_DB}` with `gitlab_clickhouse_main_production`.
1. Replace `${GKG_WRITER_PASSWORD}`, `${GKG_READER_PASSWORD}`, and `${GKG_SIPHON_READER_PASSWORD}` with a new password for each user.
1. Save the data lake statements, which name `gkg_siphon_reader`, to `datalake.sql`:

   ```shell
   grep gkg_siphon_reader clickhouse-setup.sql > datalake.sql
   ```

1. Save all other statements to `graph.sql`:

   ```shell
   grep -v gkg_siphon_reader clickhouse-setup.sql > graph.sql
   ```

1. As a ClickHouse administrator, run `datalake.sql` on the data lake instance and `graph.sql` on the graph instance.
1. In `orbit-values.yaml`, set `clickhouseSetup.enabled` to `false`.
