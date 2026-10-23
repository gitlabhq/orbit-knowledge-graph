---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Configure PostgreSQL logical replication and install Siphon so GitLab data reaches ClickHouse.
title: Set up data replication for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.2.2.
- Generally available in GitLab 19.5.

{{< /history >}}

GitLab Orbit reads GitLab data from a copy of the GitLab database in ClickHouse.
[Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) keeps that copy up to date.
It runs as three deployments on Kubernetes:

| Deployment | Role |
|------------|------|
| Producer | Reads the PostgreSQL write-ahead log and publishes each changed row to NATS. |
| Consumer | Reads NATS and writes the rows into ClickHouse. |
| Reconciler | Recomputes derived columns, such as the namespace traversal path, on a schedule, and republishes the affected rows through NATS. |

Prerequisites:

- The [prerequisites](getting-started.md#prerequisites) for GitLab Orbit on GitLab Self-Managed.
- ClickHouse set up for GitLab, with the GitLab ClickHouse migrations applied.
  For more information, see [ClickHouse](getting-started.md#clickhouse).
- Superuser access to the GitLab PostgreSQL server.
- Helm 3 and `kubectl` access to the cluster.

<!-- TODO(GA): update the Siphon image and chart versions in the examples to the GA release. -->
Complete the sections on this page in order. The versions in the commands are examples.
Use the latest [Siphon release](https://gitlab.com/gitlab-org/analytics-section/siphon/-/releases)
and [Siphon Helm chart](https://gitlab.com/gitlab-org/analytics-section/platform-insights/siphon-helm-charts/-/releases).

## Turn on logical replication in PostgreSQL

The Siphon producer stops at startup unless `wal_level` is `logical`. Changes to `wal_level`,
`max_replication_slots`, and `max_wal_senders` require a full PostgreSQL restart, not a reload.
`gitlab-ctl reconfigure` does not restart PostgreSQL.

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   postgresql['wal_level'] = 'logical'
   postgresql['max_replication_slots'] = 10
   postgresql['max_wal_senders'] = 10

   # Accept connections from the cluster. Replace with the address and CIDR for your network.
   postgresql['listen_address'] = '0.0.0.0'
   postgresql['md5_auth_cidr_addresses'] = ['10.0.0.0/8']

   # Keep GitLab itself on the local socket. Without this, setting listen_address
   # also repoints GitLab at that address, and GitLab loses its database connection.
   gitlab_rails['db_host'] = '/var/opt/gitlab/postgresql'
   ```

1. Reconfigure GitLab, then restart PostgreSQL:

   ```shell
   sudo gitlab-ctl reconfigure
   sudo gitlab-ctl restart postgresql
   ```

1. Confirm the settings:

   ```shell
   sudo gitlab-psql -c 'SHOW wal_level'
   sudo gitlab-psql -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level` returns `logical`, the other two match `gitlab.rb`, and no row reports
   `pending_restart = t`. A partially applied combination leaves PostgreSQL unable to start at its next
   restart. To recover, correct `/etc/gitlab/gitlab.rb`, reconfigure, and confirm that the service runs.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab Helm chart does not manage a production PostgreSQL, so apply these settings on your own server
or managed database.

1. Set these parameters. On a managed database, use the parameter group of the provider.
   Some providers expose `wal_level` under a different name, such as a logical replication flag.

   | Parameter | Value |
   |-----------|-------|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10` or more |
   | `max_wal_senders` | `10` or more |

1. Restart the server.
1. Allow connections from the cluster to reach port 5432.
1. Confirm the settings:

   ```shell
   psql -h <postgresql_host> -U <admin_user> -d gitlabhq_production \
     -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level` returns `logical`, the other two match the values you set, and no row reports
   `pending_restart = t`.

{{< /tab >}}

{{< /tabs >}}

## Create the Siphon PostgreSQL users

Create the three Siphon roles as a superuser. Only a role with the `REPLICATION` attribute can grant it,
and the GitLab application role does not have it.

Connect to `gitlabhq_production` as a superuser and run:

```sql
CREATE USER siphon WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

The roles can share one password. The values file on this page connects as `siphon_replicator` only.
The Rake task in the next section also grants the other two roles read access, for a later split-user setup.

## Create the publication and grants

GitLab 19.2.2 and later includes an idempotent Rake task that creates the publication, the helper function that
Siphon calls to add tables to it, and read access to every GitLab schema.

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. Confirm that the task exists:

   ```shell
   sudo gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. Run the task:

   ```shell
   sudo gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

1. Find the toolbox deployment:

   ```shell
   kubectl -n <gitlab_namespace> get deploy -l app=toolbox
   ```

1. Confirm that the task exists:

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. Run the task:

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< /tabs >}}

The task creates the publication `siphon_publication_main_1` and grants `EXECUTE` on
`public.siphon_alter_publication` to the `siphon` role. The producer calls the function on the
`siphon_replicator` connection, so grant it to `siphon_replicator` too. As a superuser, run:

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

The publication stays empty until the producer starts and adds tables through the same function.

## Create the Siphon ClickHouse user

Siphon writes into the database that GitLab already uses. It needs no schema rights, because the
GitLab ClickHouse migrations create the target tables.

Connect to ClickHouse as an administrator and run:

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY '<your_password>';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

The `dictGet` grant is required. Without it, every table backed by a dictionary stays empty, and the consumer
log shows permission errors. The pods still pass their health checks, because the probe only scrapes the
metrics endpoint.

On a managed ClickHouse that restricts the `system` database, also run:

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## Make the passwords available to Siphon

Siphon reads both passwords from environment variables backed by a Kubernetes Secret.
Create the Siphon namespace first.
The values file in the next section expects a Secret named `siphon-secrets` in the Siphon namespace,
with these keys:

| Key | Holds |
|-----|-------|
| `pg-password` | The password for the `siphon`, `siphon_replicator`, and `siphon_snapshot` PostgreSQL roles |
| `ch-siphon-password` | The password for the ClickHouse `siphon` user |

Sync the values from the secret manager that you already run, for example with the External Secrets Operator.
Do not store the plaintext elsewhere.

## Install Siphon

With `configMode: split`, Siphon reads its table list from the `gitlab-siphon-tables` image at pod startup,
so the list matches your GitLab version without manual updates. The GitLab cloud native images (CNG) build
publishes the image, `registry.gitlab.com/gitlab-org/build/cng/gitlab-siphon-tables`, for every release from
GitLab 19.5.0, and for some earlier patch releases. `global.gitlabVersion` selects its tag. The chart converts
the version to the CNG tag format, `vX.Y.Z`, so `19.5.0`, `v19.5.0`, and `v19.5.0-ee` all select the `v19.5.0` tag.
Confirm that the tag exists in the
[`gitlab-siphon-tables` container registry](https://gitlab.com/gitlab-org/build/CNG/container_registry/11611957).
A missing tag prevents pods from starting.

CNG publishes no tag for GitLab versions earlier than 19.3.4, or for GitLab 19.4.0 and 19.4.1. For these
versions, use the deprecated `registry.gitlab.com/gitlab-org/gitlab/gitlab-siphon-tables` image, which GitLab
publishes with `vX.Y.Z-ee` tags. Add this block to `siphon-values.yaml`:

```yaml
splitTables:
  image:
    repository: gitlab-org/gitlab/gitlab-siphon-tables
    tag: '{{ include "siphon.gitlabVersion" . }}-ee'
```

For more information, see
[Using the legacy tables image](https://gitlab.com/gitlab-org/analytics-section/platform-insights/siphon-helm-charts#using-the-legacy-tables-image).
Switch to the CNG image after you upgrade GitLab.

1. Save the following as `siphon-values.yaml` and replace the placeholders:

   ```yaml
   configMode: split

   global:
     # Selects the tag of registry.gitlab.com/gitlab-org/build/cng/gitlab-siphon-tables.
     # Must match your GitLab version exactly, patch level included.
     gitlabVersion: v19.5.0

   image:
     repository: registry.gitlab.com/gitlab-org/analytics-section/siphon
     tag: 0.0.137-beta

   waitForMigrations:
     enabled: false

   siphonConnectionConfigMap:
     create: true
     data:
       prometheus:
         port: 8080
       connection:
         queueing:
           driver: nats
           url: nats://nats.nats.svc.cluster.local:4222
           stream_name: siphon_stream_main_db
           nats_config:
             replicas: 1
             max_age_seconds: 1296000
         clickhouse:
           host: <clickhouse_host>
           # Native protocol, not the HTTP port GitLab uses.
           port: 9000
           ssl: false
           username: siphon
           password: "${CLICKHOUSE_SIPHON_PASSWORD}"
           database: gitlab_clickhouse_main_production
         databases:
           main:
             host: <postgresql_host>
             port: 5432
             database: gitlabhq_production
             user: siphon_replicator
             password: "${SIPHON_DB_PASSWORD}"
             ssl_mode: require
             advisory_lock_id: 1
             application_name: siphon_main_1
             advisory_lock_timeout_ms: 100
             advisory_lock_timeout_fuzziness_ms: 50
             lock_timeout_ms: 500
             lock_timeout_fuzziness_ms: 300
       overrides:
         siphon_main_1:
           database_ref: main

   siphonLayoutConfigMap:
     create: true
     data:
       stream_name: siphon_stream_main_db
       refresh_mode: inline
       partitions_monitoring_interval_in_seconds: 3600
       max_column_size_in_bytes: 10485760
       producers:
         main:
           - siphon_main_1
       consumers:
         - siphon_consumer_1
       reconcilers:
         - siphon_reconciler_1
       # Point the producer at the publication the Rake task created. Without this
       # override, the producer derives the name from the application identifier
       # and tries to create its own.
       replication_overrides:
         siphon_main_1:
           publication_name: siphon_publication_main_1
       # GitLab splits its schema three ways even on one database. Map ci and sec onto main.
       database_mapping:
         ci: main
         sec: main

   deployments:
     postgres-producer:
       configMode: split
       split:
         role: producer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     clickhouse-consumer:
       configMode: split
       split:
         role: consumer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     reconciler:
       configMode: split
       split:
         role: reconciler
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
   ```

1. Install the chart. Adapt the namespace and the install method to your own tooling.

   ```shell
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm repo update

   helm upgrade --install siphon siphon/siphon \
     --version 2.0.0 \
     --namespace siphon \
     --create-namespace \
     --values siphon-values.yaml
   ```

1. Confirm that the `postgres-producer`, `clickhouse-consumer`, and `reconciler` pods are `Running`:

   ```shell
   kubectl -n siphon get pods
   ```

Siphon does not restart its pods when you change the values file. To apply a change, run
`kubectl -n siphon rollout restart deployment`.

### Values file settings

| Setting | Requirement |
|---------|-------------|
| `database_mapping` | Required. The GitLab schema is split across three logical databases (`main`, `ci`, and `sec`), whether or not your instance stores them separately. Without the mapping, the generator fails. Do not remove the table definitions that reference `ci` and `sec` instead, because that silently drops every CI, vulnerability, and dependency table. |
| `waitForMigrations.enabled` | `false` for a Linux package instance. Otherwise, Siphon pods wait for a GitLab migrations job that does not exist in the cluster, and never start. For the GitLab Helm chart, you can set it to `true` with `waitForMigrations.namespace` set to the GitLab namespace. Siphon then starts only after GitLab completes the database migrations for `global.gitlabVersion`. |
| `stream_name` | Must match the stream name that GitLab Orbit reads. See [shared configuration values](getting-started.md#shared-configuration-values). |
| `advisory_lock_id` and the lock timeouts | Required. The producer stops at startup without them. |
| `nats_config.replicas` | Must match your NATS cluster size. A single NATS server supports only one replica. |
| `ssl_mode` | Must match what the PostgreSQL server offers. `require` works with a Linux package PostgreSQL, which serves TLS by default. A server without TLS refuses the connection, and the producer stops at startup with `server refused TLS connection`. |
| `connection.replication.use_alter_publication_function` | Must stay `true`, the chart default. The publication belongs to the GitLab database user, so a direct `ALTER PUBLICATION` from a Siphon role fails. The `EXECUTE` grant on `public.siphon_alter_publication` exists for this setting. |
| `connection.clickhouse.connection_settings` | Required on a replicated ClickHouse cluster: `insert_quorum: "auto"`, `insert_quorum_parallel: "0"`, and `async_insert: "0"`. With the default `select_sequential_consistency` reads, they make the delete and refresh queries of Siphon safe against a lagging replica. Leave them out on a single node and on ClickHouse Cloud. See [high availability](getting-started.md#high-availability). |
| `max_age_seconds` | Controls how far back a consumer can replay. Retaining 15 days of every changed row across more than 60 tables produces a large JetStream file store. Size the NATS volume for the full retention window, or lower the value. |

### Object storage for large rows

The values file does not configure an object store, so Siphon uses a NATS JetStream object store and needs
no credentials.

To use an external bucket, add `object_storage_config` under `queueing` with `identifier`, `type`, and
`bucket_name`, and give every Siphon pod credentials for the bucket:

- `type: s3`: Siphon follows the standard AWS SDK chain. Set `AWS_REGION`, and either attach an instance role
  or pass `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` through `env` and `envFromSecrets`.
  For an S3-compatible service, also set `AWS_ENDPOINT_URL_S3`.
- `type: gcs`: Siphon uses application default credentials.

## Verify replication

The first copy processes one table at a time and pauses replication while it merges each table, so its
duration depends on the number of tables, not on the amount of data. A GitLab 19.2.2 instance replicates
more than 60 tables. These checks confirm that replication runs, not that the first copy is complete.

1. Confirm that Siphon reads PostgreSQL. The slot must be active, and `confirmed_flush_lsn` must
   advance after a write in GitLab:

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. Confirm that rows arrive in ClickHouse, and compare the counts with the same tables in PostgreSQL:

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   `FINAL` is required, because the tables are `ReplacingMergeTree` and an updated row appears more than once
   until a background merge completes. The ClickHouse counts are usually slightly higher, because a table can
   receive a placeholder row on first write.

> [!warning]
> An inactive replication slot holds write-ahead log on the GitLab PostgreSQL disk and can fill it. If you
> stop Siphon for longer than your disk headroom allows, drop the slot with
> `SELECT pg_drop_replication_slot('siphon_main_1_slot');` and take a fresh snapshot later.

## Next step

- [Set up GitLab Orbit](orbit-setup.md)
