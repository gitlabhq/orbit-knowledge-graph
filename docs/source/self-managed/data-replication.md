---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Configure PostgreSQL logical replication and install Siphon so GitLab data reaches ClickHouse.
title: Set up data replication
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

GitLab Orbit reads GitLab data from a copy of the GitLab database in ClickHouse, not from the
GitLab database itself. [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) keeps
that copy up to date.

Siphon runs as three deployments on Kubernetes:

| Deployment | Role |
|------------|------|
| Producer | Reads the PostgreSQL write-ahead log and publishes each changed row to NATS. |
| Consumer | Reads NATS and writes the rows into ClickHouse. |
| Reconciler | Recomputes derived columns, such as the namespace traversal path, on a schedule, and republishes the affected rows through NATS. |

Prerequisites:

- The [prerequisites](getting-started.md#prerequisites) for GitLab Orbit on GitLab Self-Managed.
- A ClickHouse instance set up for GitLab, with the GitLab ClickHouse migrations applied. For more
  information, see [ClickHouse](getting-started.md#clickhouse).
- Superuser access to the GitLab PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- Helm 3 and `kubectl` access to the cluster.

Set up replication in this order:

1. Turn on logical replication in PostgreSQL.
1. Create the Siphon PostgreSQL users.
1. Create the publication and grants.
1. Create the Siphon ClickHouse user.
1. Make the passwords available to Siphon.
1. Install Siphon.

## Turn on logical replication in PostgreSQL

The Siphon producer stops at startup unless `wal_level` is `logical`. Changes to `wal_level`,
`max_replication_slots`, and `max_wal_senders` all require a full PostgreSQL restart, not a reload, and
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

1. Save the file, reconfigure GitLab, then restart PostgreSQL:

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
   restart. To recover, correct `/etc/gitlab/gitlab.rb`, reconfigure, and confirm the service is running.

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab Helm chart does not manage a production PostgreSQL, so apply these settings on your own server
or managed database.

1. Set the following parameters:

   | Parameter | Value |
   |-----------|-------|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10` or more |
   | `max_wal_senders` | `10` or more |

   On a managed database, set them through the parameter group of the provider rather than in
   `postgresql.conf`. Some providers expose `wal_level` under a different name, such as a logical
   replication flag.

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

Siphon uses three roles. Create them as a superuser. A role can grant `REPLICATION` only if it already has
the `REPLICATION` attribute, and the GitLab application role does not have it.

Connect to `gitlabhq_production` as a superuser and run:

```sql
CREATE USER siphon WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

The three roles can use the same password.

With the values file in this procedure, Siphon connects as `siphon_replicator` for every connection. The
other two roles exist because the Rake task in the next step also grants them read access. A split-user
setup can use them later.

## Create the publication and grants

GitLab ships a Rake task that prepares PostgreSQL for Siphon. The task is idempotent and creates:

- The publication.
- The helper function Siphon calls to add tables to the publication.
- Read access to every GitLab schema.

GitLab 19.2.2 and later includes the task.

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

The task creates a publication named `siphon_publication_main_1` and grants `EXECUTE` on
`public.siphon_alter_publication` to the `siphon` role. The producer calls the function on the
`siphon_replicator` connection, so grant the same `EXECUTE` permission to `siphon_replicator`. As a
superuser, run:

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

The publication is empty until the producer starts. Siphon then adds tables to the publication through the
same function.

## Create the Siphon ClickHouse user

Siphon writes into the same database GitLab already uses. Siphon requires no schema rights, because the
GitLab ClickHouse migrations create the target tables.

Connect to ClickHouse as an administrator and run:

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY '<your_password>';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

The `dictGet` grant is required. Without it, the pods still pass their health checks, because the probe
only scrapes the metrics endpoint. The failure appears as permission errors in the consumer log. Every
table backed by a dictionary stays empty.

On a managed ClickHouse that restricts the `system` database, also run:

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## Make the passwords available to Siphon

Siphon reads both passwords from environment variables backed by a Kubernetes Secret. The namespace must
exist before you create the Secret. The values file in the following section expects one Secret named
`siphon-secrets` in the Siphon namespace, with these keys:

| Key | Holds |
|-----|-------|
| `pg-password` | The password for the `siphon`, `siphon_replicator`, and `siphon_snapshot` PostgreSQL roles |
| `ch-siphon-password` | The password for the ClickHouse `siphon` user |

You should keep the values in the secret manager you already run. Sync them into the cluster with a tool
such as the External Secrets Operator. Do not store the plaintext elsewhere.

## Install Siphon

If you use `configMode: split`, Siphon does not include its own table list. At pod startup, Siphon mounts the
`gitlab-siphon-tables` image, which contains only the table definitions from the GitLab source tree. GitLab
publishes the `gitlab-siphon-tables` image for every release. This means that Siphon's table list matches your
GitLab version without manual updates.

The image is `registry.gitlab.com/gitlab-org/gitlab/gitlab-siphon-tables`. It is not part of the Siphon
project. `global.gitlabVersion` selects its tag and pins the replicated table set to your GitLab version.
Tags start at `v19.2.0-ee`. Before you install, confirm the tag exists in the
[`gitlab-siphon-tables` container registry](https://gitlab.com/gitlab-org/gitlab/container_registry/11634641).
A missing tag prevents pods from starting.

1. Save the following as `siphon-values.yaml` and replace the placeholders:

   ```yaml
   configMode: split

   global:
     # Tag of registry.gitlab.com/gitlab-org/gitlab/gitlab-siphon-tables.
     # Must match your GitLab version exactly, patch level included.
     gitlabVersion: v19.2.2-ee

   image:
     repository: registry.gitlab.com/gitlab-org/analytics-section/siphon
     tag: 0.0.124-beta

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

1. Install the chart:

   ```shell
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm repo update

   helm upgrade --install siphon siphon/siphon \
     --version 1.18.0 \
     --namespace siphon \
     --create-namespace \
     --values siphon-values.yaml
   ```

   These commands are a reference for a direct Helm install. Adjust the namespace names and the
   deployment method to match your own tooling.

1. Confirm that all three deployments are running:

   ```shell
   kubectl -n siphon get pods
   ```

The output lists the `postgres-producer`, `clickhouse-consumer`, and `reconciler` pods in the `Running`
state.

Siphon does not restart its pods when you change the values file. To apply a change, restart the
deployments:

```shell
kubectl -n siphon rollout restart deployment
```

### Values file settings

| Setting | Requirement |
|---------|-------------|
| `database_mapping` | Required. The GitLab schema is split across three logical databases (`main`, `ci`, and `sec`), whether or not your instance stores them separately. Without the mapping, the generator fails. Do not remove the table definitions that reference `ci` and `sec` instead, because that silently drops every CI, vulnerability, and dependency table. |
| `stream_name` | Must match the stream name GitLab Orbit reads. For the full list of values both sides share, see [Shared configuration values](getting-started.md#shared-configuration-values). |
| `advisory_lock_id` and the lock timeouts | Required. The producer stops at startup without them. |
| `nats_config.replicas` | Must match your NATS cluster size. A single NATS server supports only one replica. |
| `ssl_mode` | Must match what the PostgreSQL server offers. The example uses `require`, which works with a Linux package PostgreSQL because it serves TLS by default. A server that does not serve TLS refuses the connection, and the producer stops at startup with `server refused TLS connection`. |
| `connection.replication.use_alter_publication_function` | Must stay `true`, which is the chart default. The `EXECUTE` grant on `public.siphon_alter_publication` exists for this setting: the publication belongs to the GitLab database user, so a direct `ALTER PUBLICATION` from a Siphon role fails. |
| `max_age_seconds` | Controls how far back a consumer can replay. Retaining 15 days of every changed row across more than 60 tables produces a large JetStream file store. Size the NATS volume for the full retention window, or lower the value. |

### Object storage for large rows

Rows too large for the stream go to an object store. The preceding values file does not configure one, so
Siphon uses a NATS JetStream object store and needs no credentials.

To use an external bucket, add `object_storage_config` under `queueing` with `identifier`, `type`, and
`bucket_name`, and give every Siphon pod credentials for the bucket. With `type: s3`, Siphon follows the
standard AWS SDK chain, so set `AWS_REGION` and either attach an instance role or pass
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` through `env` and `envFromSecrets`. For an S3-compatible
service, also set `AWS_ENDPOINT_URL_S3`. For Google Cloud Storage, use `type: gcs` with application default
credentials.

## Verify replication

The first copy processes one table at a time and pauses replication while each table is merged. The
duration therefore depends on the number of tables, not on the amount of data. A GitLab 19.2.2 instance
replicates more than 60 tables. These checks confirm that replication is running. They do not confirm that
the first copy is complete.

1. Confirm that Siphon is reading PostgreSQL. The slot must be active, and `confirmed_flush_lsn` must
   advance after a write in GitLab:

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. Confirm that rows are arriving in ClickHouse:

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   Compare the counts with the same tables in PostgreSQL. `FINAL` is required, because the tables are
   `ReplacingMergeTree` and an updated row appears more than once until a background merge completes. The
   ClickHouse counts are usually slightly higher, because a table can receive a placeholder row on first
   write.

> [!warning]
> An inactive replication slot holds write-ahead log on the GitLab PostgreSQL disk and can fill it. If you
> stop Siphon for longer than your disk headroom allows, drop the slot with
> `SELECT pg_drop_replication_slot('siphon_main_1_slot');` and take a fresh snapshot later.

## Next step

- [Set up GitLab Orbit](orbit-setup.md)
