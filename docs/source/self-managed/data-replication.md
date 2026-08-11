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

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.3.

{{< /history >}}

GitLab Orbit does not read your GitLab database. It reads a copy of it in ClickHouse, kept up to date by
[Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon).

Siphon runs as three deployments on Kubernetes. The producer reads the PostgreSQL write-ahead log and
publishes each changed row to NATS. The consumer reads NATS and writes the rows into ClickHouse. The
reconciler recomputes derived columns, such as the namespace traversal path, on a schedule and
republishes the affected rows through NATS.

Prerequisites:

- The [prerequisites](getting-started.md#prerequisites), including a ClickHouse instance already set up
  for GitLab with its migrations applied.
- Superuser access to the GitLab PostgreSQL server, and a maintenance window to restart it once.
- Helm 3 and `kubectl` access to the cluster.

## Step 1: Turn on logical replication in PostgreSQL

Siphon refuses to start unless `wal_level` is `logical`. This setting needs a full PostgreSQL restart,
not a reload.

{{< tabs >}}

{{< tab title="Linux package" >}}

1. Edit `/etc/gitlab/gitlab.rb`:

   ```ruby
   postgresql['wal_level'] = 'logical'
   postgresql['max_replication_slots'] = 10
   postgresql['max_wal_senders'] = 10

   # Let the cluster reach PostgreSQL. Replace with the address and CIDR that fit your network.
   postgresql['listen_address'] = '0.0.0.0'
   postgresql['md5_auth_cidr_addresses'] = ['10.0.0.0/8']

   # Keep GitLab itself on the local socket. Without this, setting listen_address
   # also repoints GitLab at that address and it loses its database connection.
   gitlab_rails['db_host'] = '/var/opt/gitlab/postgresql'
   ```

1. Reconfigure, then restart PostgreSQL:

   ```shell
   sudo gitlab-ctl reconfigure
   sudo gitlab-ctl restart postgresql
   ```

   Reconfigure alone does not restart PostgreSQL, so the new `wal_level` does not take effect until you
   restart it.

1. Confirm the setting:

   ```shell
   sudo gitlab-psql -c 'SHOW wal_level'
   ```

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

The GitLab Helm chart does not manage a production PostgreSQL, so apply these settings on your own
server or managed database.

1. Set the following parameters:

   | Parameter | Value |
   |---|---|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10` or higher |
   | `max_wal_senders` | `10` or higher |

   On a managed database, set them through the provider's parameter group rather than in
   `postgresql.conf`. Some providers expose `wal_level` under a different name, such as a
   logical replication flag.

1. Restart the server. All three parameters need a restart.

1. Confirm the setting:

   ```shell
   psql -h <postgresql-host> -U <admin-user> -d gitlabhq_production -c 'SHOW wal_level'
   ```

1. Allow connections from the cluster to reach port 5432.

{{< /tab >}}

{{< /tabs >}}

## Step 2: Create the Siphon PostgreSQL users

Siphon uses three roles. Creating them needs a superuser, because granting `REPLICATION` needs a role
that already has it, and the GitLab application role does not.

Connect to `gitlabhq_production` as a superuser and run:

```sql
CREATE USER siphon WITH PASSWORD 'PASSWORD_HERE'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD 'PASSWORD_HERE'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD 'PASSWORD_HERE'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

The three roles can share one password.

In the configuration on this page, Siphon connects as `siphon_replicator` for all of its work. The other
two roles exist so that the task in the next step grants them read access as well, which keeps the door
open for a split-user setup later.

## Step 3: Create the publication and grants

GitLab ships a task that installs the publication, the helper function Siphon calls to add tables to
it, and read access to every GitLab schema. It is idempotent. Confirm it is present first with
`gitlab-rake -T | grep gitlab:siphon:setup`, because it was added in 19.3.

{{< tabs >}}

{{< tab title="Linux package" >}}

```shell
sudo gitlab-rake gitlab:siphon:setup
```

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

```shell
kubectl -n <gitlab-namespace> exec -it deploy/<release>-toolbox -- gitlab-rake gitlab:siphon:setup
```

Find the deployment with `kubectl -n <gitlab-namespace> get deploy -l app=toolbox` if you are not sure
of the release name.

{{< /tab >}}

{{< /tabs >}}

The task creates a publication named `siphon_publication_main_1` and grants `EXECUTE` on
`public.siphon_alter_publication` to the `siphon` role. Grant it to `siphon_replicator` as well,
because the producer calls the function on that connection. As a superuser, run:

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

The publication is empty at this point. Siphon fills it through that function when the producer starts.

## Step 4: Give Siphon a ClickHouse user

Siphon writes into the same database GitLab already uses. It needs no schema rights, because the GitLab
ClickHouse migrations create the target tables.

Connect to ClickHouse as an administrator and run:

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY 'PASSWORD_HERE';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

`dictGet` is not optional. Without it the pods still pass their health checks, because the probe only
scrapes the metrics endpoint. The failure shows up as permission errors in the consumer log while every
table backed by a dictionary stays empty.

On a managed ClickHouse that restricts the `system` database, also run:

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## Step 5: Store the passwords in Kubernetes

```shell
kubectl create namespace siphon

kubectl -n siphon create secret generic siphon-secrets \
  --from-literal=pg-password='PASSWORD_HERE' \
  --from-literal=ch-siphon-password='PASSWORD_HERE'
```

## Step 6: Install Siphon

Setting `configMode: split` makes Siphon build its table list at pod startup from an image that ships
with GitLab. The list then always matches your GitLab version and you never maintain it by hand.

1. Save the following as `siphon-values.yaml`. Replace the placeholders, and set `global.gitlabVersion`
   to your GitLab version in tag form. That value is an image tag, so check it exists in the
   [container registry](https://gitlab.com/gitlab-org/gitlab/container_registry) first. A tag that does
   not exist stops the pods before they start. A tag one patch behind is safe.

   ```yaml
   configMode: split

   global:
     # Tag of the gitlab-siphon-tables image. Must match your GitLab version
     # exactly, patch level included. Tags start at v19.2.0-ee.
     gitlabVersion: v19.3.0-ee

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
           host: <clickhouse-host>
           # Native protocol, not the HTTP port GitLab uses.
           port: 9000
           ssl: false
           username: siphon
           password: "${CLICKHOUSE_SIPHON_PASSWORD}"
           database: gitlab_clickhouse_main_production
         databases:
           main:
             host: <postgresql-host>
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
       # Point the producer at the publication Step 3 created. Without this it derives the
       # name from the application identifier and tries to create its own.
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
     --values siphon-values.yaml
   ```

1. Check that all three deployments are running:

   ```shell
   kubectl -n siphon get pods
   ```

Siphon does not restart its pods when you change the values file. After a change, run
`kubectl -n siphon rollout restart deployment` so the new configuration takes effect.

### Notes on the values file

- `database_mapping` is not optional. The GitLab schema is split across three logical databases
  (`main`, `ci`, and `sec`) whether or not your instance stores them separately. Without the mapping,
  the generator refuses to run. Do not remove the table definitions that reference `ci` and `sec`
  instead, because that silently drops every CI, vulnerability, and dependency table.
- `stream_name` must match the value GitLab Orbit reads. It appears again in
  [Set up GitLab Orbit](orbit-setup.md).
- The lock timeouts and `advisory_lock_id` are required. The producer stops at startup without them.
- Set `nats_config.replicas` to match your NATS cluster size. A single NATS server supports only one
  replica.
- `connection.replication.use_alter_publication_function` must stay `true`. The chart sets it by
  default, and Step 3 depends on it: the publication belongs to the GitLab database user, so a direct
  `ALTER PUBLICATION` from a Siphon role fails.
- `max_age_seconds` is how far back a consumer can replay. Fifteen days of every changed row across
  roughly 60 tables is a large JetStream file store, so size the NATS volume for it or lower the value.
- Rows too large for the stream go to an object store. The values file above leaves that alone, so
  Siphon uses a NATS JetStream object store and needs no credentials. To use an external bucket
  instead, add `object_storage_config` under `queueing` with `identifier`, `type`, and `bucket_name`,
  and give every Siphon pod credentials for it. With `type: s3` Siphon follows the standard AWS SDK
  chain, so set `AWS_REGION` and either attach an instance role or pass `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` through `env` and `envFromSecrets`. For an S3-compatible service, also set
  `AWS_ENDPOINT_URL_S3`. For Google Cloud Storage, use `type: gcs` with application default credentials.

## Verify

The first copy takes one table at a time and pauses replication while each one is merged, so the time it
takes tracks the number of tables rather than the amount of data. A 19.3 instance replicates a little
over 60 tables. These checks confirm the path works rather than that it has finished.

1. PostgreSQL is being read. The slot should be active, and its position should move after a write in
   GitLab:

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. Rows are arriving in ClickHouse:

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   Compare the counts with the same tables in PostgreSQL. `FINAL` matters, because the tables are
   `ReplacingMergeTree` and an updated row appears more than once until a background merge. Expect the
   ClickHouse side to still run slightly high, because a table can pick up a placeholder row on first
   write.

> [!warning]
> An inactive replication slot holds write-ahead log on the GitLab PostgreSQL disk and can fill it. If
> you stop Siphon for longer than your disk headroom allows, drop the slot with
> `SELECT pg_drop_replication_slot('siphon_main_1_slot');` and take a fresh snapshot later.

## Next step

[Set up GitLab Orbit](orbit-setup.md).
