---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find and fix common problems with GitLab Orbit on GitLab Self-Managed.
title: Troubleshooting GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

When a request to GitLab Orbit fails, check these logs first:

- The GitLab Rails log `knowledge_graph.log` and the Workhorse log.
  For their locations, see [Log system](https://docs.gitlab.com/administration/logs/).
- The GitLab Orbit webserver log:

  ```shell
  kubectl -n gitlab-orbit logs deployment/orbit-webserver
  ```

Then check the endpoint, the certificate, and the load balancer against [Networking for GitLab Orbit](networking.md).

## Error: `missing selected ALPN property`

GitLab logs `credentials: cannot check peer: missing selected ALPN property`.
This issue occurs when a load balancer or proxy between GitLab and the webserver does not pass HTTP/2.

To resolve this issue, forward TCP at the load balancer, and terminate TLS on the webserver.

## Error: `i/o timeout` on port 50054

This issue occurs on AWS when GitLab connects to one fixed Network Load Balancer address, and the webserver pod runs in a different zone.

To resolve this issue, turn on cross-zone load balancing in the webserver Service annotations.

## Error: `ontology archive vNN is missing`

The dispatcher stops with `ontology archive vNN is missing; seed the catalog from the exact deployed release before upgrading`.
This issue occurs when NATS does not hold the schema archive of the active graph, and the running image does not include it.
For example, the archive was never published, the NATS volume was reset, or the graph comes from a release that the running image cannot upgrade.

To resolve this issue, point `clickhouse.graph.database` at a new database, upgrade GitLab Orbit, and wait for the first index to finish.
On a replicated cluster, create the database first.
If you created the users manually, run `graph.sql` again with `${GRAPH_DB}` set to the new database.
After the first index finishes, you can drop the old database to free disk.

## GitLab pods stay in `ContainerCreating`

After you turn on GitLab Orbit in the GitLab Helm chart values, new GitLab pods stay in `ContainerCreating`.

This issue occurs when the Secret that `global.appConfig.orbit.jwtSecret` names does not exist in the GitLab namespace.
The GitLab chart does not generate the key.

To resolve this issue, create the Secret, then upgrade GitLab again.
For the steps, see [Turn on GitLab Orbit in GitLab](orbit-setup.md#turn-on-gitlab-orbit-in-gitlab).

## Error: `insecure non-private channel`

GitLab logs `Skipping authorization header on insecure non-private channel`, and every request to GitLab Orbit fails authentication.

This issue occurs when the gRPC endpoint has no `tls://` scheme, and its host name resolves to an address outside the loopback and private ranges.
GitLab then does not send the authorization token.

To resolve this issue, use an endpoint host name that resolves only to loopback or private addresses, or set the endpoint to `tls://` and terminate TLS on the webserver.
For the address ranges, see [Use a plaintext connection](networking.md#use-a-plaintext-connection).

## Siphon pods do not start

The Siphon pods never reach the `Running` state.

This issue occurs for one of these reasons:

- The `gitlab-siphon-tables` registry has no tag for the GitLab version in `global.gitlabVersion`.
- `waitForMigrations.enabled` is `true` with a Linux package instance.
  The pods wait for a GitLab migrations job in the cluster that does not exist.
- The cluster cannot mount image volumes, and the pods fail with `no such file` errors under `/etc/tables`.

To resolve this issue:

- Confirm that the [`gitlab-siphon-tables` registry](https://gitlab.com/gitlab-org/build/CNG/container_registry/11611957) has a tag for your GitLab version, and set `global.gitlabVersion` to that version.
- On a Linux package instance, set `waitForMigrations.enabled` to `false`.
- If your cluster cannot use image volumes, set `splitTables.volumeMode: initCopy`.
  For more information, see [Image volumes](getting-started.md#image-volumes).

Then run `helm upgrade` with the new values.
For the values file, see [Install Siphon](data-replication.md#install-siphon).

## PostgreSQL does not start after the restart

On a Linux package instance, PostgreSQL does not start after you restart it for logical replication.

This issue occurs when `/etc/gitlab/gitlab.rb` holds a partially applied combination of `wal_level`, `max_replication_slots`, and `max_wal_senders`.

To resolve this issue, correct the three settings in `/etc/gitlab/gitlab.rb`, run `sudo gitlab-ctl reconfigure`, then confirm that PostgreSQL is running.
For the values, see [Turn on logical replication in PostgreSQL](data-replication.md#turn-on-logical-replication-in-postgresql).

## Siphon logs permission errors, and the data lake tables stay empty

The Siphon consumer pod is `Running`, but its logs show permission errors from ClickHouse, and the tables in the data lake stay empty.

This issue occurs when the ClickHouse role of the Siphon user lacks the `dictGet` grant.

To resolve this issue, as a ClickHouse administrator, grant `dictGet` on the data lake database to the `siphon_app` role:

```sql
GRANT dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
```

Then restart the Siphon deployments:

```shell
kubectl -n siphon rollout restart deployment
```

For the full grants, see [Create the Siphon ClickHouse user](data-replication.md#create-the-siphon-clickhouse-user).

## CI/CD, vulnerability, and dependency tables are empty

Rows arrive in tables such as `siphon_namespaces` and `siphon_projects`, but the CI/CD, vulnerability, and dependency tables in the data lake stay empty, and Siphon logs no error.

This issue occurs when the table definitions that reference the `ci` and `sec` databases were removed from the Siphon configuration, for example to work around a generator failure caused by a missing `database_mapping`.
GitLab splits its schema across the `main`, `ci`, and `sec` databases, whether or not your instance stores them separately.

To resolve this issue, restore the table definitions, map `ci` and `sec` onto `main` in `database_mapping` in `siphon-values.yaml`, then restart the Siphon deployments.
For more information, see [Values file settings](data-replication.md#values-file-settings).

## Migrations stop after an upgrade

After an upgrade that changes the schema, the graph migrations stop, and GitLab Orbit does not switch to the new graph version.

This issue occurs when the GitLab Orbit ClickHouse users lack the `SELECT` grants on `system` tables that the setup statements define.
A managed ClickHouse that restricts the `system` database is a common cause.

To resolve this issue, apply the setup statements from the GitLab Orbit release that you deploy:

- With the chart setup hook on, run `helm upgrade` again.
  The hook applies the grants before the upgrade.
- If you created the users manually, run `graph.sql` and `datalake.sql` from the current image again.
  For the steps, see [Create the users manually](clickhouse.md#create-the-users-manually).

## The NATS volume is full

The persistent volume of NATS JetStream is full.

This issue occurs when `nats_config.max_age_seconds` is missing from `siphon-values.yaml`, or when the JetStream volume is too small for the window that it sets.

To resolve this issue, set `max_age_seconds` to the number of seconds of changed rows that the stream keeps, for example `1296000` for 15 days, and size the JetStream volume for that window.
For the volume setting, see [Configure NATS](getting-started.md#configure-nats).
For the size, see [NATS](sizing.md#nats).

## Queries return no rows after you turn on indexing

Right after you turn on indexing for a group, the verification query returns no rows, while the pods are `Running` and the `/-/readiness` endpoint passes.

This issue occurs when the first index has not finished.
The first index takes from several minutes to a few hours, and pod status does not confirm indexing.
An hourly sweep picks up any data that indexing missed.

To resolve this issue, check the indexing progress, and send the query again when the summary ends with `Ready.`:

```shell
glab orbit status --full-path <my-group>
```

For more information, see [Check indexing progress](../cli.md#check-indexing-progress).

## Recover from data loss

GitLab Orbit builds the graph from the data lake and from your repositories.
If you lose the graph database, index again to rebuild it.

Prerequisites:

- A data lake that Siphon still replicates to.

To rebuild the graph database:

1. Run the `helm upgrade` command from [Install GitLab Orbit](orbit-setup.md#install-gitlab-orbit).
   The setup hook creates the graph database again.
   On a replicated cluster, or if you create the users manually, create the database first.
   For the statement, see [High availability](clickhouse.md#high-availability).
1. Restart the GitLab Orbit pods:

   ```shell
   kubectl -n gitlab-orbit rollout restart deployment
   ```

   The dispatcher creates the graph tables in the empty database.
   The indexers then index every group with GitLab Orbit turned on.
1. After the first index finishes, run the query from [Verify the installation](orbit-setup.md#verify-the-installation).

## Related topics

- [Troubleshooting GitLab Orbit](../troubleshooting.md)
- [Monitor GitLab Orbit](observability.md)
