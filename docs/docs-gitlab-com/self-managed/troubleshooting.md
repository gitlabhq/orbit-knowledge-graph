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
