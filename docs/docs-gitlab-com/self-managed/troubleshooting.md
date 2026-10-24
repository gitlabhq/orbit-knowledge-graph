---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find and fix common problems with GitLab Orbit on GitLab Self-Managed.
title: Troubleshooting GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

When a request to GitLab Orbit fails, check these logs first:

- The GitLab Rails log `knowledge_graph.log`, and the Workhorse log.
  For their locations, see [Log system](https://docs.gitlab.com/administration/logs/).
- The GitLab Orbit webserver log:

  ```shell
  kubectl -n gitlab-orbit logs deployment/orbit-webserver
  ```

Then check the endpoint, the certificate, and the load balancer against
[TLS and network requirements](#tls-and-network-requirements).

## Error: `missing selected ALPN property`

GitLab logs `credentials: cannot check peer: missing selected ALPN property`.
This issue occurs when a load balancer or proxy between GitLab and the webserver does not pass HTTP/2.

To resolve this issue, forward TCP at the load balancer, and terminate TLS on the webserver.

## Error: `i/o timeout` on port 50054

This issue occurs on AWS when GitLab connects to one fixed Network Load Balancer address,
and the webserver pod runs in a different zone.

To resolve this issue, turn on cross-zone load balancing.
For more information, see [TLS and network requirements](#tls-and-network-requirements).

## Error: `ontology archive vNN is missing`

The dispatcher stops with `ontology archive vNN is missing; seed the catalog from the exact deployed release before upgrading`.
This issue occurs when NATS does not hold the schema archive of the active graph, and the running image does not include it.
For example, the archive was never published, the NATS volume was reset, or the graph comes from a release that the running image cannot upgrade.

To resolve this issue, point `clickhouse.graph.database` at a new database, upgrade GitLab Orbit, and let it index again.
