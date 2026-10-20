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

When you work with GitLab Orbit on GitLab Self-Managed, you might encounter the following issues.

## Queries return `502` and the GitLab logs show `Unimplemented`

The GitLab Orbit release is too old for your GitLab version. GitLab 19.3 and later call a gRPC service
that earlier releases do not have.

To resolve this issue, upgrade GitLab Orbit. For the minimum release, see
[Version compatibility](getting-started.md#version-compatibility).

## GitLab cannot connect, and the log shows an ALPN error

A load balancer or proxy between GitLab and the webserver does not keep HTTP/2. For example, an AWS Classic
Load Balancer, or a proxy that terminates TLS and forwards HTTP/1.1.

To resolve this issue, forward TCP to the webserver and terminate TLS on the webserver. For more
information, see [TLS and network requirements](orbit-setup.md#tls-and-network-requirements).

## Connections time out after the webserver pod moves

On AWS, cross-zone load balancing is off on the Network Load Balancer. The load balancer node in one zone
cannot reach a pod in a different zone.

To resolve this issue, turn on cross-zone load balancing. For more information, see
[TLS and network requirements](orbit-setup.md#tls-and-network-requirements).

## Queries fail after a GitLab reconfigure or upgrade

GitLab generated a new shared key, and the key in the `gkg-secrets` Secret no longer matches. This happens on
the Linux package when a tool replaces `/etc/gitlab/gitlab-secrets.json`.

To resolve this issue:

1. Set `gitlab_rails['orbit_secret']` in `/etc/gitlab/gitlab.rb` to one key, and reconfigure GitLab.
1. Put the same key in the `gitlab-jwt-verifying-key` and `gitlab-jwt-signing-key` entries of the Secret.
1. Restart the GitLab Orbit pods:

   ```shell
   kubectl -n gitlab-orbit rollout restart deployment
   ```

## Siphon pods fail with `no such file` under `/etc/tables`

The cluster dropped the image volume that holds the table definitions, because the `ImageVolume` feature
gate is off.

To resolve this issue, set `splitTables.volumeMode: initCopy` in `siphon-values.yaml`, or use a
Kubernetes version that supports image volumes. For more information, see
[Kubernetes](getting-started.md#kubernetes).

## Siphon fails with `Stream.Target ... required`

Siphon releases earlier than 0.0.130-beta cannot read some table definitions in the CNG
`gitlab-siphon-tables` image.

To resolve this issue, upgrade Siphon to 0.0.130-beta or later.

## The container registry stops after you change PostgreSQL

On the Linux package, `postgresql['listen_address']` also moves the registry database connection from the
local socket to TCP. The registry database user has no password, so the connection fails.

To resolve this issue, set `registry['database']['host'] = '/var/opt/gitlab/postgresql'` in
`/etc/gitlab/gitlab.rb`, and reconfigure GitLab. For more information, see
[Turn on logical replication in PostgreSQL](data-replication.md#turn-on-logical-replication-in-postgresql).

## The dispatcher stops with `ontology archive vNN is missing`

The graph database was created by a GitLab Orbit release earlier than 0.107.0. Later releases cannot
upgrade it in place.

To resolve this issue, create an empty graph database, point GitLab Orbit at it, and index again. For more
information, see [Upgrade GitLab Orbit](orbit-setup.md#upgrade-gitlab-orbit).

## The ClickHouse disk fills with system tables

The ClickHouse system log tables, such as `query_log` and `text_log`, have no time limit by default.

To resolve this issue, add a TTL to each log. For more information, see
[System log retention](getting-started.md#system-log-retention).

## A user gets no results for a project they are a member of

GitLab Orbit grants access through group membership only. A role that is assigned only on a project does
not give access to GitLab Orbit.

To resolve this issue, give the user the Reporter role or higher on the group. For more information, see
[Roles required to query GitLab Orbit](../remote/security.md#roles-required-to-query-gitlab-orbit).
