---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Prerequisites, installation order, and shared configuration values for GitLab Orbit on GitLab Self-Managed.
title: Get started with GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.3.

{{< /history >}}

GitLab Orbit depends on three systems that it does not install: ClickHouse, Kubernetes, and NATS.
The remaining setup steps require all three systems to exist and be reachable.

## Prerequisites

- GitLab 19.3 or later.
- Administrator access to the GitLab instance and to its PostgreSQL server.
- A maintenance window for one PostgreSQL restart.
- ClickHouse 26.2 or later, set up for GitLab.
- Kubernetes 1.33 or later.
- NATS with JetStream turned on.

## Installation order

Each step depends on the step before it. Install the components in this order:

1. Set up ClickHouse for GitLab and run the GitLab ClickHouse migrations.
1. Install NATS on the cluster with JetStream turned on.
1. [Set up data replication](data-replication.md): prepare PostgreSQL, then install Siphon. After this
   step, GitLab rows arrive in the ClickHouse data lake.
1. [Set up GitLab Orbit](orbit-setup.md): create the ClickHouse identities, install the chart, point
   GitLab at GitLab Orbit, and turn on indexing for a top-level group.

Steps 1 to 3 do not depend on GitLab Orbit. You can verify each one on its own.

## ClickHouse

GitLab Orbit requires ClickHouse 26.2 or later, because the graph uses full-text indexes and materialized
common table expressions introduced in that release. GitLab supports any 25.x or 26.x release, so a 25.x
release serves the rest of GitLab but not GitLab Orbit. All other GitLab requirements for ClickHouse are
unchanged.

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
| `gkg` | The GitLab Orbit dispatcher (schema) and indexer (data) | All three GitLab Orbit components |

The two databases can be on separate ClickHouse instances. If they are, give GitLab, Siphon, and
GitLab Orbit credentials for the instance that holds the database each one uses.

The `gitlab_clickhouse_main_production` database exists after ClickHouse setup is complete. The `gkg`
database is created when you set up GitLab Orbit.

## Kubernetes

GitLab Orbit requires Kubernetes 1.33 or later, with the `ImageVolume` feature gate turned on.

GitLab Orbit can run on a separate cluster from GitLab. That cluster must reach GitLab, PostgreSQL,
ClickHouse, and NATS, and GitLab must reach that cluster.

## NATS

Siphon publishes every changed row to a NATS JetStream stream, and both Siphon and GitLab Orbit read from
that stream. NATS requires JetStream turned on and a persistent volume.

Set the NATS server `max_payload` to 64 MB. The default of 1 MB is smaller than some GitLab rows. Siphon
reads the server value to decide when a row is too large to send.

## Object storage (optional)

Siphon stores snapshot events in an object store, along with any row that is still too large after you
raise `max_payload`. By default, Siphon uses a NATS JetStream object store bucket, so no additional service
is required. To keep that traffic off the JetStream volume, or to apply a separate retention policy,
configure an S3-compatible or Google Cloud Storage bucket instead.

GitLab Orbit uses no object storage. The graph is stored in ClickHouse and can be rebuilt by indexing
again. The indexer requires node-local disk for repository checkouts. The chart sizes this disk with
ephemeral storage requests.

## Shared configuration values

Choose each of these values once, and use the same value in every component that reads it. No component
validates these values against the others. If they do not match, the affected component starts but
processes no data.

| Value | Used by | Example |
|-------|---------|---------|
| NATS stream name | Siphon and GitLab Orbit | `siphon_stream_main_db` |
| Data lake database | GitLab, Siphon, and GitLab Orbit | `gitlab_clickhouse_main_production` |
| Graph database | GitLab Orbit | `gkg` |
| PostgreSQL host and port | Siphon | `postgres.example.com:5432` |
| GitLab URL reachable from the cluster | GitLab Orbit | `https://gitlab.example.com` |
| GitLab Orbit gRPC endpoint reachable from GitLab | GitLab | `tls://orbit.example.com:50054` |

GitLab Orbit expects the stream name `siphon_stream_main_db` by default. To use a different name, set
`stream_name` in `siphon-values.yaml` and `schedule.tasks.siphon.events_stream_name` in
`orbit-values.yaml`.

## Next step

- [Set up data replication](data-replication.md)
