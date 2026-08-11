---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: What to prepare before you install GitLab Orbit on GitLab Self-Managed.
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

GitLab Orbit depends on three systems that it does not install for you: ClickHouse, Kubernetes, and
NATS. Prepare all three before you start. The other pages in this section assume they exist and are
reachable.

## Prerequisites

### GitLab

GitLab 19.3 or later, with a Premium or Ultimate license.

You also need administrator access to the instance and to its PostgreSQL server, including the ability
to restart PostgreSQL once.

### ClickHouse

ClickHouse 26.2 or later, set up for GitLab as described in
[ClickHouse](https://docs.gitlab.com/integration/clickhouse/).

> [!note]
> The ClickHouse page allows any 25.x or 26.x release. GitLab Orbit is stricter. It builds its graph
> with full-text indexes and materialized common table expressions that arrived in 26.2, so a 25.x
> release serves the rest of GitLab but not GitLab Orbit. Everything else on that page applies
> unchanged.

Follow that page all the way through, including
[running the ClickHouse migrations](https://docs.gitlab.com/integration/clickhouse/#run-clickhouse-migrations)
and [enabling ClickHouse for analytics](https://docs.gitlab.com/integration/clickhouse/#enable-clickhouse-for-analytics).
Those migrations create the data lake tables that replication writes into, so replication has nowhere
to land without them.

GitLab Orbit uses two databases:

| Database | Written by | Read by |
|---|---|---|
| `gitlab_clickhouse_main_production` | GitLab, Siphon | GitLab, the GitLab Orbit indexer and dispatcher |
| `gkg` | The GitLab Orbit dispatcher (schema) and indexer (data) | All three GitLab Orbit components |

They do not have to be on the same ClickHouse instance. Split them if you prefer, and give GitLab,
Siphon, and GitLab Orbit credentials for whichever instance holds the database each one needs.

The first database already exists after you finish the ClickHouse page.
[Set up GitLab Orbit](orbit-setup.md) creates the second one.

### Kubernetes

Kubernetes 1.33 or later, with the `ImageVolume` feature gate turned on.

The cluster does not have to be the one that runs GitLab. A separate cluster next to your instance
works, as long as it can reach GitLab, PostgreSQL, ClickHouse, and NATS, and GitLab can reach it back.

### NATS

NATS with JetStream turned on, backed by a persistent volume. Siphon publishes every changed row to a
JetStream stream, and both Siphon and GitLab Orbit read from it.

Set the NATS server `max_payload` to 64 MB. The default of 1 MB is smaller than some GitLab rows, and
Siphon uses the server value to decide when a row is too large to send.

### Object storage

Optional, and not a fourth system to stand up. Siphon parks snapshot events, and rows still too large
after you raise `max_payload`, in an object store. Configure nothing and it uses a NATS JetStream
object store bucket, so a working install needs no extra service. Point it at an S3-compatible or
Google Cloud Storage bucket instead if you would rather keep that traffic off the JetStream volume, or
put it on a separate retention policy.

GitLab Orbit itself uses no object storage. Its graph lives in ClickHouse and can be rebuilt by indexing
again. The indexer needs node-local disk for repository checkouts, which the chart sizes with ephemeral
storage requests.

## Install order

Each step depends on the one before it.

1. **ClickHouse.** Set it up for GitLab and run the GitLab ClickHouse migrations. Covered by the
   [ClickHouse](https://docs.gitlab.com/integration/clickhouse/) page.
1. **NATS.** Install it on the cluster with JetStream turned on.
1. **[Data replication](data-replication.md).** Prepare PostgreSQL, then install Siphon. At the end of
   this step, GitLab rows land in the ClickHouse data lake.
1. **[GitLab Orbit](orbit-setup.md).** Create the ClickHouse identities, install the chart, point
   GitLab at it, and turn it on for a top-level group.

Steps 1 to 3 do not mention GitLab Orbit and you can verify them on their own.

## Values to decide up front

Choose these once and use the same values everywhere. Nothing checks that they agree, and a mismatch
leaves a component running quietly with nothing to do.

| Value | Used by | Example |
|---|---|---|
| NATS stream name | Siphon and GitLab Orbit | `siphon_stream_main_db` |
| Data lake database | GitLab, Siphon, and GitLab Orbit | `gitlab_clickhouse_main_production` |
| Graph database | GitLab Orbit | `gkg` |
| PostgreSQL host and port | Siphon | `postgres.example.com:5432` |
| GitLab URL reachable from the cluster | GitLab Orbit | `https://gitlab.example.com` |
| GitLab Orbit gRPC endpoint reachable from GitLab | GitLab | `tls://orbit.example.com:50054` |

`siphon_stream_main_db` is the name GitLab Orbit expects by default. If you pick another one, set
`stream_name` in `siphon-values.yaml` and `schedule.tasks.siphon.events_stream_name` in
`orbit-values.yaml`.

## Next step

[Set up data replication](data-replication.md).
