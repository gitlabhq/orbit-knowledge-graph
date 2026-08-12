---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Run GitLab Orbit against your own GitLab instance.
title: GitLab Orbit on GitLab Self-Managed
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) in GitLab 19.3.

{{< /history >}}

On GitLab.com, GitLab Orbit runs on GitLab infrastructure.
On GitLab Self-Managed, you run GitLab Orbit next to your instance.

A GitLab Orbit deployment has two parts:

- A data pipeline that copies the GitLab database into ClickHouse.
- GitLab Orbit, which turns that copy into a queryable graph.

The data pipeline does not depend on GitLab Orbit, so you can verify the pipeline before you install
GitLab Orbit.

The Linux package does not include GitLab Orbit. The only supported deployment is the GitLab Orbit Helm
chart on Kubernetes. Install the chart on the cluster that runs GitLab, or on a separate cluster next to
your instance.

GitLab Orbit on GitLab Self-Managed is in
[beta](https://docs.gitlab.com/policy/development_stages_support/#beta).
GitLab works with you to set up each deployment, so contact your account team before you plan one.

## Architecture

```mermaid
flowchart LR
    accTitle: GitLab Orbit on GitLab Self-Managed
    accDescr: GitLab writes to PostgreSQL, Siphon reads the PostgreSQL write-ahead log and replicates rows through NATS JetStream into the ClickHouse data lake, the GitLab Orbit dispatcher watches the same NATS stream and creates the graph schema in ClickHouse, the GitLab Orbit indexer takes indexing tasks from NATS and builds the property graph from the data lake and from source code fetched over the GitLab internal API, and GitLab queries the GitLab Orbit webserver over gRPC.

    subgraph GitLab["GitLab instance"]
        PG[(PostgreSQL)]
        Rails[GitLab Rails]
    end

    subgraph K8s["Kubernetes cluster"]
        Siphon[Siphon]
        NATS[NATS JetStream]
        Dispatch[GitLab Orbit dispatcher]
        Indexer[GitLab Orbit indexer]
        Web[GitLab Orbit webserver]
    end

    CH[(ClickHouse)]

    PG -- logical replication --> Siphon
    Siphon <--> NATS
    Siphon -- writes data lake --> CH
    NATS -- change events --> Dispatch
    Dispatch -- indexing tasks --> NATS
    Dispatch -- creates schema --> CH
    NATS -- indexing tasks --> Indexer
    CH -- reads data lake --> Indexer
    Indexer -- internal API --> Rails
    Indexer -- writes graph --> CH
    CH -- reads graph --> Web
    Web -- internal API --> Rails
    Rails -- gRPC --> Web
```

| Component | Function |
|-----------|----------|
| Siphon | Copies rows from PostgreSQL into the ClickHouse data lake, through NATS. |
| Dispatcher | Watches the same NATS stream, owns the graph schema, and publishes indexing tasks back to NATS. |
| Indexer | Takes indexing tasks from NATS, reads the data lake, fetches source code over the GitLab internal API, and writes the property graph. |
| Webserver | Answers queries from the graph, and fetches file content over the internal API when a query needs it. |

GitLab reaches the webserver over gRPC.

## In this section

| Page | Description |
|------|-------------|
| [Get started](getting-started.md) | Prerequisites, installation order, and shared configuration values |
| [Set up data replication](data-replication.md) | PostgreSQL logical replication and Siphon |
| [Set up GitLab Orbit](orbit-setup.md) | ClickHouse identities, the GitLab connection, the GitLab Orbit chart, and group indexing |

## Support and coverage

GitLab Orbit is not supported on a GitLab Geo secondary site, and no FIPS builds are available.

Redundancy and recovery for GitLab Orbit are not documented. GitLab Orbit holds no data of its own, so if
you lose the graph database, you can rebuild it by indexing again.
