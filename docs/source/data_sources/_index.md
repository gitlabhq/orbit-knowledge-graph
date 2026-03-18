---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Understand what data Orbit indexes and how it flows through the system.
availability_details: no
title: Data sources
---

Orbit indexes only the top-level groups where it is enabled.
Subgroups and projects inherit indexing from the top-level group.

Orbit indexes two categories of data:

1. GitLab data includes the software development lifecycle objects that make up your instance:

   - Groups and projects
   - Users
   - Work items
   - Merge requests
   - Pipelines
   - Vulnerabilities and security findings

1. Code includes the content of your repositories:

   - Source files and directories
   - Function, class, and module definitions
   - Imports and cross-file references

   Orbit indexes code from only the default branch.

```mermaid
%%{init: { "fontFamily": "GitLab Sans" }}%%
flowchart LR
  accTitle: Orbit architecture
  accDescr: Data flows from PostgreSQL through the Data Insights Platform into ClickHouse. Orbit reads data from ClickHouse and code from internal archives to serve AI agents and services. 

  subgraph GitLabCore[GitLab Core]
    PG[(PostgreSQL)]
    Repo[Repository archives]
  end

  subgraph DataPipeline[Data Insights Platform]
    Siphon[Siphon]
    NATS[NATS JetStream]
  end

  PG -- "CDC events" --> Siphon
  Siphon --> NATS
  NATS --> CH[ClickHouse]
  Repo --> Rails[Rails internal API]
  Rails --> Orbit
  CH <--> Orbit[Orbit service]
  Clients[AI agents and services] --> Orbit
```

PostgreSQL emits change data capture (CDC) events to Siphon, which forwards them through NATS JetStream into ClickHouse.
In parallel, Orbit downloads code from repository archives through the Rails internal API. Orbit combines GitLab data and code,
then writes the unified property graph to ClickHouse. Users and AI agents can query the graph through the unified context API.

## Supported languages

Orbit supports code indexing for the following languages:

| Language   | Definitions & imports | References within files | References across files |
|------------|-----------------------|-------------------------|-------------------------|
| Ruby       | {{< yes >}}           | {{< yes >}}             | {{< yes >}}             |
| Java       | {{< yes >}}           | {{< yes >}}             | {{< yes >}}             |
| Kotlin     | {{< yes >}}           | {{< yes >}}             | {{< yes >}}             |
| Python     | {{< yes >}}           | {{< yes >}}             | {{< no >}}              |
| TypeScript | {{< yes >}}           | {{< yes >}}             | {{< no >}}              |
| JavaScript | {{< yes >}}           | {{< yes >}}             | {{< no >}}              |
