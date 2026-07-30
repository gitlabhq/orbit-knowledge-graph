---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: How GitLab Orbit Remote indexes GitLab data and source code, builds a graph in ClickHouse, and exposes it as a queryable API.
title: How GitLab Orbit Remote works
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

## Indexing pipeline

GitLab Orbit indexes data from two sources and combines them into a single graph.

### SDLC data

GitLab streams change events through a change data capture (CDC) pipeline to the
[GitLab Data Insights Platform](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/data_insights_platform/).
The platform writes records to ClickHouse tables
that GitLab Orbit reads and writes its graph on top of.

This happens continuously. When a user opens a merge request, creates a work item,
or kicks off a pipeline, the change propagates to the GitLab Orbit graph within minutes.

### Source code

GitLab Orbit calls the GitLab Rails internal API to fetch source files from your repositories.
It parses each file with a language-specific parser, extracts definitions (functions,
classes, modules) and import references, and writes them as nodes and edges to the graph.

Code is indexed from the default branch only. A re-index runs automatically when
the default branch changes.

### Graph construction

After reading SDLC data and code, GitLab Orbit writes a unified graph to ClickHouse.
Each entity (a project, a user, a function definition) becomes a node.
Each relationship (a user authored a merge request, a file imports a module)
becomes a directed edge.

When you send a query, GitLab Orbit compiles the JSON query DSL to ClickHouse SQL,
executes it, and returns typed results.

## The graph model

The graph has two layers:

- SDLC layer: GitLab objects and their relationships. Projects belong to groups.
Users author merge requests. Pipelines run on projects. Work items are assigned to users.
- Code layer: Source code structure and cross-file references. Functions are defined in files.
Files import symbols from other files. Definitions exist within projects and branches.

The two layers are connected. A merge request (SDLC layer) touches files (code layer).
A user (SDLC layer) owns a definition (code layer) if they last modified the containing file.

## Performance

GitLab Orbit runs in a separate Kubernetes cluster. It does not share compute or memory
with your GitLab instance.

Initial indexing of a large group (thousands of projects, millions of lines of code)
completes in minutes. Incremental re-indexing after a change completes in seconds to minutes
depending on the size of the change.

## Query execution

All queries go through the same path:

1. GitLab Orbit receives a JSON query payload (via REST, MCP, or GitLab Duo Agent Platform).
1. The query engine validates the query against the current schema.
1. GitLab Orbit compiles the JSON DSL to ClickHouse SQL.
1. ClickHouse executes the query against the graph tables.
1. GitLab Orbit applies authorization filtering: results are scoped to entities the
   requesting user has access to in GitLab. For more information, see [Security](security.md).
1. GitLab Orbit returns typed JSON results.

You can request the compiled SQL in query responses by setting `options.include_debug_sql: true`.
This field is only populated for instance administrators and direct GitLab organization members
with Reporter or higher access.

## Data retention and deletion

When you disable GitLab Orbit on a group, your indexed data is not deleted immediately. GitLab Orbit keeps it for 30 days so you can re-enable without losing your graph history. After the grace period, all graph data for that group, including all nodes, edges, and indexing checkpoints, is permanently deleted.

If you re-enable GitLab Orbit before the 30 days are up, deletion is canceled and indexing resumes from where it left off.
