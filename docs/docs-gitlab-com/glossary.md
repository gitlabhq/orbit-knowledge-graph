---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Definitions of the terms used in the GitLab Orbit documentation.
title: GitLab Orbit glossary
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

This glossary provides definitions for terms related to GitLab Orbit.

Code layer
: The layer of the graph that holds source code structure: files, directories, definitions, imports, and the calls, uses, imports, and inheritance between them.

Definition
: A named code element, such as a class, function, method, or module. Its fully qualified name (`fqn`) includes the enclosing names.

GitLab Orbit
: The product that builds a graph of GitLab data and code for each top-level group that has the feature turned on, and answers queries on the graph. The graph holds the code of your repositories and software development lifecycle (SDLC) data, such as merge requests, pipelines, work items, and security findings. Queries run on GitLab and return only the data your role lets you see.

GitLab Orbit API
: The REST, GraphQL, and MCP endpoints of GitLab Orbit on your GitLab instance. The GitLab Orbit CLI and agents call these endpoints.

GitLab Orbit CLI
: The `orbit` binary. `glab orbit` installs it and forwards every command to it. The CLI queries the GitLab Orbit API, and also builds a local graph of the code on your machine. Some commands combine results from both.

GitLab Orbit MCP endpoint
: The Model Context Protocol (MCP) server of GitLab Orbit, at `/api/v4/orbit/mcp`, for agents that cannot run shell commands.

GitLab Orbit skill
: The instructions and query recipes your agent loads to use GitLab Orbit. `glab orbit setup` installs it.

GQL
: The GitLab Orbit query language, a read-only, openCypher-like language. Agents write GQL for you.

Indexing
: The process that reads GitLab data and code and writes the graph. GitLab indexes a group when you turn on GitLab Orbit for it, and keeps the graph current.

Local graph
: The graph the GitLab Orbit CLI builds from a repository on your machine. It holds code only and works offline.

Node
: One entity in the graph, such as a project, a user, a merge request, a file, or a code definition. Each node type has a fixed set of properties.

Property
: A named value on a node, such as `full_path` on a project or `severity` on a vulnerability.

Property graph
: The data model of GitLab Orbit. Typed nodes carry properties, and typed, directed relationships connect them.

Relationship
: A directed connection between two nodes, such as `AUTHORED` from a user to a merge request, or `CALLS` from one definition to another.

Software development lifecycle (SDLC) data
: The part of the graph that holds GitLab objects: groups, projects, users, merge requests, pipelines, work items, packages, and security findings.

Top-level group
: The group at the root of a group hierarchy. GitLab Orbit is turned on and scoped for each top-level group.

Work item
: The single node type for issues, epics, tasks, incidents, and other trackable units of work. The `work_item_type` property tells them apart.

## Related topics

- [GitLab Orbit architecture](how-it-works.md)
- [Indexed data](schema.md)
- [Query language](queries/query-language.md)
- [Local graph](cli.md#local-graph)
