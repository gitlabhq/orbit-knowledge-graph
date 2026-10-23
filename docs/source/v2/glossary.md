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

Code graph
: The part of the graph that holds source code structure: files, directories, definitions, imported symbols, and the calls, imports, and inheritance between them.

Definition
: A named code element, such as a class, function, method, or module. Its fully qualified name (`fqn`) includes the enclosing names.

GitLab Orbit CLI
: The `orbit` binary. `glab orbit` installs it and forwards every command to it.

GitLab Orbit skill
: The instructions and query recipes that your agent loads to use GitLab Orbit. `glab orbit setup` installs it.

GQL
: The GitLab Orbit query language, a read-only, OpenCypher-like language. Agents write GQL for you.

Indexing
: The process that reads GitLab data and code and writes the graph. GitLab indexes a group when you turn on GitLab Orbit for it, and keeps the graph current.

Local graph
: The graph that the GitLab Orbit CLI builds from a repository on your machine. It holds code only, works offline, and has no authorization layer.

Model Context Protocol (MCP) endpoint
: The GitLab server for the Model Context Protocol at `/api/v4/orbit/mcp`, for agents that cannot run shell commands.

Named query
: A query that GitLab provides on the server. You run it by name with parameters.

Node
: One entity in the graph, such as a project, a user, a merge request, a file, or a code definition. Each node type has a fixed set of properties.

Property
: A named value on a node, such as `full_path` on a project or `severity` on a vulnerability.

Property graph
: The data model of GitLab Orbit. Typed nodes carry properties, and typed, directed relationships connect them.

Relationship
: A directed connection between two nodes, such as `AUTHORED` from a user to a merge request, or `CALLS` from one definition to another.

Remote graph
: The graph that GitLab builds for each top-level group where GitLab Orbit is turned on. Queries run on GitLab and return only the data that your role lets you see.

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
