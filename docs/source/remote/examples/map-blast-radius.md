---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find every project, file, and definition that depends on a shared library before you change it.
title: Map the blast radius of a change
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

Follow these steps before you change a shared library or a public interface.

- Time estimate: 15-30 minutes
- Level: Intermediate

## The challenge

To answer "what breaks if I change this?", you must search every project that might import the
code.
Cross-project import references are already in the graph.

## The approach

Find the importers, map the projects, then rank the risk.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.
Replace `<shared-auth-lib>` with the library, module, or package you want to trace.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Find the importers

Use your agent to find the code that imports the library:

```plaintext
Using GitLab Orbit, find every file that imports <shared-auth-lib>, and show me
the symbols each file imports from it.
```

Expected outcome: List of importing files, with the imported symbol names.

Find all files that import a specific module:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"contains": "payments-service"}
    }
  }],
  "limit": 100
}
```

### Step 2: Map the projects

Use your agent to widen the result to whole projects:

```plaintext
Which projects depend on <shared-auth-lib>? Group the importing files by project
so I can see how far the dependency spreads.
```

Expected outcome: The projects that carry the dependency, each with its importing files.

Find projects that depend on a shared library:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"contains": "shared-auth-lib"}}
    },
    {"id": "b", "entity": "Branch", "columns": ["name", "is_default"]},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "ON_BRANCH", "from": "f", "to": "b"},
    {"type": "CONTAINS", "from": "p", "to": "b"}
  ],
  "limit": 100
}
```

### Step 3: Rank the risk

Use your agent to find the interfaces that carry the most weight:

```plaintext
Rank the definitions in <shared-auth-lib> by how many places depend on them, then
tell me what would break if I changed the public interface, and which change
carries the most risk.
```

Expected outcome: Ranked definitions by dependent count, and an assessment of the riskiest
change.

Rank the definitions that the most code imports:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"contains": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "group_by": ["def"],
  "aggregations": [
    { "count": "sym", "as": "import_count" }
  ],
  "aggregation_sort": "-import_count",
  "limit": 20
}
```

## Tips

- Use a `contains` pattern of at least three characters. Shorter patterns are rejected.
- Ask the agent to try more than one spelling of the library name when the first result is empty.
- Expect only default-branch results so an import added on a feature branch does not appear.

## Verify

Ensure that:

- The dependent projects are listed by full path, not only by name.
- Each ranked definition has a dependent count you can trace back to a query.
- The riskiest change is named with a reason.
