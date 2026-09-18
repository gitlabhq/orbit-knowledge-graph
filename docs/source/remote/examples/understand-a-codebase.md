---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Get oriented in a project you have never worked in by finding its contributors, core classes, and entry points.
title: Understand an unfamiliar codebase
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

Follow these steps when you join a project and need to know where to start reading.

- Time estimate: 15-30 minutes
- Level: Beginner

## The challenge

To get oriented in an unfamiliar project, you usually read files at random until the structure
becomes clear.
The graph already knows who works on the project, which definitions exist, and how they connect.

## The approach

Survey the contributors, map the structure, then build a reading list.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.
Replace `<my-org/my-project>` with the full path of the project you want to learn.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Survey

Use your agent to find the people behind the code:

```plaintext
I'm new to the <my-org/my-project> project. Using GitLab Orbit, show me the most
active contributors over the last few months, and what each of them works on.
```

Expected outcome: Ranked list of contributors, with the areas each one touches.

Find the most active contributors to a project:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    },
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["u"],
  "aggregations": [
    { "count": "mr", "as": "merged_mrs" }
  ],
  "aggregation_sort": "-merged_mrs",
  "limit": 10
}
```

### Step 2: Map

Use your agent to lay out the structure:

```plaintext
Now map the structure of this project: the core classes and modules, how they
relate to each other, and the main entry points.
```

Expected outcome: A description of the core classes and modules, and how they depend on each
other.

### Step 3: Read

Use your agent to turn the map into a reading list:

```plaintext
Summarize how this codebase is structured, and suggest the three files to read
first to understand it. Explain why each one is worth reading.
```

Expected outcome: A short reading list, with a reason for each file.

## Tips

- Follow up in the same conversation so the agent keeps the context from earlier steps.
- Ask the agent to open one of the recommended files or to explain how two classes relate.
- Expect only default-branch results. GitLab Orbit indexes code from the default branch.
- Ask for contributors over a specific window when a project has changed hands recently.

## Verify

Ensure that:

- The tour names real files and definitions that exist in the project.
- Each recommended file comes with a reason to read it.
