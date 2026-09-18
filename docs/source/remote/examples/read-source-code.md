---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Pull the source of a file or a single function into your agent conversation.
title: Read source code from your agent
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

Follow these steps to review real code in the conversation instead of switching to the
repository.

- Time estimate: 5-15 minutes
- Level: Beginner

## The challenge

To review code with an agent, you usually paste the code in by hand, which breaks the
conversation and risks pasting the wrong version.

## The approach

Fetch a file, fetch a definition, then review the code.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.
Replace the file path and the fully qualified name with your own.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Fetch a file

Use your agent to pull the file into the conversation:

```plaintext
Using GitLab Orbit, show me the source of <app/models/project.rb>.
```

Expected outcome: The file contents, in the conversation.

Fetch the source text of a file.
Use `limit: 1` to avoid large responses:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "f",
    "entity": "File",
    "columns": ["path", "language", "content"],
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    }
  }],
  "limit": 1
}
```

### Step 2: Fetch a definition

Use your agent to narrow the result to one function or class:

```plaintext
Now show me just the definition of <MyModule::my_function>, with its start and
end lines.
```

Expected outcome: The body of that definition, with the lines it occupies.

Fetch the source text of a specific function or class definition.
The `content` field returns the raw source text of just that definition, not the full file:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"],
    "filters": {
      "fqn": {"eq": "Gitlab::Auth::authenticate"}
    }
  }],
  "limit": 5
}
```

### Step 3: Review

Use your agent to work with the code you fetched:

```plaintext
Review this definition for correctness and explain how it fits into the rest of
the file.
```

Expected outcome: A review of the definition, grounded in the source text GitLab Orbit returned.

## Tips

- Expect slower responses for `File.content`, `Definition.content`, `MergeRequest.diff`, and `MergeRequestDiffFile.diff`, which GitLab Orbit fetches from Gitaly after the graph query.
- Filter on the properties that identify the code you want, such as `path` or `fqn`.
- Do not filter on a virtual column unless the node also carries explicit `node_ids`, and never in an aggregation query.
- Ask for the merge request diff to review a change rather than the current state. Content comes from the default branch.

## Verify

Ensure that:

- The file returned is the one you asked for, not a file with a similar path.
- The definition comes with its start and end lines so you can find it in the repository.
