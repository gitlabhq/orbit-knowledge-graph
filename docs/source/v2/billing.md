---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn which GitLab Orbit actions consume GitLab Credits and which actions are free.
title: Billing and credit usage
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment). Queries did not consume GitLab Credits.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Queries on GitLab.com consume GitLab Credits.

{{< /history >}}

<!-- TODO(GA): confirm with Pricing what the add-on includes, and how GitLab Self-Managed and GitLab Dedicated are billed. -->

On GitLab.com, GitLab Orbit runs queries on GitLab-hosted infrastructure and meters them with GitLab Credits.
Queries consume GitLab Credits on every access method, including GitLab Duo Agent Platform, the MCP endpoint,
the GitLab Orbit CLI, and the REST API.
For credit rates, and for how you purchase and consume credits across GitLab features, see
[GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

A [free trial](trial.md) includes a fixed amount of GitLab Credits for a limited number of days.

For billing on GitLab Self-Managed and GitLab Dedicated, contact your GitLab account team.

The local graph on your machine never consumes GitLab Credits.

## Metered and free actions

GitLab Orbit meters queries only. Schema, status, and discovery calls are free on every access method.

| Action | Access method | Consumes GitLab Credits |
|--------|---------------|-------------------------|
| Run a query | GitLab Duo Agent Platform, MCP `query_graph`, `glab orbit query`, `POST /api/v4/orbit/query`, `POST /api/v4/orbit/query/:name`, `POST /api/v4/orbit/agent/commands/query_graph` | {{< yes >}} |
| Fetch the graph schema | MCP `get_graph_schema`, `glab orbit ontology`, `GET /api/v4/orbit/schema`, `CALL db.schema()` | {{< no >}} |
| Check status and indexing progress | `glab orbit status`, `glab orbit graph-status`, `GET /api/v4/orbit/status`, `GET /api/v4/orbit/graph_status` | {{< no >}} |
| List tools and commands | MCP `list_commands`, `glab orbit tools`, `GET /api/v4/orbit/tools`, `GET /api/v4/orbit/agent/commands` | {{< no >}} |
| Fetch the response format reference | MCP `get_response_format`, `GET /api/v4/orbit/schema/format` | {{< no >}} |
| Index or query the local graph | `glab orbit index`, `grep`, `context`, `repo-map`, `sql`, the local MCP server | {{< no >}} |

## Cost for each query

A query that succeeds counts as one metered request, even when it returns no rows.
The size of the result, the number of nodes the query traverses, and the volume of data the query scans do not
change the cost. A query that fails costs nothing.
Named queries that GitLab provides cost the same as the queries you write yourself.

GitLab Orbit bills each query to one top-level group.
When the group has no GitLab Credits left, or its [free trial](trial.md) ends, queries fail with
`GitLab credits exhausted`. For more information, see [troubleshooting](troubleshooting.md#error-gitlab-credits-exhausted).

## Related topics

- [Start a GitLab Orbit trial](trial.md)
- [GitLab Orbit architecture](how-it-works.md)
- [Troubleshooting](troubleshooting.md)
