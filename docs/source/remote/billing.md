---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn which GitLab Orbit actions consume GitLab Credits and which actions are free.
title: Billing and credits
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

GitLab Orbit Remote runs queries on GitLab-hosted infrastructure and meters them with GitLab
Credits.
For credit rates, and for how you purchase and consume credits across GitLab features, see
[GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

GitLab Orbit Local indexes and queries repositories on your own machine and consumes no credits.

## Credit consumption during beta

During beta, GitLab Orbit queries do not consume GitLab Credits.

When GitLab Orbit is generally available, queries consume GitLab Credits on every access path,
including GitLab Duo Agent Platform, the GitLab Orbit Remote MCP server, the GitLab CLI, and the
REST API.
GitLab publishes credit rates before charging begins.

## Metered and free actions

GitLab Orbit meters queries only.
Schema, status, and discovery calls are free on every access path.

After GitLab Orbit becomes generally available, credits apply to these actions:

| Action | Access path | Consumes credits |
|--------|-------------|------------------|
| Run a query | GitLab Duo Agent Platform, MCP `query_graph`, `glab orbit query`, `POST /api/v4/orbit/query` | {{< yes >}} |
| Fetch the graph schema | MCP `get_graph_schema`, `glab orbit ontology`, `GET /api/v4/orbit/schema` | {{< no >}} |
| Check indexing status | `glab orbit status`, `glab orbit graph-status`, `GET /api/v4/orbit/status` | {{< no >}} |
| List available tools and commands | MCP `list_commands`, `glab orbit tools`, `GET /api/v4/orbit/tools` | {{< no >}} |
| Fetch the query language or response format reference | MCP `get_query_dsl`, MCP `get_response_format` | {{< no >}} |
| Index or query a local repository | The `orbit` CLI, `glab orbit` local commands, GitLab Orbit Local MCP | {{< no >}} |

## Cost per query

A successful query counts as one metered request, regardless of the number of rows it returns.
The size of the result set, the number of nodes the query traverses, and the volume of data the
query scans do not change the cost.

A query that fails costs nothing.

Queries that GitLab provides for common questions cost the same as the queries you write yourself.

## Troubleshooting

### Error: `GitLab credits exhausted`

When GitLab Orbit is generally available and your namespace has no credits left, queries are
rejected with the error `GitLab credits exhausted`.
Schema, status, and discovery calls continue to work.

To resolve, add credits to your namespace.
