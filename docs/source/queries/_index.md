---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the Orbit knowledge graph to explore your GitLab instance.
title: Queries
---

{{< details >}}

- Tier: Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Queries are the main way to work with the knowledge graph. A query is
a JSON object that defines what data to retrieve and how to structure
the results.

Queries respect role-based access control. When you query the
knowledge graph, you see only data you have permission to see in
GitLab.

You can query the graph directly by using the Orbit query language, or
have an AI agent like GitLab Duo write and run queries for you.

## Prerequisites

- Orbit must be turned on for a group or project.
- You must have the Reporter, Developer, Maintainer, or Owner role for the group or project.

## Query language

Use the Orbit query language to get data from the knowledge graph manually.

You can use the query language:

- In an AI prompt.
- In the UI, with the query editor.
- With the [Orbit API](https://docs.gitlab.com/api/orbit/).

Orbit queries are JSON objects. Each query must include:

- `query_type`: The type of query to run.
- Either:
  - `node`: The graph object to return.
  - `nodes`: An array of graph objects to return.

For example:

```json
{
  "query_type": "traversal",
  "node": {
    "id": "u",
    "entity": "User",
    "filters": { "username": "sidneyjones" }
  }
}
```

For a list of available fields, see [query language fields](query_language.md).

## Run a query in the UI

Use the query editor to write and run queries in the UI.

To run a query:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Data explorer**.
1. In the query editor, enter a query.
1. Select **Execute query**.

Orbit displays the results of the query in the **Node explorer** and **Table** views.

## GitLab Duo Agentic Chat

When Orbit is turned on, Agentic Chat automatically uses the knowledge
graph as a data source to respond to prompts.

See [use GitLab Duo Chat](https://docs.gitlab.com/user/gitlab_duo_chat/agentic_chat/#use-gitlab-duo-chat).

## Connect to the Orbit MCP server

Use the Model Context Protocol (MCP) server to integrate external AI
tools like Claude Code with Orbit.

GitLab uses `mcp-remote` to establish secure connections between Orbit
and AI tools running on your local computer. A known issue can cause
the connection to fail with a `403 incorrect_scope` error. To resolve
this issue, you must manually register the client before establishing
the connection.

### Step 1: Manually register the client

To register the client:

1. From the command line, run:

   ```shell
   npx mcp-remote "https://gitlab.com/api/v4/orbit/mcp"
   ```

1. In your browser, review and approve the authorization request.
   The `mcp-remote` command fails to establish a connection and displays a `403 incorrect_scope` error. It creates a cache directory at `~/.mcp-auth/mcp-remote-<version>/` with two files:
   - `<hash>-_client_info.json`
   - `<hash>_tokens.json`

   Save the file names for the next steps.

1. Register a client:

   ```shell
   curl --request POST \
     --header "Content-Type: application/json" \
     --data '{"redirect_uris": ["http://localhost:42826/oauth/callback"], "client_name": "MCP CLI Proxy", "resource": "https://gitlab.com/api/v4/orbit/mcp"}' \
     --url "https://gitlab.com/oauth/register"
   ```

1. In the response, verify that `scope` is set to `mcp_orbit` and save the values of:
   - `client_id`
   - `client_id_issued_at`

1. Replace the content of `<hash>_client_info.json` with:

   ```json
   {
     "redirect_uris": ["http://localhost:42826/oauth/callback"],
     "token_endpoint_auth_method": "none",
     "grant_types": ["authorization_code"],
     "client_name": "[Unverified Dynamic Application] MCP CLI Proxy",
     "scope": "mcp_orbit",
     "client_id": "<client_id_from_response>",
     "client_id_issued_at": <client_id_issued_at_from_response>
   }
   ```

1. Replace the content of `<hash>_tokens.json` with:

   ```json
   {}
   ```

1. From the command line, run `mcp-remote` again:

   ```shell
   npx mcp-remote "https://gitlab.com/api/v4/orbit/mcp"
   ```

1. In your browser, review and approve the authorization request.
   The connection should now succeed.

### Step 2: Configure MCP

To connect to the Orbit MCP server:

- Follow the instructions to [connect a client to the GitLab MCP server](https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/#connect-a-client-to-the-gitlab-mcp-server).
  Use the URL `https://gitlab.com/api/v4/orbit/mcp`.

You can now start a chat with your AI agent.

For a list of available MCP tools, see [Orbit MCP tools](mcp_tools.md).

## Example prompts

Use these example prompts with Agentic Chat or another AI agent:

- "List merged merge requests in the last 30 days for `my-project`, grouped by author."
- "Show all open issues that are blocked by merge requests with failing pipelines in `my-project`."
- "List services that directly depend on `payments-api` and show their last five deployments."
- "Find all vulnerabilities that are linked to merge requests merged in the last seven days in `my-group`, grouped by severity."
- "Show all projects where `@alice` has authored merge requests, with a count of merged vs open merge requests per project."
- "List the top 10 files in `my-group/my-project` that changed in the most failed pipelines over the past month."
