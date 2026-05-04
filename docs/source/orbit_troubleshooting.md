---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common Orbit indexing, query, and MCP connection issues.
availability_details: no
title: Troubleshooting Orbit
---

When Orbit returns less data than expected, start by checking whether the data is
indexed and whether the same user can see that data in GitLab.

## Data is missing from Orbit

You might notice that data is missing from the dashboard, query results, or AI
agent answers.

### Orbit is not turned on for the top-level group

Orbit indexes top-level groups. If Orbit is not turned on for the root group that
contains the project, Orbit does not index that project.

To resolve this issue:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Turn on Orbit for the top-level group.
1. Wait for indexing to start.

### Indexing has not finished

Initial indexing and reindexing can take time, especially for large groups or
large repositories.

To resolve this issue:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Check **Indexed content** for the group or project.
1. Wait for indexing to complete.

### You do not have permission to view the data

Orbit applies GitLab permissions to query results. If you cannot see an issue,
merge request, pipeline, vulnerability, project, or file in GitLab, Orbit should
not return it to you.

To resolve this issue:

1. Sign in as the same user who ran the Orbit query.
1. Open the object in the GitLab UI.
1. If GitLab denies access, ask a group or project owner to update your role.

### Code is not on the default branch

Orbit indexes code from the project's default branch.

To resolve this issue:

1. Confirm the code exists on the default branch. In most projects, the default
   branch is `main` or `master`.
1. If the code exists only on a feature branch, merge or cherry-pick it into the
   default branch.

## Query returns an error

### Query type is not supported

Orbit supports these query types:

- `traversal`
- `aggregation`
- `path_finding`
- `neighbors`

To resolve this issue, update the `query_type` field. For more information, see
[Orbit query language](queries/query_language.md).

### Node or relationship does not exist

If a query names a node type, relationship, or property that is not in the
schema, Orbit rejects the query.

To resolve this issue:

1. Open **Orbit** > **Schema**.
1. Confirm the node type, relationship, and property names.
1. Update the query to use names from the schema.

## MCP connection returns `403 incorrect_scope`

GitLab uses `mcp-remote` to establish secure connections between Orbit and AI
tools running on your local computer. A known issue can cause the connection to
fail with a `403 incorrect_scope` error. To resolve this issue, manually
register the client before establishing the connection.

To resolve this issue, use the GitLab CLI to configure Orbit:

```shell
glab orbit setup
```

If you cannot use the GitLab CLI, manually register an MCP client:

1. From the command line, run:

   ```shell
   npx mcp-remote "https://gitlab.com/api/v4/orbit/mcp"
   ```

1. In your browser, review and approve the authorization request.

   The command fails with `403 incorrect_scope`. It also creates a cache
   directory at `~/.mcp-auth/mcp-remote-<version>/` with two files:

   - `<hash>-_client_info.json`
   - `<hash>_tokens.json`

1. Save the file names for the next steps.
1. Register a client:

   ```shell
   curl --request POST \
     --header "Content-Type: application/json" \
     --data '{"redirect_uris": ["http://localhost:42826/oauth/callback"], "client_name": "MCP CLI Proxy", "resource": "https://gitlab.com/api/v4/orbit/mcp"}' \
     --url "https://gitlab.com/oauth/register"
   ```

1. In the response, verify that `scope` is set to `mcp_orbit` and save the
   values of:

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

The connection should now succeed. Use the MCP URL
`https://gitlab.com/api/v4/orbit/mcp`.

## Local indexer cannot find data

The local indexer stores local code graph data in `~/.orbit/graph.duckdb` by
default. It does not query the deployed GitLab.com graph.

To resolve this issue:

1. Confirm you indexed the repository:

   ```shell
   ./target/release/orbit index /path/to/repository
   ```

1. Confirm you are querying the same local data directory:

   ```shell
   echo "$ORBIT_DATA_DIR"
   ```

1. Inspect the local schema:

   ```shell
   ./target/release/orbit schema --ontology
   ```

For more information, see [Local Orbit indexer developer preview](local_indexer.md).
