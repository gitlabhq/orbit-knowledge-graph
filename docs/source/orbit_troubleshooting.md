---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common issues with Orbit indexing and knowledge graph results.
availability_details: no
title: Troubleshooting Orbit
---

When working with Orbit, you might encounter the following issues.

## Data missing from knowledge graph

You might notice that certain data does not appear in the knowledge graph or in AI agent answers.

### Orbit is not turned on for the top-level group

This issue occurs when Orbit is not turned on for the top-level group that contains the subgroup, project, or repository you expect.

To resolve this issue:

1. Turn Orbit on for the top-level group.
1. Wait for the initial indexing to complete.

### Indexing is in progress

This issue occurs when indexing for the group or project is in progress or is temporarily backlogged.

To resolve this issue:

- Wait for indexing to complete.

### User does not have permission to view the data

This issue occurs when you do not have permission to view the data in GitLab.

To resolve this issue:

1. Confirm you can see the data in the GitLab UI with the same user account.
1. If you cannot, adjust GitLab project or group membership and roles to grant access.

### Code is not on the project's default branch

This issue occurs when the code you expect to see is not on the project's default branch.

To resolve this issue:

1. Confirm the code exists on the default branch. In most projects, the default branch is `main` or `master`.
1. If the code exists only on a feature branch, merge or cherry-pick it into the default branch.

## `Error: 403 incorrect_scope`

GitLab uses `mcp-remote` to establish secure connections between Orbit
and AI tools running on your local computer. A known issue can cause
the connection to fail with a `403 incorrect_scope` error. To resolve
this issue, you must manually register the client before establishing
the connection.

To resolve this issue, use the GitLab CLI to configure Orbit.

If you cannot use the GitLab CLI, you can manually register an MCP client:

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
1. Follow the instructions to [connect a client to the GitLab MCP server](https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/#connect-a-client-to-the-gitlab-mcp-server).
   Use the URL `https://gitlab.com/api/v4/orbit/mcp`.
