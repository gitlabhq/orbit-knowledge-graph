---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common errors in GitLab Orbit Remote.
title: Troubleshooting GitLab Orbit Remote
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

When working with GitLab Orbit Remote, you might encounter the following issues.

## Error: `glab orbit` exits with code 2

Remote `glab orbit` commands might exit with code 2.

This issue occurs when the `knowledge_graph` feature flag is not enabled for your namespace or
instance.

To resolve this issue, contact your GitLab administrator to enable the `knowledge_graph` feature
flag for your namespace.

## Error: `glab orbit` exits with code 3

Remote `glab orbit` commands might exit with code 3.

This issue occurs when you are not authenticated with the GitLab CLI.

To resolve this issue, sign in:

```shell
glab auth login
```

## Error: `insufficient_scope` on the MCP endpoint

A connection to the GitLab Orbit MCP endpoint might fail with `insufficient_scope`.

This issue occurs when the personal access token or OAuth token does not include the
`mcp_orbit` scope.
The `read_api` scope alone is not sufficient for the MCP transport.

To resolve this issue, create a token with the `mcp_orbit` scope, or authenticate again to grant
the additional scope.

## Error: `403 Forbidden` for a service account

A query from a service account might fail with `403 Forbidden - No Orbit enabled namespaces available`.

This issue occurs when the account does not have the Reporter role or higher in a group where
GitLab Orbit is turned on.

To resolve this issue, add the account to a group where GitLab Orbit is turned on,
with the Reporter role or higher.
For more information, see [service accounts](security.md#service-accounts).

## Error: `403 Forbidden` with no message

A query from a service account might fail with `403 Forbidden` and no other message.

This issue occurs when no group of the account has a license for GitLab Orbit.

To resolve this issue, add the account to a group where GitLab Orbit is on.
The top-level group must have a Premium or Ultimate subscription.
If you added the account a short time ago, GitLab can keep the earlier result for a few minutes.
Wait a few minutes, then send the query again.

## Error: `404 Not Found` for a service account

All GitLab Orbit endpoints might return `404 Not Found` for a service account.

This issue occurs when the `knowledge_graph` feature flag is not enabled for the service account.
GitLab checks the flag for each user, so the flag can be on for you and off for the service account.

To resolve this issue, ask your GitLab administrator to enable the flag for the service account.

## Service account results do not include security data

Results from a service account might not include security entities.

This issue occurs when the account has the Reporter role.
GitLab Orbit removes security entities from the results and from aggregate counts.

To resolve this issue, give the account the Security Manager role in the group.

## GitLab Orbit tools are missing from a custom flow

A custom flow runs, but the agent does not have the `orbit_list_commands` or
`orbit_invoke_command` tools. The flow does not show an error.

This issue occurs when the flow configuration does not list the GitLab Orbit
tools, or when the user who triggered the flow has not turned on GitLab Orbit.

<!-- vale orbit.StandaloneProductName = NO -->

To resolve this issue, add the tools to the flow `toolset`. Then ask the user to
select **Use Orbit in GitLab Duo** and **Other Foundational Agents** in their preferences.

<!-- vale orbit.StandaloneProductName = YES -->

For more information, see [Use GitLab Orbit in a custom flow](access/duo.md#use-gitlab-orbit-in-a-custom-flow).
