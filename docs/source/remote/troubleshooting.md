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
