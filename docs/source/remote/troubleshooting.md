---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common errors in GitLab Orbit Remote.
title: Troubleshoot GitLab Orbit Remote
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

GitLab Orbit Remote errors occur when running `glab orbit remote` commands.
GitLab Orbit Remote requires GitLab Premium or Ultimate and the `knowledge_graph`
feature flag to be enabled on your instance.

### Exit code 2

**Symptoms:** `glab orbit remote` commands exit with code 2.

**Cause:** The `knowledge_graph` feature flag is not enabled for your
namespace or instance.

**Resolution:** Contact your GitLab administrator to enable the
`knowledge_graph` feature flag for your namespace.

### Exit code 3

**Symptoms:** `glab orbit remote` commands exit with code 3.

**Cause:** You are not authenticated with the GitLab CLI.

**Resolution:** Log in:

```shell
glab auth login
```

### `insufficient_scope` on the MCP endpoint

**Symptoms:** Connecting to the GitLab Orbit MCP endpoint fails with
`insufficient_scope`.

**Cause:** The personal access token or OAuth token does not include the
`mcp_orbit` scope. The `read_api` scope alone is not sufficient for the MCP
transport.

**Resolution:** Create a new token with the `mcp_orbit` scope, or
re-authenticate to grant the additional scope.