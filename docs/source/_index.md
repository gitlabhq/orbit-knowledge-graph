---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query your GitLab instance as a property graph. Find blast radius, trace dependencies, and answer SDLC questions that GitLab alone cannot.
title: GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

GitLab Orbit indexes your local code repositories and remote GitLab data to create
a queryable, read-only property graph. The graph provides a point-in-time
snapshot of your entire GitLab instance and checked out code.

Query the graph to learn about the relationships between
your source code, merge requests, pipelines, and other SDLC data.

Follow the tutorial below to get started.

## Prerequisites

- A top-level group with GitLab Orbit Remote indexing turned on. To check, go to the
  [GitLab Orbit dashboard](https://gitlab.com/dashboard/orbit/explore), or ask
  your group Owner.
- Install the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/), version 1.115.0 or later.

## Step 1: Set up your AI assistant

Connect GitLab Orbit to:

- GitLab Duo Agent Platform
- An external agent, like Claude Code

For the most coverage, use both.

### Use GitLab Duo Agent Platform

Turn on the GitLab Duo Agent Platform setting
for GitLab Orbit to give agents access to
your graph:

1. In the top bar, select **Search or go to** > **Preferences**.
1. Under **Behavior**, select the **Use Orbit in GitLab Duo** checkbox. Keep the defaults.
1. Select **Save changes**.
1. Open the GitLab Duo Chat sidebar and confirm GitLab Orbit is turned on.

Now, foundational agents can access your graph.

If GitLab Orbit doesn't appear, indexing might not be turned on for your group.
For help, see [troubleshooting](troubleshooting.md#exit-code-2).

### Use an external agent

To connect to an external agent:

1. Install the GitLab Orbit CLI:

   ```shell
   glab orbit --install
   ```

1. Set up your AI assistant:

   ```shell
   glab orbit setup claude
   ```

   Replace `claude` with `codex`, `opencode`, or `pi`.

1. Verify the connection:

   ```shell
   glab orbit remote status
   ```

   ```json
   {
     "status": "healthy",
     "version": "0.115.0"
   }
   ```

## Step 2: Run your first query

Check out an indexed project, or go to one in GitLab, and ask your agent:

```plaintext
Using Orbit, tell me what does this project do, and how is it structured?
```
