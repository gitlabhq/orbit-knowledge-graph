---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query your codebase, pipelines, dependencies, and security with ready-made prompts.
title: Query examples
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

GitLab Orbit answers questions about your entire software development lifecycle.
Each example pairs a plain-language prompt with the graph queries an agent runs to answer it.

## Prompts and queries

A prompt is what you type. A query is the
JSON the agent sends to GitLab Orbit on your behalf.

You ask an AI agent a question in plain language.
The agent turns that question into one or more GitLab Orbit queries, runs them, and explains the
results.

The first prompt in each example starts with `Using GitLab Orbit`.
That phrase tells the agent to consult the graph instead of searching your repository directly.
The exact wording is not required, but a prompt that does not mention GitLab Orbit might be
answered from another source.
Later steps continue in the same conversation, so they do not repeat the phrase.

Where you type the prompt depends on your agent:

- GitLab Duo Chat and GitLab Duo Agent Platform have GitLab Orbit built in.
- External agents, such as Claude Code or Codex, connect through the
  [MCP server](../access/mcp.md) or the [`glab` CLI](../access/glab.md).

| Example | Description |
|---------|-------------|
| [Attribute CI/CD compute cost to code](attribute-ci-cost.md) | Rank recurring failures and trace them to the code that causes them |
| [Understand an unfamiliar codebase](understand-a-codebase.md) | Find the contributors, core classes, and entry points of a project |
| [Map the blast radius of a change](map-blast-radius.md) | Find everything that depends on a library before you change it |
| [Triage pipeline failures](triage-pipeline-failures.md) | Rank projects, jobs, and failure reasons across your organization |
| [Trace security risk to its source](trace-security-risk.md) | Prioritize open vulnerabilities and trace how they got there |
| [Read source code from your agent](read-source-code.md) | Pull a file or a single definition into the conversation |
