---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit gives your agent a graph of your GitLab data and code, so it can answer questions about changes, pipelines, security, and code structure.
title: GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

<!-- TODO(GA): set the GA release and link the GA work item in the history line. -->

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Feature flag `knowledge_graph` removed](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.
- Generally available in GitLab 19.x.

{{< /history >}}

GitLab Orbit indexes your GitLab data and code into a graph that your agent can query.
Ask what breaks if you change a service, who should review a file, or why pipelines fail.
Your agent answers with a few graph queries, not hundreds of file reads and API calls.

{{< cards >}}

- [Quickstart](quickstart.md)
- [Use cases](use-cases.md)
- [How GitLab Orbit works](how-it-works.md)

{{< /cards >}}

## What you can ask

| Goal | Example prompt |
|------|----------------|
| [Understand a codebase](use-cases.md#understand-a-codebase) | `I'm new to <my-group/my-project>. Using GitLab Orbit, give me a tour.` |
| [Find the blast radius of a change](use-cases.md#find-the-blast-radius-of-a-change) | `Using GitLab Orbit, what breaks if I change the public interface of <shared-payments-lib>?` |
| [Review the history of a project](use-cases.md#review-the-history-of-a-project) | `Using GitLab Orbit, who changed <app/models/project.rb> most, and who should review my change to it?` |
| [Keep your pipelines healthy](use-cases.md#keep-your-pipelines-healthy) | `Using GitLab Orbit, which CI/CD jobs fail most often in <my-group>?` |
| [Trace security risk to its source](use-cases.md#trace-security-risk-to-its-source) | `Using GitLab Orbit, which projects in <my-group> have critical vulnerabilities?` |

## See an answer

Ask your agent:

```plaintext
Using GitLab Orbit, which projects in <my-group> have the most failed pipelines?
```

The agent runs one graph query in place of many API calls:

```gql orbit-query
MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project)
WHERE pl.status = 'failed' AND p.full_path STARTS WITH 'my-group/'
RETURN p.full_path, count(pl) AS failed_pipelines
ORDER BY failed_pipelines DESC
LIMIT 3
```

Then it answers from the rows. The answer is similar to:

```plaintext
1. my-group/payments-api: 47 failed pipelines
2. my-group/web-frontend: 31 failed pipelines
3. my-group/auth-service: 12 failed pipelines
```

## Before you start

<!-- TODO(GA): confirm with Pricing what the add-on includes. -->
- GitLab Orbit is a paid add-on for top-level groups with a Premium or Ultimate subscription.
  On GitLab.com, an Owner can [start a free trial](trial.md).
- An Owner turns on GitLab Orbit for a top-level group. On GitLab Self-Managed and GitLab Dedicated,
  an administrator can also turn it on. GitLab Orbit then indexes the group, its subgroups, and projects.
  On GitLab Dedicated, GitLab runs GitLab Orbit for you.
- On GitLab Self-Managed, an administrator [sets up GitLab Orbit](self-managed/_index.md) first.
- To query the graph, you need the Reporter, Security Manager, Developer, Maintainer, or Owner role. Security data needs the Security Manager role.
  For more information, see [roles](security.md#roles).

## Connect your agent

| Connect with | Use it when |
|--------------|-------------|
| [GitLab Orbit CLI and skill](agents/_index.md) | You use Claude Code, Codex, OpenCode, or another agent that runs shell commands. |
| [GitLab Duo](agents/duo.md) | You use GitLab Duo Agent Platform. You do not install anything. |
| [MCP endpoint](agents/mcp.md) | Your agent supports MCP and cannot run shell commands. |
| [REST API](queries/api.md) | You build a script, a scheduled report, or a custom tool on the graph. |

## Code on your machine

The GitLab Orbit CLI also indexes repositories on your machine, so your agent can search code that you have not pushed,
offline and with no GitLab account. For more information, see [GitLab Orbit Local](local/_index.md).

## Billing

For which calls consume GitLab Credits on each offering, see [billing](how-it-works.md#billing).

## Reference

{{< cards >}}

- [GitLab Orbit CLI](cli.md)
- [What GitLab Orbit indexes](schema.md)
- [Queries](queries/_index.md)
- [Security](security.md)
- [GitLab Self-Managed](self-managed/_index.md)
- [Troubleshooting](troubleshooting.md)

{{< /cards >}}
