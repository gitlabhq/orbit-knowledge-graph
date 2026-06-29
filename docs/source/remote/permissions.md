---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Roles required to query Orbit Remote, including the Security Manager role for security data.
title: Orbit Remote permissions
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

Orbit reuses your existing GitLab permissions. A query runs as you and returns only the
entities you can already see in GitLab, checked at query time. Enabling Orbit on a group
does not give anyone access they did not already have. Access is hierarchical: a role on a
top-level group applies to every subgroup and project beneath it.

## Roles required to query

To query a group, you need the Reporter role or higher on it. This matches the access
level other GitLab Analytics features require.

## Security data

Security data carries a higher requirement: the Security Manager role. The security domain
covers vulnerabilities, security findings, security scans, scanners, and CVE/CWE
identifiers.

The higher requirement keeps aggregations secure. A count or group-by cannot be redacted
row by row after it runs, so without a role requirement a Reporter could infer security
details from aggregate results alone. Requiring the Security Manager role on security
entities closes that gap. A user with only the Reporter role still queries the rest of the
graph, but security entities are dropped from the results, including from aggregate counts.

| Data domain | Minimum role |
|---|---|
| Core, code review, CI/CD, planning | Reporter |
| Security | Security Manager |

These requirements apply to every access method: the REST API, MCP, `glab`, and the GitLab
Duo Agent Platform, including the Security Analyst Agent.

## Enabling Orbit

Enabling Orbit on a group is a separate action that requires the Owner role. See
[Get started](getting-started.md#prerequisites).
