---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: How Orbit Remote secures your data, including the roles required to query, the authorization model, licensing, and programmatic access.
title: Orbit Remote security
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

Orbit reuses the existing GitLab permissions of the user making a query. A query runs as
that user and returns only what they can already see in GitLab. If your user or agent
cannot see something in the GitLab UI, they cannot see it in the graph. Enabling Orbit on a
group grants no one access they did not already have, and access is hierarchical: a role on
a top-level group applies to every subgroup and project beneath it.

## Roles required to query

To query a group, you need the Reporter role or higher on it. This matches the access level
other GitLab Analytics features require.

Security data has a higher requirement: the Security Manager role. The security domain
covers vulnerabilities, security findings, security scans, scanners, and CVE/CWE
identifiers. The higher requirement keeps aggregations secure: a count or group-by cannot
be filtered row by row after it runs, so without it a Reporter could infer security details
from aggregate results alone. A user with only the Reporter role still queries the rest of
the graph, but security entities are dropped from results, including from aggregate counts.

| Data domain | Minimum role |
|---|---|
| Core, code review, CI/CD, planning | Reporter |
| Security | Security Manager |

## Security architecture

Orbit never invents permissions. GitLab is the single source of truth for who can see what,
and every query is authorized through GitLab.

Access is enforced in layers:

- **Organization isolation.** A query only ever sees data in your own organization.
- **Hierarchical, role-based scoping.** Results are limited to the groups, subgroups, and
  projects where you hold the required role. Sibling groups stay out of scope.
- **Per-result checks.** Before results are returned, GitLab re-checks your permission on
  each item and removes anything you cannot access. This catches confidential items and
  runtime controls such as SAML group links and IP restrictions.

Orbit is read-only. It reads changes from GitLab and never writes back, runs in a separate
environment, and stores no permission data of its own.

## Licensing

Orbit Remote requires GitLab Premium or Ultimate. Access follows your GitLab subscription
and the requesting user's permissions: every query is authorized through GitLab, and what
Orbit returns changes as that access changes.

Programmatic queries through the REST API and MCP consume GitLab Credits. Reading the
schema, checking indexing status, and listing tools are free, and queries made through the
GitLab Duo Agent Platform are zero-rated. If your credits are exhausted, query requests are
rejected until more credits are available.

## Programmatic access

Programmatic access uses your existing GitLab authentication, scoped to what the token owner
can see in GitLab.

- REST API: a standard (legacy) personal access token with the `read_api` scope, sent as a
  Bearer token. Fine-grained personal access tokens are not yet supported. See
  [REST API](access/api.md).
- MCP: GitLab OAuth. Native HTTP clients request the `mcp_orbit` scope. See [MCP](access/mcp.md).
- GitLab Duo Agent Platform: no token to configure. See [GitLab Duo Agent Platform](access/duo.md).
