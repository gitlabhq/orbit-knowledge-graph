---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: How GitLab Orbit Remote secures your data, including the roles required to query, the authorization model, and programmatic access.
title: GitLab Orbit Remote security
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

Responses from queries made to GitLab Orbit include only the information that is available to
your role. If you or an agent try to access a part of GitLab that requires a higher user
role, related information will not be displayed in the graph.

Access in GitLab Orbit is hierarchical. A role assigned at the top-level group applies to every
subgroup and project beneath it. Turning on GitLab Orbit does not change existing access.

## Roles required to query GitLab Orbit

To query a group, you must have the Reporter role or higher for that group.

Access to security data requires the Security Manager role. This includes the following data:

- Vulnerabilities
- Security findings
- Security scans
- Scanners
- CVE/CWE identifiers

The Security Manager role is required because aggregate results cannot be filtered after
query execution, which could otherwise expose security details to users with the Reporter
role. A user with the Reporter role can query the rest of the graph, but security entities
are dropped from results, including from aggregate counts.

| Data domain | Minimum role |
|---|---|
| Core, code review, CI/CD, planning | Reporter |
| Security | Security Manager |

## Security architecture

GitLab Orbit never invents permissions. GitLab is the single source of truth for who can see what,
and every query is authorized through GitLab.

Access is enforced in the following layers:

- Authorized namespace scope. Results are limited to the groups, subgroups, and
  projects where you hold the required role. These namespaces can belong to more than
  one organization, and sibling groups stay out of scope.
- Checks on each result. Before results are returned, GitLab re-checks your permission on
  each item and removes anything you cannot access. This catches confidential items and
  runtime controls such as SAML group links and IP restrictions.

Group [IP address restrictions](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address) apply to query results: a request from an IP outside a group's allowed ranges returns no results from that group.

GitLab Orbit is read-only. It reads changes from GitLab and never writes back, runs in a separate
environment, and stores no permission data of its own.

## Programmatic access

Programmatic access uses your existing GitLab authentication, scoped to what the token owner
can see in GitLab.

- REST API: a personal access token with the `read_api` scope, or a fine-grained personal
  access token with the GitLab Orbit **Read** permission, sent as a Bearer token. For more information,
  see [REST API](access/api.md).
- MCP: GitLab OAuth. Native HTTP clients request the `mcp_orbit` scope. A fine-grained personal
  access token needs the same GitLab Orbit **Read** permission as the REST API.
  For more information, see [MCP](access/mcp.md).
- GitLab Duo Agent Platform: no token to configure. For more information, see [GitLab Duo Agent Platform](access/duo.md).

### Fine-grained personal access tokens

A fine-grained token works with GitLab Orbit the same way it works with
[Global Search](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens_rest/#search-resources).
The token check is a gateway: it decides whether the token may call the endpoint. The results
follow the token owner's access in GitLab, not the other permissions on the token.

- Add the GitLab Orbit **Read** permission under the **User** tab. The token can then call every
  GitLab Orbit REST endpoint and the MCP endpoint, and sees everything the token owner can see.
- Or add the GitLab Orbit **Read** permission under **Group and project access** for one or more
  groups. The token can call the same endpoints. GitLab Orbit then trims the token owner's access
  to the selected groups and their subgroups. Data outside those groups does not appear in results.
- A token with the permission under both tabs, or with **All memberships**, is not trimmed.
- GitLab Orbit does not support project selections. A token that selects only projects gets `403`.
- A fine-grained token without that permission gets `403` from every GitLab Orbit endpoint,
  including MCP.
- Other permissions on the token have no effect on GitLab Orbit results. A token without the Work item
  **Read** permission still gets work items from GitLab Orbit when the owner can read them in GitLab.
- The Reporter floor and the Security Manager rule in
  [Roles required to query GitLab Orbit](#roles-required-to-query-gitlab-orbit) still apply.
- Personal access tokens do not go through SAML SSO enforcement. A token from a user with an
  expired SAML session still reads what the user's memberships allow.
