---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn how GitLab Orbit protects your data, which roles you need to query it, and how to set up programmatic access.
title: GitLab Orbit security
---

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Feature flag `knowledge_graph` removed](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.
- Generally available in GitLab 19.5.

{{< /history >}}

A query returns only the data that your role lets you see in GitLab, also when an agent sends it.

The [local graph](local/_index.md) has no authorization layer. Anyone who can read the graph file can read all of it.
For more information, see [storage and privacy](local/_index.md#storage-and-privacy).

## Roles

A role for a top-level group applies to each subgroup and project in it.
Turning on GitLab Orbit does not change existing access.

| Action | Minimum role |
|--------|--------------|
| Query groups, projects, users, merge requests, CI/CD, and work items | Reporter |
| Query security data | Security Manager |
| [Turn on indexing](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group) | The Owner role for the top-level group. On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn on indexing. |

Security data is vulnerabilities, security findings, security scans, scanners, and CVE and CWE identifiers.
It needs the Security Manager role, because GitLab cannot filter aggregate results after the query runs.
For a user with the Reporter role, the response leaves out security entities, also from aggregate counts.

Queries from administrators and auditors return data from each group where GitLab Orbit is on, not only their own groups.

## How GitLab enforces access

GitLab authorizes every query. GitLab Orbit adds no permissions of its own, and stores no permission data.

GitLab enforces access in two layers:

- Namespace scope. Results include only the groups where you have the required role, directly or through a shared group, and their subgroups and projects. A role for a project alone does not give access to that project.
  A group next to yours in the same parent group stays out of scope unless you have a role for it.
- A check on each result. Before GitLab returns the results, it checks your permission on each item again,
  and removes each item that you cannot access. This check covers confidential items, SAML group links,
  and group [IP address restrictions](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address).
  A request from outside the allowed IP address ranges of a group returns no results from that group.

The graph is read-only. It reads changes from GitLab and never writes back.

## Programmatic access

Results include only what the token owner can see in GitLab.

| Access method | Authentication |
|---------------|----------------|
| [REST API](queries/api.md) | A personal access token with the `read_api` scope, or a fine-grained personal access token, sent as a Bearer token |
| [MCP endpoint](agents/mcp.md) | GitLab OAuth. Native HTTP clients request the `mcp_orbit` scope. |
| [GitLab Duo Agent Platform](agents/duo.md) | No token to configure |
| [Service account](#service-accounts) | A personal access token of a bot user, scoped to the groups where the account is a member |

### Fine-grained personal access tokens

When you [create a fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/#create-a-fine-grained-personal-access-token),
add the **GitLab Orbit** resource.

- Group and project resources are not supported. A token with only group and project resources gets `403 Forbidden`.
- SAML SSO enforcement does not apply. The token keeps working after the SAML session of its owner expires.

## Service accounts

Use a [service account](https://docs.gitlab.com/user/profile/service_accounts/) for automation that runs
without a personal account, such as a script, a scheduled report, or an agent.
To see data from a group, the service account must be a direct member of the group,
or a member of a group that is shared with the group.
Project membership, and membership of groups where GitLab Orbit is off, do not give access.

> [!warning]
> Do not give a service account administrator or auditor access.
> A leaked token can show data from every group where GitLab Orbit is on.

### Set up a service account

Prerequisites:

- The Owner role for the group.

To set up a service account:

1. [Create a group service account](https://docs.gitlab.com/user/profile/service_accounts/#create-a-service-account)
   in each top-level group that the tool must query.
1. [Create a personal access token](https://docs.gitlab.com/user/profile/service_accounts/#create-a-personal-access-token-for-a-service-account)
   for the service account with the `read_api` scope. Service accounts cannot use fine-grained personal access tokens.
1. [Add the service account](https://docs.gitlab.com/user/profile/service_accounts/#add-a-service-account-to-a-group-or-project)
   to the lowest subgroup that has the data. If the tool must read security data, select the **Security Manager** role.

### Verify the scope of a service account

Verify the scope after you set up the account, and each time you change its memberships:

1. Save this request in a file named `request.json`. The query uses the [Cypher query language](queries/cypher.md).

   ```json
   {
     "query": "MATCH (g:Group) WHERE g.visibility_level IN ['private', 'internal', 'public'] RETURN g.name, g.full_path LIMIT 50",
     "response_format": "raw"
   }
   ```

1. Send the query with the service account token.

   ```shell
   curl --request POST \
     --header "Authorization: Bearer <your_token>" \
     --header "Content-Type: application/json" \
     --data @request.json \
     --url "https://gitlab.com/api/v4/orbit/query"
   ```

1. Make sure that the `full_path` values include only the groups where you added the account.

## Related topics

- [How GitLab Orbit works](how-it-works.md)
- [REST API](queries/api.md)
- [MCP](agents/mcp.md)
- [Troubleshooting](troubleshooting.md)
