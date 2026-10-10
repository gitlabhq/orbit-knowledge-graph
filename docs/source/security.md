---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn how GitLab Orbit protects your data, which roles you need to query it, and how to set up programmatic access.
title: GitLab Orbit security
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

A query to the server graph returns only the data that your role lets you see in GitLab.
If you or an agent ask for data that needs a higher role, the response does not include it.

The local graph has no authorization layer and needs no authentication.
Anyone who can run the CLI on your machine can read all data in it.
For more information, see [How GitLab Orbit works](how-it-works.md).

## Roles

Access is hierarchical. A role in a top-level group applies to each subgroup and project in it.
When you turn on GitLab Orbit, existing access does not change.

| Action | Minimum role |
|--------|--------------|
| Query core, code review, CI/CD, and planning data | Reporter |
| Query security data | Security Manager |
| Turn on indexing | Owner of the top-level group |

To turn on indexing, see [turn on GitLab Orbit for your group](_index.md#turn-on-gitlab-orbit-for-your-group).

Security data includes vulnerabilities, security findings, security scans, scanners, and CVE and CWE identifiers.

Security data needs the Security Manager role because GitLab cannot filter aggregate results after
the query runs. Without this rule, aggregate counts could show security details to a user with the Reporter role.
A user with the Reporter role can query the rest of the graph.
The response drops security entities for that user, also from aggregate counts.

Administrators and auditors can read all resources on the instance.
Their queries return data from each group where GitLab Orbit is on, not only their own groups.

## How GitLab enforces access

GitLab is the single source of truth for permissions. GitLab authorizes every query.
GitLab Orbit never adds permissions of its own.

GitLab enforces access in two layers:

- Namespace scope. Results include only the groups, subgroups, and projects where you have the
  required role. These namespaces can be in more than one organization. Sibling groups stay out of scope.
- A check on each result. Before GitLab returns the results, it checks your permission on each item again.
  It removes each item that you cannot access. This check catches confidential items and runtime controls,
  such as SAML group links and IP address restrictions.

Group [IP address restrictions](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address)
apply to query results. A request from an IP address outside the allowed ranges of a group returns no results from that group.

The server graph is read-only. It reads changes from GitLab and never writes back.
It runs in a separate environment and stores no permission data.

## Programmatic access

Programmatic access uses your GitLab authentication.
Results include only what the token owner can see in GitLab.

| Access method | Authentication |
|---------------|----------------|
| [REST API](queries/api.md) | A personal access token with the `read_api` scope, or a fine-grained personal access token, sent as a Bearer token |
| [MCP endpoint](agents/mcp.md) | GitLab OAuth. Native HTTP clients request the `mcp_orbit` scope. |
| [GitLab Duo Agent Platform](agents/duo.md) | No token to configure |
| [Service account](#service-accounts) | A personal access token of a bot user, scoped to the groups where the account is a member |

### Fine-grained personal access tokens

When you use a fine-grained personal access token:

- Results include only what the token owner can see.
- Group and project resources are not supported. A token with only group and project resources
  gets a `403 Forbidden` response during authentication.
- SAML SSO enforcement does not apply. The token continues to work after the SAML session of its owner expires.

To use a token with GitLab Orbit, add the **GitLab Orbit** resource to the token when you create it.
For more information, see
[create a fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/#create-a-fine-grained-personal-access-token).

## Service accounts

Use a [service account](https://docs.gitlab.com/user/profile/service_accounts/) to query the graph
from a script, a CI/CD job, or an AI agent.
To see data from a group, the service account must be one of these:

- A direct member of the group.
- A member of a group that is shared with the group.

Project membership does not give access.
Membership of a group outside the top-level groups where GitLab Orbit is on does not give access.

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
   for the service account with the `read_api` scope. Fine-grained personal access tokens are not supported.
1. [Add the service account](https://docs.gitlab.com/user/profile/service_accounts/#add-a-service-account-to-a-group-or-project)
   to the lowest subgroup that has the data. If the tool must read security data, select the **Security Manager** role.

### Verify the scope of a service account

Verify the scope after you set up the account. Do it again each time you change its memberships.

To verify the scope:

1. Save this [query](queries/query-language.md) in a file named `request.json`.

   ```json orbit-query
   {
     "query": {
       "query_type": "traversal",
       "nodes": [{
         "id": "g",
         "entity": "Group",
         "filters": {"visibility_level": {"in": ["private", "internal", "public"]}},
         "columns": ["name", "full_path"]
       }],
       "limit": 50
     },
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
