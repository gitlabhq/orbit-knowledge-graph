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

Administrators and auditors can read all resources on the instance.
Their queries return data from every group where GitLab Orbit is on, not only their own groups.

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

- REST API: a personal access token with the `read_api` scope, or a
  [fine-grained personal access token](#fine-grained-personal-access-tokens), sent as a Bearer token.
  For more information, see the [REST API](access/api.md#authentication).
- Service accounts: a bot user with a personal access token, scoped to the groups where the
  account is a member. For more information, see [service accounts](#service-accounts).
- MCP: GitLab OAuth. Native HTTP clients request the `mcp_orbit` scope. For more information, see [MCP](access/mcp.md).
- GitLab Duo Agent Platform: no token to configure. For more information, see [GitLab Duo Agent Platform](access/duo.md).

### Fine-grained personal access tokens

You can use a fine-grained personal access token
to authenticate with GitLab Orbit Remote.

If you use a fine-grained personal access token:

- Results from read operations are scoped to the token owner's access level.
- Group and project resources are not supported. A token generated with only group and project resources
gets a `403 Forbidden` response during authentication.
- SAML SSO enforcement does not apply to personal access tokens. A token continues to work after the owner's SAML session expires.

If you want to configure a token to
work with GitLab Orbit Remote, add the **GitLab Orbit** resource
to the token when you create it. For more information,
see [create a fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/#create-a-fine-grained-personal-access-token).

## Service accounts

Use a [service account](https://docs.gitlab.com/user/profile/service_accounts/) to query GitLab Orbit from a script, a CI/CD job, or an AI agent.
To see data from a group, a service account must be either:

- A direct member of the group.
- A member of the group that is shared with the target group.

Membership to a project, or of a group outside the top-level groups where
GitLab Orbit is turned on, does not grant access to GitLab Orbit.

> [!warning]
> Do not give a service account administrator or auditor access.
> A leaked token can expose every group where GitLab Orbit is turned on.

### Set up a service account

Set up a service account so a tool can query GitLab Orbit without a personal account.

Prerequisites:

- The Owner role for the group.

To set up a service account:

1. [Create a group service account](https://docs.gitlab.com/user/profile/service_accounts/#create-a-service-account) in each top-level group the tool must query.
1. [Create a personal access token](https://docs.gitlab.com/user/profile/service_accounts/#create-a-personal-access-token-for-a-service-account) for the service account with the scope set to `read_api`. Fine-grained personal access tokens are not supported.
1. [Add the service account](https://docs.gitlab.com/user/profile/service_accounts/#add-a-service-account-to-a-group-or-project) to the lowest subgroup with queryable data.
   - Select the **Security Manager** role if the tool must read security data.

### Verify the scope of a service account

Verify the scope after you set up the account, and each time you change its memberships.

To verify the scope:

1. Add the following [JSON query](queries/query-language.md) in a file called `request.json`:

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

1. Send the query with the service account token:

   ```shell
   curl --request POST \
     --header "Authorization: Bearer <your_token>" \
     --header "Content-Type: application/json" \
     --data @request.json \
     --url "https://gitlab.com/api/v4/orbit/query"
   ```

1. Check that the `full_path` values match only the groups that you added the account to.
