---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn how GitLab Orbit protects your data, which roles you need to query it, and how to set up programmatic access.
title: GitLab Orbit security
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

A query returns only the data that your role lets you see in GitLab, even when an agent sends it.

## Roles

A role for a top-level group applies to each subgroup and project in it.
When you turn on GitLab Orbit, existing access does not change.

| Action | Roles |
|--------|-------|
| Query groups, projects, users, merge requests, CI/CD, and work items | Reporter, Security Manager, Developer, Maintainer, or Owner |
| Query security data | Security Manager, Developer, Maintainer, or Owner |
| [Turn on GitLab Orbit](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group) | The Owner role for the top-level group. On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn on GitLab Orbit. |

Security data is vulnerabilities, security findings, security scans, scanners,
and Common Vulnerabilities and Exposures (CVE) and Common Weakness Enumeration (CWE) identifiers.
Because GitLab cannot filter aggregate results after a query runs, the response for a user with the Reporter role
leaves out security entities, even from aggregate counts.

Queries from administrators and auditors return data from each group where GitLab Orbit is on, not only their own groups.

## How GitLab Orbit enforces access

GitLab authorizes every query.
GitLab Orbit adds no permissions of its own, and stores no permission data.

GitLab Orbit enforces access in two layers:

- Namespace scope. Results include only the groups where you have the required role, directly or through a shared group,
  and their subgroups and projects.
  A role for a project alone does not give access to that project.
  A group next to yours in the same parent group stays out of scope unless you have a role for it.
- A check on each result. Before GitLab returns the results, it checks your permission on each item again,
  and removes each item that you cannot access.
  This check covers confidential items, SAML group links,
  and group [IP address restrictions](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address).
  A request from outside the allowed IP address ranges of a group returns no results from that group.

The graph is read-only.
It reads changes from GitLab and never writes back.

## Programmatic access

Results include only what the token owner can see in GitLab.

| Access method | Authentication |
|---------------|----------------|
| [REST API](https://docs.gitlab.com/api/orbit/) | A personal access token with the `read_api` scope, or a fine-grained personal access token, sent as a Bearer token |
| [GitLab Orbit CLI](cli.md) | Your `glab auth login` credential, or a personal access token with the `read_api` scope in `GITLAB_TOKEN` |
| [Model Context Protocol (MCP) endpoint](agents/mcp.md) | GitLab OAuth, where the client requests the `mcp_orbit` scope, or a personal access token with the `read_api` scope |
| [GitLab Duo Agent Platform](agents/duo.md) | No token to configure |
| [Service account](#service-accounts) | A personal access token of a bot user, scoped to the groups where the account is a member |

### Fine-grained personal access tokens

When you [create a fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/#create-a-fine-grained-personal-access-token),
add the **GitLab Orbit** resource.

GitLab Orbit does not support group and project resources.
A token with only group and project resources gets `403 Forbidden`.

SAML SSO enforcement does not apply.
The token keeps working after the SAML session of its owner expires.

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
- GitLab Orbit is [turned on for the group](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).

To set up a service account for GitLab Orbit:

1. [Create a service account and a personal access token](https://docs.gitlab.com/user/profile/service_accounts/#create-a-service-account) with the `read_api` scope.
   Service accounts cannot use fine-grained personal access tokens.
1. Add the service account with the Reporter role to the lowest subgroup that has the data.
   To read security data, use the Security Manager role.
   Repeat this step for each group that the service account must query.

### Verify the scope of a service account

Verify the scope after you set up the account, and each time you change its memberships:

1. Save this request in a file named `request.json`.
   The `query` value is a [GQL](queries/query-language.md) query.

   ```json
   {
     "query": "MATCH (g:Group) WHERE g.visibility_level IN ['private', 'internal', 'public'] RETURN g.name, g.full_path LIMIT 50",
     "response_format": "raw"
   }
   ```

1. Send the query with the service account token.

   ```shell
   curl --request POST \
     --header "Authorization: Bearer <your_access_token>" \
     --header "Content-Type: application/json" \
     --data @request.json \
     --url "https://gitlab.example.com/api/v4/orbit/query"
   ```

1. Make sure that the `full_path` values include only the groups where you added the account, and their subgroups.
   If you see other groups, check the memberships of the account, including groups that its groups are shared with.

## Related topics

- [GitLab Orbit architecture](how-it-works.md)
- [Troubleshooting](troubleshooting.md)
