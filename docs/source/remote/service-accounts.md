---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query GitLab Orbit from scripts, CI/CD jobs, and AI agents with a service account whose group memberships limit what it can read.
title: Service accounts
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

Use a [service account](https://docs.gitlab.com/user/profile/service_accounts/) to query GitLab Orbit from a script, a CI/CD job, or an AI agent.
A service account is a bot user with no password.
The account authenticates to the [GitLab Orbit REST API](access/api.md) with a personal access token.
Like a person, the account sees only data from the groups where it is a member.

## Service account scope

A new service account has no access to the graph.
The account gets scope from these memberships:

- Direct membership of a group with the Reporter role or higher.
  The membership also covers all subgroups and projects in that group.
- Membership of a group that is shared with another group.
  The account gets the lower of its own role and the role in the group link.

These memberships do not add to the scope:

- Membership of a project only.
- Membership with the Guest or Planner role.
- An expired membership.
- Membership of a group outside the top-level groups where GitLab Orbit is on.

The role controls which data the account can query:

| Role                         | Data the account can query                             |
|------------------------------|--------------------------------------------------------|
| Reporter or higher           | Core, code review, CI/CD, and planning entities        |
| Security Manager             | Reporter data, and vulnerabilities and findings        |
| Administrator or auditor     | All data in every group where GitLab Orbit is on       |

For more information, see [roles required to query GitLab Orbit](security.md#roles-required-to-query-gitlab-orbit).

> [!warning]
> Do not give a query bot administrator or auditor access.
> GitLab Orbit does not limit the scope of these accounts, so a leaked token exposes every group where GitLab Orbit is on.

## Set up a service account for GitLab Orbit

Set up a service account to give a tool read access to specific groups.

Prerequisites:

- The Owner role for the top-level group.

To set up a service account:

1. [Create a group service account](https://docs.gitlab.com/user/profile/service_accounts/#create-a-service-account).
   A project service account can only join its own project, so it cannot get scope.
   A group service account can only join its own group and subgroups, so create one for each top-level group.
1. [Create a personal access token](https://docs.gitlab.com/user/profile/service_accounts/#create-a-personal-access-token-for-a-service-account) for the account.
   Fine-grained personal access tokens are not supported.
   - Select only the `read_api` scope.
   - Set an expiration date.
   - Store the token in a secret store, such as a [masked CI/CD variable](https://docs.gitlab.com/ci/variables/#mask-a-cicd-variable).
     GitLab shows the token only once.
1. [Add the account to each group](https://docs.gitlab.com/user/profile/service_accounts/#add-a-service-account-to-a-group-or-project) that the tool must query.
   Select the Reporter role, or the Security Manager role if the tool must read security data.

## Verify the scope of a service account

Verify the scope after you set up the account, and each time you change its memberships.

To verify the scope:

1. Put this query in a file named `request.json`:

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
     --header "Authorization: Bearer <your_access_token>" \
     --header "Content-Type: application/json" \
     --data @request.json \
     --url "https://gitlab.com/api/v4/orbit/query"
   ```

1. Check that the `full_path` values match only the groups that you added the account to.
   If other groups appear, check that the account is not an administrator or auditor.

## Limit the access of a service account

Give each service account the smallest scope that its task needs:

- Use one service account for each tool, so you can revoke one token without an effect on other tools.
- Add the account to the lowest subgroup that contains the data.
- [Rotate the token](https://docs.gitlab.com/user/profile/service_accounts/#rotate-a-personal-access-token) before it expires.
  An expired token stops all queries from the tool.
- Remove the account from a group when the tool no longer needs that group.

## Related topics

- [GitLab Orbit Remote security](security.md)
- [Personal access tokens](https://docs.gitlab.com/user/profile/personal_access_tokens/)

## Troubleshooting

When you query GitLab Orbit with a service account, you might encounter the following issues.

### Error: `403 Forbidden - No Orbit enabled namespaces available`

This error occurs when the service account has no membership that gives it scope.

To resolve this issue, add the account with the Reporter role or higher to a group in a top-level group where GitLab Orbit is on.

### Error: `403 Forbidden`

A `403 Forbidden` response with no other message occurs when no group of the account has a license for GitLab Orbit.

To resolve this issue, add the account to a group in a top-level group with a Premium or Ultimate subscription where GitLab Orbit is on.
If you added the account a short time ago, GitLab can keep the earlier result for a few minutes.
Wait a few minutes, then send the query again.

### Error: `404 Not Found`

A `404 Not Found` response from all GitLab Orbit endpoints occurs when the `knowledge_graph` feature flag is not enabled for the service account.
GitLab checks the flag for each user, so the flag can be on for you and off for the service account.

To resolve this issue, ask your GitLab administrator to enable the flag for the service account.

### Results do not include security data

GitLab Orbit removes security entities from the results and from aggregate counts when the account has the Reporter role.

To resolve this issue, give the account the Security Manager role in the group.
