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

Use a [service account](https://docs.gitlab.com/user/profile/service_accounts/) to query GitLab Orbit
from a script, a CI/CD job, or an AI agent.
A service account is a bot user with no password.
The account authenticates to the GitLab Orbit REST API with a personal access token.

A service account sees only the data that its group memberships allow.
If the account cannot read a group in GitLab, no results from that group appear in the graph.
GitLab Orbit applies the same authorization model to a service account as to a person.

## Service account scope

A new service account has no access to the graph.
You give it access when you add it as a member of a group.

GitLab Orbit builds the scope of each request from these memberships:

- Direct membership of a group with the Reporter role or higher.
  The membership also covers all subgroups and projects in that group.
- Membership of a group that is shared with another group.
  The account gets the lower of its own role and the role in the group link.

These memberships do not add to the scope:

- Membership of a project only.
  To query a project, add the account to a group that contains the project.
- Membership with the Guest or Planner role.
- An expired membership.
- Membership of a group outside the top-level groups where GitLab Orbit is on.

The role that the account holds controls which data domains it can read:

| Access                       | Data the account can query                             |
|------------------------------|--------------------------------------------------------|
| Guest or Planner             | None                                                   |
| Reporter or higher           | Core, code review, CI/CD, and planning entities        |
| Security Manager             | Reporter data, and vulnerabilities and findings        |
| Administrator or auditor     | All data in every group where GitLab Orbit is on       |

For more information about the roles, see
[roles required to query GitLab Orbit](security.md#roles-required-to-query-gitlab-orbit).

### Administrator and auditor accounts

GitLab Orbit does not limit an account that can read all resources on the instance.
This applies to administrators and auditors.
Their queries return data from every group where GitLab Orbit is on, not only their own groups.

> [!warning]
> Do not give a query bot administrator or auditor access.
> A leaked token for such an account exposes the graph of every group where GitLab Orbit is on.

## Set up a service account for GitLab Orbit

Set up a service account to give an automated tool read access to the graph for specific groups.

Prerequisites:

- The Owner role for the top-level group.

To set up a service account:

1. [Create a group service account](https://docs.gitlab.com/user/profile/service_accounts/#create-a-service-account).
   Do not use a project service account.
   A project service account can only be a member of its own project, so it cannot get scope.
   A group service account can only join its own group and the subgroups of that group.
   To query more than one top-level group, create one service account for each group.
1. [Create a personal access token](https://docs.gitlab.com/user/profile/service_accounts/#create-a-personal-access-token-for-a-service-account)
   for the service account:
   - Select the `read_api` scope.
     GitLab Orbit is read-only, so the account does not need the `api` scope.
   - Set an expiration date.
   - Copy the token.
     GitLab shows it only once.
   - Store the token in a secret store, such as a [masked CI/CD variable](https://docs.gitlab.com/ci/variables/#mask-a-cicd-variable).
1. [Add the service account to each group](https://docs.gitlab.com/user/profile/service_accounts/#add-a-service-account-to-a-group-or-project)
   that the tool must query.
   Select the Reporter role.
   Select the Security Manager role only if the tool must read security data.

Use a standard (legacy) personal access token.
Fine-grained personal access tokens are not supported.

## Verify the scope of a service account

Verify the scope after you set up the account, and each time you change its memberships.
The query returns the groups that the account can read.

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

1. Compare the `full_path` values in the response with the groups that you added the account to.
   The response must not contain other groups.
   If the response contains groups from outside those memberships, check that the account is
   not an administrator or auditor.

The service account can also call the other [REST API endpoints](access/api.md).

## Limit the access of a service account

Give each service account the smallest scope that its task needs:

- Use one service account for each tool or integration.
  Then you can revoke one token without an effect on the other tools.
- Add the account to the lowest subgroup that contains the data, not to the top-level group.
- Use the Reporter role unless the tool must read security data.
- Use the `read_api` scope.
- [Rotate the token](https://docs.gitlab.com/user/profile/service_accounts/#rotate-a-personal-access-token)
  before it expires.
  A revoked or expired token stops all queries from the tool.
- Remove the account from a group when the tool no longer needs that group.

## Related topics

- [GitLab Orbit Remote security](security.md)
- [Personal access tokens](https://docs.gitlab.com/user/profile/personal_access_tokens/)

## Troubleshooting

When you query GitLab Orbit with a service account, you might encounter the following issues.

### Error: `403 Forbidden - No Orbit enabled namespaces available`

You might get this error when the service account has no group membership that gives it scope.

To resolve this issue:

- Add the account to a group in a top-level group where GitLab Orbit is on.
- Give the account the Reporter role or higher.

### Error: `403 Forbidden`

You might get a `403 Forbidden` response with no other message.
GitLab Orbit returns this response when none of the groups of the account has a license for
GitLab Orbit.

To resolve this issue:

- Add the account to a group in a top-level group with a Premium or Ultimate subscription where
  GitLab Orbit is on.

If you added the account to a group a short time ago, GitLab can keep the earlier result for a
few minutes.
Wait a few minutes, then send the query again.

### Error: `404 Not Found`

You might get a `404 Not Found` response from all GitLab Orbit endpoints.

This issue occurs when the `knowledge_graph` feature flag is not enabled for the service account.
GitLab checks the flag for the user that sends the request, so the flag can be on for you and off
for the service account.

To resolve this issue, contact your GitLab administrator to enable the `knowledge_graph` feature
flag for the service account.

### Results do not include security data

A service account with the Reporter role can query the graph, but GitLab Orbit removes security
entities from the results and from aggregate counts.

To resolve this issue:

- Give the account the Security Manager role in the group.
