---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use the GitLab Orbit dashboard to check indexing, explore the schema, run queries, and manage which groups have GitLab Orbit turned on.
title: GitLab Orbit dashboard
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

In the GitLab Orbit dashboard, you check the state of your graph and run queries in the GitLab UI.

<!-- vale orbit.StandaloneProductName = NO -->

To open the dashboard, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.

If GitLab Orbit is not on for any of your groups, the dashboard lists your groups.
The heading is **Try Orbit for free** when a group is eligible for a free trial.
Otherwise, the heading is **Orbit is not turned on for your groups yet**.
To turn on GitLab Orbit, select the groups, then select **Turn on Orbit**.
On GitLab.com, you can also try GitLab Orbit with a free trial.
For more information, see [Start a free trial](trial.md).

The dashboard has four tabs.
**Leave feedback** is in the page header.
For administrators, the header also has **Settings**.

| Tab | Description |
|-----|-------------|
| **Overview** | Shows the indexing progress and the entity counts **In your graph**. The **Quickstart** cards **Use Orbit in GitLab Duo** and **Use Orbit with your own agents** include **Copy command** and a **Connect an MCP client** link. |
| **Schema** | Lists the entities by area: **Projects**, **Merge requests**, **Pipelines**, **Work items**, **Vulnerabilities**, **Packages**, **Container registry**, and **Code**. You can search the entities, and open **Entity details** to see the **Properties** and **Relationships** of one entity, with the count in your graph. |
| **Query editor** | Runs GQL queries. You can write a query, or describe what you want and select **Generate** to have GitLab Duo write the query. You can start from the **Examples** (aggregation, neighbors, shortest path, and filters), check **GQL syntax**, run the query, and select **Export CSV**. When a query fails, select **Ask Duo to fix this query**. |
| **Group status** | Shows the GitLab Orbit state of your top-level groups, where you turn GitLab Orbit on or off. To change a group, you need the Owner role for the group. **View billing** and **Contact sales** link to pages outside the dashboard. |

<!-- vale orbit.StandaloneProductName = YES -->

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [GitLab Orbit query language](queries/query-language.md)
- [Security](security.md)
