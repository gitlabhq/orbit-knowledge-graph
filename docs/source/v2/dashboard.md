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

The GitLab Orbit dashboard shows the state of your graph and lets you query it in the GitLab UI.

<!-- vale orbit.StandaloneProductName = NO -->

To open it, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.

If GitLab Orbit is not on for any of your groups, the dashboard lists your groups.
The heading is **Try Orbit for free** when a group is eligible for a free trial,
or **Orbit is not turned on for your groups yet**.
To turn it on, select the groups, then select **Turn on Orbit**.

The dashboard has four tabs. **Leave feedback** is in the page header.
For administrators, the header also has **Settings**.

| Tab | Use it to |
|-----|-----------|
| **Overview** | See the indexing progress and the entity counts **In your graph**, and follow the **Quickstart** cards **Use Orbit in GitLab Duo** and **Use Orbit with your own agents**, which include a **Copy command** button and a **Connect an MCP client** link. |
| **Schema** | Explore the entities by area (**Projects**, **Merge requests**, **Pipelines**, **Work items**, **Vulnerabilities**, **Packages**, **Container registry**, **Code**), search them, and open **Entity details** to see the **Properties** and **Relationships** of one entity, with the count in your graph. |
| **Query editor** | Write a GQL query, or describe what you want and let GitLab Duo write the query (**Generate**). Start from the **Examples** (aggregation, neighbors, shortest path, filters), check **GQL syntax**, run the query, and **Export CSV**. When a query fails, select **Ask Duo to fix this query**. |
| **Group status** | See the GitLab Orbit state of your top-level groups and turn GitLab Orbit on or off. You need the Owner role in a group to change it. **View billing** and **Contact sales** link out. |

<!-- vale orbit.StandaloneProductName = YES -->

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Start a GitLab Orbit trial](trial.md)
- [GitLab Orbit query language](queries/query-language.md)
- [Security](security.md)
