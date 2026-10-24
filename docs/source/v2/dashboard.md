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

To open the dashboard, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.

If GitLab Orbit is not turned on for any of your groups, the dashboard lists your groups.
To turn on GitLab Orbit, you need the Owner role for the top-level group.
Select the groups, then select **Turn on GitLab Orbit**.
On GitLab.com, you can also try GitLab Orbit with a [free trial](trial.md).

Administrators also see **Settings** in the page header.

The dashboard has these tabs:

| Tab | Description |
|-----|-------------|
| **Overview** | Shows a map of your graph, the entity counts **On your map**, and the projects that are still indexing. |
| **Schema** | Shows **How your data connects**, with the entities by area: **Core**, **Work items**, **Merge requests**, **Pipelines**, **Code**, **Vulnerabilities**, **Packages**, **Container registry**, and **AI agents**. Select an entity to open **Entity details**, with its properties, its connected entities, and the count in your graph. |
| **Query editor** | Runs GQL queries. Write a query, or select **Write it with GitLab Duo**. You can start from the **Examples**, open the **Query reference** for the schema and the **GQL syntax**, select **Run query**, and select **Export CSV**. When a query fails, select **Ask GitLab Duo to fix this query**. |
| **Status** | Shows the GitLab Orbit status of your top-level groups, where you turn GitLab Orbit on or off. To change a group, you need the Owner role for the group. **View billing** links to a page outside the dashboard. |

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [GitLab Orbit query language](queries/query-language.md)
- [Security](security.md)
