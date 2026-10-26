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

{{< history >}}

- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.

{{< /history >}}

In the GitLab Orbit dashboard, you check the state of your graph and run queries in the GitLab UI.

To open the dashboard, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.

Administrators also see **Settings** in the page header. It opens the instance settings for GitLab Orbit,
such as automatic indexing. For more information, see [administer GitLab Orbit](https://docs.gitlab.com/integration/orbit/).

## Check indexing progress

Check which projects GitLab Orbit is still indexing.

To check indexing progress:

1. In the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
1. Select the **Overview** tab.

The tab shows the entity counts **On your map** and the projects that are still indexing.

## Explore the schema

Find out which entities your graph holds and how they connect.

To explore the schema:

1. In the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
1. Select the **Schema** tab.
1. Under **How your data connects**, select an entity.

**Entity details** shows the properties of the entity, its connected entities, and its count in your graph.

## Run a query

Run a query on your graph and export the results.

To run a query:

1. In the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
1. Select the **Query editor** tab.
1. Write a query, or select **Write it with GitLab Duo**.
   To start from an example, select **Examples**.
1. Select **Run query**.
1. Optional. To export the results to a CSV file, select **Export CSV**.

The tab shows the results of the query.
If the query fails, select **Ask GitLab Duo to fix this query**.

## Turn GitLab Orbit on or off for a group

Turn GitLab Orbit on or off for one of your top-level groups.

Prerequisites:

- The Owner role for the top-level group.

If GitLab Orbit is not turned on for any of your groups, the dashboard has no tabs and lists the top-level groups that you own.
To turn on GitLab Orbit, select the groups, select **Turn on GitLab Orbit**, then in the confirmation dialog, select **Turn on GitLab Orbit**.
On GitLab.com, you can also try GitLab Orbit with a [free trial](trial.md).

Otherwise, to turn GitLab Orbit on or off:

1. In the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
1. Select the **Status** tab.
1. Select the checkbox next to your top-level group.
1. Select **Actions** > **Turn on GitLab Orbit** or **Actions** > **Turn off GitLab Orbit**.
1. In the confirmation dialog, select **Turn on GitLab Orbit** or **Turn off GitLab Orbit**.

The tab shows the new GitLab Orbit status of the group.
When you turn off GitLab Orbit, GitLab retains the indexed data of the group for 30 days, then deletes it.
For more information, see [data retention and deletion](how-it-works.md#data-retention-and-deletion).

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [GitLab Orbit query language](queries/query-language.md)
- [Security](security.md)
