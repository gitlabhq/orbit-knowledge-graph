---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: On GitLab.com, try GitLab Orbit for free in a top-level group. Learn how to start the trial and what happens when it ends.
title: Start a GitLab Orbit trial
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/258545) in GitLab 19.5 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `orbit_trial`. Disabled by default.
- Generally available in GitLab 19.5.

{{< /history >}}

On GitLab.com, you can start a free GitLab Orbit trial yourself in an eligible top-level group that does not already have GitLab Orbit.
The trial turns on GitLab Orbit for the group, its subgroups, and projects, through every [access method](agents/_index.md).
On GitLab Self-Managed and GitLab Dedicated, contact your GitLab account team.

## Start a trial

<!-- vale orbit.StandaloneProductName = NO -->

Prerequisites:

- GitLab.com.
- The Owner role for the top-level group.
- The group has a Premium or Ultimate subscription, and does not already have GitLab Orbit.
- The group is eligible for a trial. If it is not, the **Orbit** settings page says that the group is not eligible and shows **Contact sales**.
  To get access to GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).

To start a trial:

1. In the top bar, select **Search or go to** and find your top-level group.
1. In the left sidebar, select **Settings** > **Orbit**.
1. Turn on the **Turn on Orbit** toggle. The page shows the terms of the trial for the group.
1. In the **Start a free Orbit trial?** dialog, select **Start free trial**.

The page shows **Orbit is indexing `<my-group>`**, then **Orbit is ready for `<my-group>`**.

If none of your groups has GitLab Orbit turned on, you can also start the trial from **Your work**:

1. In the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
1. On the **Try Orbit for free** page, select your groups.
1. Select **Turn on Orbit**, then confirm.

<!-- vale orbit.StandaloneProductName = YES -->

To connect your agent, continue with the [quickstart](quickstart.md#step-2-sign-in-and-set-up-your-agent).

## When the trial ends

When the trial ends, GitLab Orbit pauses for the group, and queries fail.
To keep using GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).
For information about GitLab Orbit pricing and usage, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

GitLab keeps the indexed data for a limited time. For more information, see [data retention and deletion](how-it-works.md#data-retention-and-deletion).

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Troubleshooting](troubleshooting.md)
