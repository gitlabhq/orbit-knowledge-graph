---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Start a free GitLab Orbit trial in a top-level group on GitLab.com, and find out what happens when the trial ends.
title: Start a GitLab Orbit trial
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/258545) in GitLab 19.5.

{{< /history >}}

On GitLab.com, you can start a free GitLab Orbit trial yourself in an eligible top-level group that does not already have GitLab Orbit.
The trial turns on GitLab Orbit for the group, its subgroups, and its projects, through every [access method](agents/_index.md).
On GitLab Self-Managed and GitLab Dedicated, contact your GitLab account team.

## Start a free trial

Start a free trial to try GitLab Orbit in a top-level group before you buy it.

Prerequisites:

- The Owner role for the top-level group.
- A top-level group that does not already have GitLab Orbit.
- A group that is eligible for a trial.
  If the group is not eligible, the **GitLab Orbit** settings page shows **Contact sales** in place of the toggle.
  To get access to GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).

To start a trial:

1. In the top bar, select **Search or go to** and find your top-level group.
1. In the left sidebar, select **Settings** > **GitLab Orbit**.
   The **GitLab Orbit trial** section shows the terms of the trial for the group.
1. Turn on the **Turn on GitLab Orbit** toggle.
1. In the **Turn on GitLab Orbit for `<my-group>`?** dialog, select **Turn on GitLab Orbit**.

The page shows **GitLab Orbit is indexing `<my-group>`**, then **GitLab Orbit is ready for `<my-group>`**.

If none of your groups has GitLab Orbit turned on, you can also start the trial from the [GitLab Orbit dashboard](dashboard.md).

To connect your agent, continue with the [quickstart](quickstart.md#step-2-set-up-your-agent-to-use-gitlab-orbit).

## When the trial ends

At the end of the trial, GitLab turns off GitLab Orbit for the group, and queries fail.
To continue with GitLab Orbit, contact sales.
For information about GitLab Orbit pricing and usage, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

GitLab keeps the indexed data for a limited time.
For more information, see [data retention and deletion](how-it-works.md#data-retention-and-deletion).

## Related topics

- [Troubleshooting](troubleshooting.md)
