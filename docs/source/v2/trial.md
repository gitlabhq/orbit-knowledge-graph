---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: On GitLab.com, try GitLab Orbit for free in a top-level group. Learn what the trial includes, how to start it, and what happens when it ends.
title: Start a GitLab Orbit trial
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com

{{< /details >}}

<!-- TODO(GA): set the GA release and link the GA work item in the history line. -->
{{< history >}}

- [Introduced](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/19) in GitLab 19.5 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `orbit_trial`. Disabled by default.
- Generally available in GitLab 19.x.

{{< /history >}}

On GitLab.com, you can try GitLab Orbit for free in a top-level group that does not have the GitLab Orbit add-on.
On GitLab Self-Managed and GitLab Dedicated, contact your GitLab account team.

## What the trial includes

- GitLab Orbit for one top-level group, its subgroups, and projects, through every
  [access path](agents/_index.md), including GitLab Duo.
- A fixed amount of GitLab Credits for a limited number of days.
  The GitLab Orbit settings page of the group shows both before you start.

## Start a trial

Prerequisites:

- The Owner role for the top-level group.
- The group has a Premium or Ultimate subscription, and does not have the GitLab Orbit add-on.
- The group is eligible for a trial. If it is not, the dialog shows `This group is not eligible for a GitLab Orbit trial.`
  To buy the add-on, [contact sales](https://about.gitlab.com/sales/).

<!-- vale orbit.StandaloneProductName = NO -->

To start a trial:

1. In the top bar, select **Search or go to** and find your top-level group.
1. Select **Settings** > **Orbit**.
1. Turn on the **Turn on Orbit** toggle.
1. In the **Start a free Orbit trial?** dialog, select **Start free trial**.

The page shows **Orbit is indexing**, then **Orbit is ready**.

If none of your groups has GitLab Orbit turned on, you can also go to **Your work** > **GitLab Orbit**.
On the **Try Orbit for free** page, select your groups, select **Turn on Orbit**, then confirm.

<!-- vale orbit.StandaloneProductName = YES -->

To connect your agent, continue with the [quickstart](quickstart.md#step-2-install-the-cli-and-sign-in).

## When the trial ends

The trial ends when its days end or its GitLab Credits run out, whichever comes first.
GitLab Orbit then pauses for the group, and queries stop returning results.
To keep using GitLab Orbit, [contact sales](https://about.gitlab.com/sales/) to buy the add-on or more GitLab Credits.

GitLab keeps the indexed data for 30 days. If GitLab Orbit is on again in that time, indexing continues
from where it stopped. For more information, see [data retention and deletion](how-it-works.md#data-retention-and-deletion).

## Related topics

- [GitLab Orbit quickstart](quickstart.md)
- [Billing](how-it-works.md#billing)
- [Troubleshooting](troubleshooting.md)
