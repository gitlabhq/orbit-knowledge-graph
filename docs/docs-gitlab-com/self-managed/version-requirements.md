---
stage: Analytics
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: The GitLab, Helm chart, and ClickHouse versions that each GitLab Orbit release supports.
title: Version requirements for GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab Self-Managed

{{< /details >}}

Each GitLab Orbit release works with a range of GitLab, Helm chart, and ClickHouse versions.
Before you install or upgrade GitLab Orbit or GitLab, find your GitLab Orbit version in each table.

The tables show the newest release first.
A GitLab Orbit version such as 1.0.0 includes its later patch releases, unless a newer row names a patch release.
A range that ends with **and later** has no known upper limit.

## GitLab

<!-- Add a row at the top when a GitLab Orbit minor release changes the supported GitLab versions. -->

| GitLab Orbit version | GitLab version |
|----------------------|----------------|
| 1.0.0 | 19.4.4, 19.5.2, or 19.6.0 and later |

When you upgrade GitLab, confirm that your GitLab Orbit version supports the new GitLab version.
If it does not, upgrade GitLab Orbit first.

## Helm chart

<!-- Add a row at the top when a chart release changes the supported GitLab Orbit versions. -->

| GitLab Orbit version | Helm chart version |
|----------------------|--------------------|
| 1.0.0 | 7.0.0 and later |

Each chart release installs a default GitLab Orbit version.
To install a different supported version, set `image.tag` in `orbit-values.yaml`.
The chart stops the installation when the GitLab Orbit version is older than the chart supports.

For all chart releases, see [GitLab Orbit Helm chart releases](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts/-/releases).

## ClickHouse

<!-- Add a row at the top when a GitLab Orbit minor release changes the supported ClickHouse versions. -->

| GitLab Orbit version | ClickHouse version |
|----------------------|--------------------|
| 1.0.0 | 26.6 and later |

GitLab also writes to the data lake, so the data lake must use a ClickHouse version that GitLab supports too.
For the versions that GitLab supports, see [Supported ClickHouse versions](https://docs.gitlab.com/integration/clickhouse/#supported-clickhouse-versions).

## Related topics

- [Get started with GitLab Orbit on GitLab Self-Managed](getting-started.md)
- [ClickHouse for GitLab Orbit](clickhouse.md)
- [Upgrade GitLab Orbit](orbit-setup.md#upgrade-gitlab-orbit)
