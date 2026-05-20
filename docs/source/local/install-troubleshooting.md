---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Pin a version, force a reinstall, or delete the local Orbit graph.
title: Advanced installation and troubleshooting
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

For the standard install, see [Get started with Orbit Local](getting-started.md).

## Install a specific version

Pin to a published release tag like `v0.59.1`.

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --version v0.59.1
```

{{< /tab >}}

{{< tab title="Windows" >}}

The piped one-liner cannot forward arguments. Download the script first, then
run it:

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 -OutFile install.ps1
.\install.ps1 -Version v0.59.1
```

{{< /tab >}}

{{< /tabs >}}

## Force a reinstall

The installer skips re-downloading if `orbit` already exists. Use the force
flag to overwrite the binary.

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --force
```

Combine flags to reinstall a specific version:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --version v0.59.1 --force
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 -OutFile install.ps1
.\install.ps1 -Force
```

Combine flags to reinstall a specific version:

```powershell
.\install.ps1 -Version v0.59.1 -Force
```

{{< /tab >}}

{{< /tabs >}}

## Delete the local graph

Orbit Local stores indexed data in a single DuckDB file. Remove it to start
over; the next `orbit index` run rebuilds it from scratch.

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
rm ~/.orbit/graph.duckdb
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
Remove-Item "$env:USERPROFILE\.orbit\graph.duckdb"
```

{{< /tab >}}

{{< /tabs >}}

## Uninstall the binary

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
rm ~/.local/bin/orbit
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
Remove-Item "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
```

Optionally, remove the install directory from your user `PATH` via
**System Properties > Environment Variables**.

{{< /tab >}}

{{< /tabs >}}
