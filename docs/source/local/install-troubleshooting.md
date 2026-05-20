---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Pin a version, force a reinstall, change the install directory, or delete the local Orbit graph.
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

### macOS and Linux

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --version v0.59.1
```

### Windows

The piped one-liner cannot forward arguments. Download the script first, then
run it:

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 -OutFile install.ps1
.\install.ps1 -Version v0.59.1
```

## Force a reinstall

The installer skips re-downloading if `orbit` already exists. Use the force
flag to overwrite the binary.

### macOS and Linux

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --force
```

### Windows

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 -OutFile install.ps1
.\install.ps1 -Force
```

Combine flags to reinstall a specific version:

```shell
# macOS and Linux
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --version v0.59.1 --force
```

```powershell
# Windows
.\install.ps1 -Version v0.59.1 -Force
```

## Install to a custom directory

The default install directories are `~/.local/bin` on macOS and Linux, and
`%LOCALAPPDATA%\Programs\orbit` on Windows.

### Windows

```powershell
.\install.ps1 -InstallDir "C:\Tools\orbit"
```

The macOS and Linux installer does not currently accept a custom directory.
Move the binary manually after installation if needed.

## Delete the local graph

Orbit Local stores indexed data in a single DuckDB file. Remove it to start
over; the next `orbit index` run rebuilds it from scratch.

### macOS and Linux

```shell
rm ~/.orbit/graph.duckdb
```

### Windows

```powershell
Remove-Item "$env:USERPROFILE\.orbit\graph.duckdb"
```

## Uninstall the binary

### macOS and Linux

```shell
rm ~/.local/bin/orbit
```

### Windows

```powershell
Remove-Item "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
```
