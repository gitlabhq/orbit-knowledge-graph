---
name: orbit-wrapper
description: Use the `glab orbit` CLI for questions about code structure, blast radius, cross-project links, and relationships across GitLab entities, and to build a repo map. It works on hosted or local data. Skip it for single-entity lookups or writes that `glab` already handles.
version: 0.1.0
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab) and network access to the GitLab instance for Orbit Remote commands.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-wrapper
---

# Orbit skill (wrapper)

This skill is a thin wrapper. The instructions live in the skill bundled with
the installed Orbit CLI, so they always match the installed version.

!`glab orbit skills get orbit`

If the block above is not a skill (it should begin with `---`), run
`glab orbit skills get orbit` and follow what it prints. For the standalone
Orbit binary, run `orbit skills get orbit`.

If that prints help or an unknown-command error instead of a skill, the
installed tooling is too old:

- `glab orbit` needs glab v1.117.0 or later. Upgrade glab if `glab orbit` is
  not recognized.
- `skills get orbit` serves the skill composed for your GitLab instance from
  Orbit CLI v0.131.0 or later. Orbit CLI v0.126.0 added `skills get`, but
  older releases only print the embedded local skill. Upgrade the Orbit CLI.

If the Orbit CLI is not installed, ask the user before installing it. Do not
pass `--yes` or set `GLAB_ORBIT_CLI_AUTO_DOWNLOAD` without their approval.
