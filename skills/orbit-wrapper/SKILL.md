---
name: orbit-wrapper
description: Use Orbit (`glab orbit` or `orbit`) to search code and read it with callers and tests. It reads MRs, issues, and failed jobs with the code they cite. Graph queries answer relationship and aggregation questions across GitLab. Skip it for writes that `glab` handles.
version: 0.2.2
license: MIT
compatibility: Requires glab v1.126.0 or later with the GitLab Orbit CLI v0.131.0 or later, and network access to the GitLab instance for commands that call the GitLab Orbit API.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-wrapper
---

# Orbit skill (wrapper)

This skill is a thin wrapper. It loads the Orbit skill that your GitLab
instance serves, composed with sections from your installed GitLab Orbit CLI, so it
always matches your setup.

!`glab orbit skills get orbit`

If the block above is not a skill (it should begin with `---`), run
`glab orbit skills get orbit` and follow what it prints. For the standalone
Orbit binary, run `orbit skills get orbit`.

Links such as `references/recipes.md` in the loaded skill are not files next
to this wrapper. Print them with `glab orbit skills get orbit <path>`.

If that prints help or an unknown-command error instead of a skill, the
installed tooling is too old:

- `glab orbit` and `glab orbit skills` need glab v1.126.0 or later. Upgrade
  glab if either is missing.
- GitLab Orbit CLI v0.126.0 to v0.130.x print only the local skill
  (`name: orbit-cli`). Upgrade to v0.131.0 or later to get the skill your
  instance serves.

If the GitLab Orbit CLI is not installed, ask the user before installing it. Do not
pass `--yes` or set `GLAB_ORBIT_CLI_AUTO_DOWNLOAD` without their approval.
