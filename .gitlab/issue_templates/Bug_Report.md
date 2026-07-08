<!--
TEMPLATE CONVENTION — read before filling this out

Each section below is for the *important* content a triager needs: the
one-line summary, the precise repro, the exact error. Keep it short.

Long-form output — full logs, file dumps, exhaustive hypothesis lists,
agent reasoning — goes in the Logs <details> block or the Agent context
block at the bottom, not in the sections above.

Agents: if you feel the urge to write a wall of text, write it inside the
Agent context block. The top sections stay terse.
-->

### Summary

<!-- One-sentence description of the bug. What is broken? You can also title
the issue accordingly. -->

### Expected behavior

<!-- What should happen? -->

### Actual behavior

<!-- What actually happens? Include the exact error shown in the UI/CLI. -->

### Steps to reproduce

<!-- A minimal, reliable reproduction. Be precise: the exact command or
action, and how you invoked Orbit (see Environment below). Attach a
screenshot or short video if it helps. -->
1.
2.
3.

### Environment

Which Orbit are you using?

- [ ] **Orbit Remote** — the hosted graph on GitLab.com (`glab orbit remote`, MCP, GitLab Duo, or the REST API)
- [ ] **Orbit Local** — runs on your machine (the `orbit` binary or `glab orbit local`)

<!-- Fill in the block for the surface you selected; delete the other. -->

**Orbit Remote**

- GitLab.com group or project path (e.g. `gitlab-org/gitlab`):
- Access method: <!-- glab orbit remote / MCP client / GitLab Duo / REST API -->
- Approximate time of the failure (include timezone, or use UTC):

**Orbit Local**

- Orbit version (`orbit --version`, or `glab orbit local --version`):
- Access method: <!-- orbit CLI / glab orbit local / MCP client -->
- OS and version:
- CPU architecture (`uname -m`):

### Logs

<details><summary>Expand logs</summary>

<!--
Sanitize secrets (tokens, private file paths) before pasting. Prefer full
error output over a trimmed snippet.

Orbit Local: re-run the failing command with `--verbose` and paste the
stderr output (e.g. `glab orbit local <command> --verbose`).

Orbit Remote: paste the exact command and its full output. Include
`glab orbit remote status` and note the exit code — 2 (feature flag off),
3 (not authenticated), 4 (access denied), or 5 (rate limited).

Common errors and fixes: https://docs.gitlab.com/orbit/troubleshooting/
-->

</details>

### Workaround

<!-- If you found a temporary workaround, describe it. -->

### Additional context

<!-- Hypotheses, related changes, or links that might help triage. Keep this
to a few lines — full analysis goes in the Agent context block below. -->

### Checklist

- [ ] Reproduced on the latest available version
- [ ] Searched existing issues for duplicates
- [ ] Included minimal, reliable reproduction steps
- [ ] Attached relevant logs and environment details
- [ ] Removed sensitive information from logs/configs

<details>
<summary><b>Agent context</b> — extended analysis, hypothesis chains, file walkthroughs</summary>

<!--
Agents: put extended reasoning here. Full hypothesis chains, file-by-file
walkthroughs, related-code dumps, and anything else that would bury the
sections above belongs in this block.
-->

</details>

<!-- Don't edit below -->
/label ~"group::context-systems" ~"Category:Orbit" ~"knowledge graph"
/label ~"type::bug"
