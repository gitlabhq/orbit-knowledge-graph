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

<!--
LABELS: every Orbit issue is classified on three axes, exactly one
orbit::<area>, one type::, and one priority::. type::bug and the standing
labels are applied below; add the area and priority with quick actions under
this comment (quick actions inside a comment do not run) or in the UI. If
this turns out to be a missing capability rather than a defect, swap
type::bug for type::feature.

Pick the area from the bug's substance, not its title keywords. When two
areas fit, choose the one where the fix will land.

Areas:
  orbit::query               Query engine, DSL, compiler, pagination, agent query ergonomics
  orbit::graph-completeness  Ontology and data gaps
  orbit::indexing            Code + SDLC indexing pipeline correctness
  orbit::reliability         Reliability and scalability, 5xx defects, incidents
  orbit::security            Security-related Orbit work
  orbit::dx                  CI/e2e, tooling, contributor flow
  orbit::ux                  Product UX surfaces
  orbit::dap-integration     DAP/DWS integration
  orbit::monetization        Monetization engineering and pricing
  orbit::analytics           Telemetry and usage dashboards
  orbit::integrations        External integrations (Jira etc.)
  orbit::code-graph          Code-graph features (code intelligence etc.)
  orbit::local               Local knowledge graph: DuckDB indexer, glab orbit local
  orbit::infra               Infrastructure, delivery, and production readiness

Priority: priority::1 (urgent) through priority::4 (low).

Copy these out of the comment and edit:
/label ~"orbit::query"
/label ~"priority::2"
-->

/label ~"group::context-systems" ~"Category:Orbit"
/label ~"type::bug"
