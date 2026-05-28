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

<!-- What actually happens? Include exact errors shown in UI/CLI. -->

### Steps to reproduce

<!-- Provide a minimal, reliable reproduction. Be precise. Include information how
you started the gkg (IDE integration, standalone etc).
Attach screenshots or video if possible -->
1.
2.
3.

### Environment

- OS distribution and version:
- CPU architecture:
- Gkg version (`gkg -V`):

### Logs

<details><summary>Expand logs</summary>

<!-- Paste relevant logs. Prefer full stack traces and surrounding context. Sanitize secrets. -->

<!-- See [Viewing logs](https://gitlab-org.gitlab.io/rust/knowledge-graph/getting-started/troubleshooting/#viewing-logs) for how to view logs. -->

</details>

### Workaround

<!-- If you found a temporary workaround, describe it. -->

### Additional context

<!-- Hypotheses, related changes, or links that might help triage. Keep this
to a few lines — full analysis goes in the Agent context block below. -->

### Checklist

- [ ] Used latest available version of gkg
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
/label ~"devops::analytics" ~"section::analytics" ~"knowledge graph"
/label ~"type::bug"
