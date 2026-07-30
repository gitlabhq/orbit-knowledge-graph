<!--
TEMPLATE CONVENTION — read before filling this out

Each section below is for the *important* content a triager needs: the
problem statement and the shape of the solution. Keep it short.

Long-form output — exhaustive design exploration, related-code dumps,
alternatives considered, agent reasoning — goes in the Agent context
block at the bottom, not in the sections above.

Agents: if you feel the urge to write a wall of text, write it inside the
Agent context block. The top sections stay terse.
-->

### Problem to Solve

<!--
Describe at a high level what the problem is and why it needs to be solved.

Headline first, supporting detail below. Diagrams and key links belong
here. Long narratives belong in the Agent context block.

Please keep this description updated with any discussion that takes place so
that reviewers can understand your intent. Keeping the description updated is
especially important if they didn't participate in the discussion.
-->

### Proposed Solution

<!--
Describe at a high level what the proposed solution is and why it is the
best solution. Sketch, diagram, or 3-5 bullets is ideal. Exhaustive
alternatives and full design exploration go in the Agent context block.
-->

<details>
<summary><b>Agent context</b> — extended analysis, alternatives considered, related-code dumps</summary>

<!--
Agents: put extended reasoning here. Full design exploration, alternatives
considered, related-code dumps, and anything else that would bury the
sections above belongs in this block.
-->

</details>

<!--
LABELS: every Orbit issue is classified on three axes, exactly one
orbit::<area>, one type::, and one priority::. The two standing labels are
applied below; add the three axis labels with quick actions under this
comment (quick actions inside a comment do not run) or in the UI.

Pick the area from the issue's substance, not its title keywords. When two
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

Type: type::bug for defects, type::feature for capability gaps (even when
filed as bugs), type::maintenance for refactors, docs, and upkeep. Docs work
also gets ~documentation.

Priority: priority::1 (urgent) through priority::4 (low).

Copy these out of the comment and edit:
/label ~"orbit::query"
/label ~"type::feature"
/label ~"priority::2"
-->

/label ~"group::context-systems" ~"Category:Orbit"

/assign me
