---
name: orbit-planning
description: >-
  Classify and fix any Orbit/GKG work item, epic, or MR with the canonical
  label taxonomy (orbit:: area, type::, priority::), manage portal roadmap
  membership (orbit-roadmap::), set due dates and health, and keep epics
  conforming to the planning doctrine. Use when labeling new or old issues or
  MRs, adding or marking deliverables on the roadmap, creating or repurposing
  epics, triaging the backlog, or any "add the appropriate labels" request.
version: 1.0.0
---

# Orbit planning: labels, epics, roadmap

Everything the portal renders is API truth. Curation happens by putting the
right labels, dates, and health on GitLab items, never by editing YAML.
The portal repo's [label-taxonomy-and-epic-cleanup.md](https://gitlab.com/gitlab-org/orbit/portal/-/blob/main/docs/label-taxonomy-and-epic-cleanup.md) tracks the scheme and its history.

## Fix any item: the core loop

Most requests reduce to "make this item conform". For any issue, task, epic,
or MR:

1. Read the item. Pick the area from its substance, not its title keywords.
   When two areas fit, choose where the fix will land.
2. Check it against the checklist for its kind (below).
3. Apply changes with `workItemUpdate` and label ids (MRs use
   `mergeRequestSetLabels`). Snippets are in Mechanics at the bottom.

Checklist by kind:

| Kind | Must have |
|---|---|
| Open issue/task | one `orbit::<area>`, one `type::`, one `priority::1-4`, `Category:Orbit`, `group::context-systems` |
| Closed issue/task | one `orbit::<area>`, one `type::`, `workflow::complete`, due date = close date (fixed). No retroactive `priority::` |
| Epic | area label (unless a multi-area container), `type::`, `Category:Orbit`, `group::context-systems`; a due date if it is a deliverable |
| MR | `Category:Orbit`, `group::context-systems`, one `type::`, area label where obvious; docs MRs also get `documentation` |

Flat flags combine freely on top: `customer-zero` and `documentation` (both
gitlab-org labels). Two more exist but only in narrower scopes, so they work
only there: `beta-blocker` is a knowledge-graph project label and
`design partner` lives at the orbit group. Both are drift; promote or retire
before relying on them elsewhere.

Type rules of thumb: docs work is `type::maintenance` + `documentation`
(KG precedent: #493, #604694). Coverage and discussion-log items are
`type::ignore`. Perf defects are `type::bug`; perf refactors are
`type::maintenance`. Capability gaps are `type::feature` even when filed as
bugs.

Legacy labels map as: flat `PREP` becomes `orbit::infra`, `devex` becomes
`orbit::dx`, flat `P1`/`P2` become `priority::1`/`priority::2`, untyped
`bug`/`feature`/`maintenance` become the `type::` equivalent. Strip `GKG::*`
status labels on sight. See Retired and legacy labels before recreating
anything.

Leave labels outside this taxonomy alone: GitLab's automatic classification
labels (`section::`, `devops::`, `automation:`, `backend`, `frontend`) and
other teams' scoped labels (`bug::availability`, R&D priority variants) stay
on the item. Only strip the legacy labels named above.

## The taxonomy

Every issue gets exactly three classifications:

| Axis | Labels | Rule |
|---|---|---|
| Area | `orbit::<area>` | exactly one (scoped: one label per scope) |
| Type | `type::bug` / `type::feature` / `type::maintenance` | exactly one |
| Priority | `priority::1-4` | one; portal renders Urgent/High/Medium/Low |

GitLab treats everything before the last `::` as the scope and allows one
label per scope, so adding an area silently evicts any other bare `orbit::`
label. That eviction is often the desired cleanup; check what fell off.

## Where labels live (critical)

All orbit labels are defined at the `gitlab-org` group. Never create project
labels and never create labels at `gitlab-org/orbit`. gitlab-org labels reach
every descendant (knowledge-graph, gkg-service, the monolith); orbit-group
labels cannot be applied to `gitlab-org/gitlab` issues, where real Orbit work
also lives. A project-level duplicate of a group label is a bug. Fix it by
swapping carriers to the group label and deleting the project copy. The one
sanctioned duplicate set: the `gitlab-com` group mirrors the taxonomy
(`group::context-systems`, `Category:Orbit`, the 14 areas) so items in
gitlab-com projects (runbooks, handbook, www) can classify.

Every `orbit::` area must be configured in the portal repo's [config/labels.yaml](https://gitlab.com/gitlab-org/orbit/portal/-/blob/main/config/labels.yaml) `areas:`
(exact label string), and every configured area must exist as a gitlab-org
label. The two lists mirror each other; the portal renders swimlanes only
for configured areas. After editing the config run
`bun run --filter @orbit/cli seed` in the portal repo. Label changes on GitLab alone need no
seed rebuild. The app syncs live.

## Roadmap membership

An item is on the portal roadmap iff it carries one `orbit-roadmap::<confidence>`
label. Everything else derives:

- Status: closed means shipped, always. Open: `workflow::in dev|in review|verification|complete`
  render as building; `workflow::planning breakdown|ready for development` as
  scoping; validation/design values as discovery; none as planned.
- Quarter: from the due date. GitLab fiscal year runs Feb through Jan and is
  named for the year it ends: FY27 Q1 = Feb-Apr 2026, Q2 = May-Jul,
  Q3 = Aug-Oct, Q4 = Nov 2026-Jan 2027. No due date lands in "Unscheduled".
- Risk: the native health-status widget (On track / Needs attention / At risk).
- Confidence: committed means leadership can quote the date; likely means a
  target quarter with a known dependency that could move it; exploratory means
  real intent, no commitment.

One row per deliverable. When an epic is the roll-up, the `orbit-roadmap::`
label goes on the epic only. Remove it from child issues or the roadmap shows
duplicate rows. Children appear automatically as the epic's work list.

Shipped history: a closed deliverable epic stays on the roadmap with
`orbit-roadmap::committed` + `workflow::complete` and renders as shipped in
the quarter of its due date. If it never had a due date, set one fixed to its
close date. Workstream containers and planning epics stay off the roadmap.

Hygiene (the portal nudges on unshipped items): every roadmap item needs a
due date, a `workflow::` label, and a health status.

Closed-item dates (Angelo, 2026-07-10): every closed/completed item touched
during curation gets its due date backfilled to its close date (fixed),
roadmap member or not. Completion dates are the historical record. Watch for
bulk-close dates that lie; when children were closed in a bookkeeping sweep,
date the epic to when the capability actually landed (last substantive MR or
child).

## Epic doctrine

Epics are time-based deliverables: a clear outcome and a fixed due date.
Long-standing themes are `orbit::` area labels, not epics. An issue can live
under an epic (when/what ships) and carry an area label (what kind of work).

Two different close-outs, do not mix them. The test: an epic that maps to
exactly one area is a bucket; one that spans areas or wraps a whole
workstream is a container.

- Bucket epic converted to a label: label all children with the area, close
  the epic with a pointer comment, then unparent the children. The label is
  the sole carrier afterward.
- Completed deliverable or workstream epic: close with `workflow::complete`
  and a fixed due date, and keep the children. They are the record of the
  work. Never unparent these. Containers and planning epics also stay off
  the roadmap.

Multi-area containers (like a whole workstream) get `type::` and workflow but
deliberately no `orbit::` area; forcing one would be noise.

## Mechanics

Resolve any label id you don't have. `search` is a substring match across all
group labels, so search for a specific name, not a bare scope (searching
`priority::` returns 80+ other teams' priority variants and pages the real
ones out):

```bash
glab api "groups/gitlab-org/labels?search=orbit%3A%3Aquery&only_group_labels=true&include_ancestor_groups=false&per_page=50"
```

Resolve the work item gid first. The mutation needs a `WorkItem` gid; an
`Issue` gid with the same number will be rejected, so query workItems:

```bash
glab api graphql -f query='query { project(fullPath: "gitlab-org/orbit/knowledge-graph") {
  workItems(iids: ["971"]) { nodes { id } } } }'
# epics, from the group that owns them (the & reference names it):
# group(fullPath: "gitlab-org") { workItem(iid: "20992") { id } }
```

Label and date updates go through `workItemUpdate` with label ids, never
names (name-based REST is ambiguous whenever two same-named labels are
visible):

```bash
glab api graphql -f query='mutation {
  workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/185491112",
    labelsWidget: {addLabelIds: ["gid://gitlab/GroupLabel/52162908"],
                   removeLabelIds: ["gid://gitlab/GroupLabel/40929324"]},
    startAndDueDateWidget: {dueDate: "2026-10-15", isFixed: true}
  }) { errors }
}'
```

MRs use `mergeRequestSetLabels` (one APPEND call, one REMOVE call):

```bash
glab api graphql -f query='mutation {
  mergeRequestSetLabels(input: {projectPath: "gitlab-org/orbit/knowledge-graph",
    iid: "2033", labelIds: ["gid://gitlab/GroupLabel/52162078"],
    operationMode: APPEND}) { errors }
}'
```

Sweep candidates by label with the group workItems connection (`labelName`
accepts wildcards, `includeDescendants: true` reaches subgroup projects):

```bash
glab api graphql -f query='query { group(fullPath: "gitlab-org") {
  workItems(labelName: ["orbit-roadmap::*"], includeDescendants: true, first: 100) {
    nodes { iid title state webUrl } } } }'
```

New areas are minted at gitlab-org with the description from the portal's `config/labels.yaml`:

```bash
glab api -X POST "groups/gitlab-org/labels" \
  -f name="orbit::query" -f color="#1f75cb" \
  -f description="Query engine, DSL, compiler, pagination, agent query ergonomics"
```

Label ids (gitlab-org group; all 14 areas exist):

- Roadmap: `orbit-roadmap::committed` 52162908, `::likely` 52162910, `::exploratory` 52162911
- Areas: `orbit::security` 52162078, `orbit::code-graph` 52177075,
  `orbit::monetization` 52177262, `orbit::analytics` 52177482,
  `orbit::local` 52177716, `orbit::infra` 52177717, `orbit::reliability` 52177930,
  `orbit::query` 52179830, `orbit::graph-completeness` 52180114,
  `orbit::indexing` 52180352, `orbit::dx` 52180353, `orbit::ux` 52180354,
  `orbit::dap-integration` 52180355, `orbit::integrations` 52180356
- Priority: `priority::1` 3857370, `priority::2` 3857523, `priority::3` 3857529,
  `priority::4` 3857543
- Flat flags: `customer-zero` 52179831
- `Category:Orbit` 52177066; `type::bug` 2278648, `type::feature` 10230929,
  `type::maintenance` 15119514, `type::ignore` 26375975, `documentation` 2278655,
  `group::context-systems` 49441857, `workflow::complete` 28669354

Gotchas learned the hard way:

- Label promotion (`PUT /projects/:id/labels/:label_id/promote`) merges
  same-named project labels only in the group's direct child projects; copies
  in subgroup projects survive and need a manual carrier swap (enumerate by
  label gid, swap, delete the emptied label).
- Epic due dates can be inherited (they roll up to the max child due). Pin
  with `isFixed: true` when backfilling children.
- GraphQL `count` caps at 1000; paginate with `pageInfo` for real totals.
- Never filter out confidential items; the portal badges them instead.
- Batch mutations with aliases (~10 per request) to stay under the complexity
  limit of 250.
- The gitlab-com group mirrors the taxonomy (2026-07-12): `group::context-systems`
  (46682047, renamed in place from the empty space variant), `Category:Orbit`
  (52206994), all 14 `orbit::` areas, and the three `orbit-roadmap::` labels
  (Angelo's call: cross-top-level-group duplicates are fine). The old runbooks
  project label (52110393) is deleted; its ten MRs carry the group label. Keep
  colors/descriptions in sync with gitlab-org when a label changes. The portal
  sweeps both top-level groups: roadmap (roadmap.yaml groups), areas
  (labels.yaml gitlab-com track entry — its names list must mirror `areas:` by
  hand), and triage (labels.yaml triage.scopes). Adding a new area means three
  config touches: `areas:`, the gitlab-com track entry, and the label itself
  at both groups.

## Sweeping the backlog (parallelizable)

For a bulk pass over old issues, work project by project so two people never
touch the same item:

1. Enumerate open issues missing an axis:
   `glab api "projects/gitlab-org%2Forbit%2Fknowledge-graph/issues?state=opened&per_page=100&page=N"`
   and check each for one `orbit::` area, one `type::`, one `priority::`.
2. Classify per the core loop above. Nothing stays orphaned (Meg's rule:
   every issue classifies with exactly one area).
3. Apply via `workItemUpdate` with label ids, a batch of aliased mutations
   per ~10 items.

New issues get all three axes at creation time. New deliverables additionally
get an epic (or join one), a fixed due date, and an `orbit-roadmap::` label at
the epic level.

## Retired and legacy labels

Retired, deleted, do not recreate (audited gone 2026-07-11):

- `orbit::customer-zero`, `orbit::GA`, `orbit::backlog`: deleted at gitlab-org
  2026-07-10 after `Category:Orbit` was backfilled onto every carrier that
  lacked it (761 work items + 799 MRs, of 1,918 enumerated). The bare
  `orbit::` scope belongs to areas; genuine Customer Zero reports get flat
  `customer-zero`.
- Ad-hoc `knowledge graph` / `knowledge-graph` / `knowledge_graph` labels and
  the `group::knowledge graph` project shadows: deleted 2026-07-08 after 524
  carriers got `Category:Orbit`. `group::knowledge-graph` itself was renamed
  in place to `group::context-systems`, keeping its associations.
- Untyped project-level `bug`/`feature`/`maintenance`: verified nonexistent.

Legacy, still live, never apply to new items:

- `knowledge graph` (gitlab-org/orbit group, 52177082, ~221 carriers): the one
  deliberate survivor, mirroring the www categories.yml feature-category slug.
  Keep-or-retire is an open decision.
- `orbit::hackathon` (gitlab-org, 50319029, 47 closed carriers) and
  `orbit_hackathon::L0/L1/L2` (orbit group): hackathon provenance, history only.
- `Big Rock::Orbit` (gitlab-org, 52180349): exists with zero carriers; unused,
  fate undecided.
- gitlab-org/orbit group drift (labels living one level too low): `GKG::*`
  status labels (11 closed-only carriers), `ai_review::*`, `design partner`.
  Pending retirement once boards key off the new axes.
- knowledge-graph project: flat `PREP` (4 carriers, folds into `orbit::infra`),
  `devex` (empty, deletable), `group::context systems` space-variant dup
  (52185456, empty, deletable; canonical is hyphenated 49441857 at gitlab-org).

## Conventions

Open every comment posted to a GitLab issue/MR/epic with `> from agent` on
its own line. Before bulk changes (more than ~20 items), state the plan and
counts first. Record label and epic decisions in the portal repo's
`docs/label-taxonomy-and-epic-cleanup.md`. Rename GKG or Knowledge Graph to
Orbit in any title you touch, keeping literal code identifiers
(gkg-webserver, GKG_INDEXER, backticked flag names) intact.
