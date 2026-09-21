---
name: orbit-planning
description: >-
  Label Orbit work items, epics, and MRs with the canonical taxonomy (orbit::
  area, type::, priority::). Manage roadmap membership (orbit-roadmap::), due
  dates, health, and epic hygiene. Use when you label or triage issues or MRs,
  add or mark roadmap deliverables, or draft or repurpose epics.
version: 2.0.0
allowed-tools: Read, Bash(glab *)
---

# Orbit planning

The portal reads labels, dates, and health from GitLab. Curate on GitLab, not in YAML.
The scheme and its decisions live in the portal repo:
[label-taxonomy-and-epic-cleanup.md](https://gitlab.com/gitlab-org/orbit/portal/-/blob/main/docs/label-taxonomy-and-epic-cleanup.md)
and [config/labels.yaml](https://gitlab.com/gitlab-org/orbit/portal/-/blob/main/config/labels.yaml).

## Fix one item

Read the item. Pick the area from the substance of the work, not the title.
If two areas fit, pick the one where the fix lands. Then apply the checklist for its kind.

| Kind | Required labels and dates |
|---|---|
| Open issue or task | one `orbit::<area>`, one `type::`, one `priority::1-4`, `Category:Orbit`, `group::context-systems` |
| Closed issue or task | one `orbit::<area>`, one `type::`, `workflow::complete`, fixed due date equal to the close date, no `priority::` |
| Epic | `type::`, `Category:Orbit`, `group::context-systems`, one area except for a multi-area container, a due date if it is a deliverable |
| MR | `Category:Orbit`, `group::context-systems`, one `type::`, an area when the work maps to one area, `documentation` for docs |

Type rules: docs work is `type::maintenance` plus `documentation`. Coverage and discussion
items are `type::ignore`. Perf defects are `type::bug`. Perf refactors are
`type::maintenance`. A capability gap is `type::feature` even when filed as a bug.

Map old labels: `PREP` to `orbit::infra`, `devex` to `orbit::dx`, `P1`/`P2` to
`priority::1`/`priority::2`, and bare `bug`/`feature`/`maintenance` to their `type::` form.

Leave other teams' labels alone: `section::`, `devops::`, `automation:`, `backend`,
`frontend`, `bug::availability`, and R&D priority variants stay on the item.

GitLab allows one label per scope. Adding an area removes any other `orbit::` label.
Check what fell off.

## Where labels live

Define all Orbit labels at the `gitlab-org` group. Orbit work also lives in
`gitlab-org/gitlab`, and lower-level labels cannot reach it. Do not create project labels
or labels at `gitlab-org/orbit`. A project copy of a group label is a defect. Move its
carriers to the group label. Then delete the copy.

The `gitlab-com` group holds a sanctioned mirror for runbooks, handbook, and www items.
Keep its colors and descriptions in step with `gitlab-org`.

A new `orbit::` area needs four touches: the `gitlab-org` label, the `gitlab-com`
mirror label, and both the `areas:` list and the `gitlab-com` track entry in the portal's
`config/labels.yaml`. The portal renders swimlanes only for configured areas.
After a config change run `bun run seed` in the portal repo. Label-only changes need no seed.

## Roadmap membership

An item is on the roadmap when it carries one `orbit-roadmap::<confidence>` label.
Committed means leadership can quote the date. Likely means a target quarter with a
known dependency. Exploratory means intent without a date.

The rest derives from GitLab fields:

- Status: closed renders as shipped. Open `workflow::in dev|in review|verification|complete`
  renders as building. `workflow::planning breakdown|ready for development` renders as
  scoping. Validation or design values render as discovery. No label renders as planned.
- Quarter: from the due date. GitLab FY runs Feb to Jan and is named for the year it ends.
  FY27 Q1 is Feb-Apr 2026. No due date renders as unscheduled.
- Risk: the native health-status widget (On track, Needs attention, At risk).

One row per deliverable. When an epic is the roll-up, only the epic carries the
`orbit-roadmap::` label. Children with the label create duplicate rows.
Every unshipped roadmap item needs a due date, a `workflow::` label, and a health status.

A shipped deliverable stays on the roadmap with `orbit-roadmap::committed` and
`workflow::complete`. Workstream containers and planning epics stay off the roadmap.

The fixed due date of a closed item is the date the work landed. Bulk-close dates
are bookkeeping, not completion dates. Use the last substantive MR or child instead.

## Epics

An epic is a dated deliverable with a clear outcome. A long-standing theme is an
`orbit::` area label, not an epic. An epic that maps to one area is a bucket.
An epic that spans areas or wraps a workstream is a container.

- Bucket: label every child with the area, close the epic with a pointer comment,
  then unparent the children.
- Completed deliverable or container: close with `workflow::complete` and a fixed due date.
  Keep the children as the record of the work.

A container gets `type::` and `workflow::` but no area label.

## Bulk sweeps

Work one project at a time. This keeps two people off the same item. Enumerate open
issues with `glab api "projects/<path>/issues?state=opened&per_page=100&page=N"`.
Classify each one that misses an axis with the checklist. Apply in batches.
State the plan and counts before you change more than 20 items.

A person approves every new issue and epic. Prepare the draft and labels. Run the
create call only after the user approves in this session. New issues get all three axes
at creation. New deliverables also get an epic, a fixed due date, and an
`orbit-roadmap::` label on the epic.

## Labels outside the taxonomy

Do not add or create these labels. The portal ignores them.

- `orbit::customer-zero`, `orbit::GA`, `orbit::backlog`
- `knowledge graph` and its variants, `group::knowledge graph`, `group::context systems`
- `orbit::hackathon`, `orbit_hackathon::*`, `Big Rock::Orbit`
- `GKG::*`, `ai_review::*`, `design partner`, `beta-blocker`, `PREP`, `devex`

Remove `GKG::*` when you see it. Customer Zero reports get flat `customer-zero`.

## Mechanics

Use label ids, never names. Same-named labels make name-based REST ambiguous.
Label search is a substring match. Search a full name, not a bare scope.

```bash
glab api "groups/gitlab-org/labels?search=orbit%3A%3Aquery&only_group_labels=true&include_ancestor_groups=false&per_page=50"
```

The mutation needs a `WorkItem` gid. The API rejects an `Issue` gid with the same number.

```bash
glab api graphql -f query='query { project(fullPath: "gitlab-org/orbit/knowledge-graph") {
  workItems(iids: ["971"]) { nodes { id } } } }'
# epics: group(fullPath: "gitlab-org") { workItem(iid: "20992") { id } }
```

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

MRs use `mergeRequestSetLabels` with `operationMode: APPEND` or `REMOVE`.

```bash
glab api graphql -f query='mutation {
  mergeRequestSetLabels(input: {projectPath: "gitlab-org/orbit/knowledge-graph",
    iid: "2033", labelIds: ["gid://gitlab/GroupLabel/52162078"],
    operationMode: APPEND}) { errors }
}'
```

Sweep by label. `labelName` accepts wildcards and `includeDescendants: true` reaches
subgroup projects.

```bash
glab api graphql -f query='query { group(fullPath: "gitlab-org") {
  workItems(labelName: ["orbit-roadmap::*"], includeDescendants: true, first: 100) {
    nodes { iid title state webUrl } } } }'
```

Mint a new area at `gitlab-org` with the description from `config/labels.yaml`:

```bash
glab api -X POST "groups/gitlab-org/labels" \
  -f name="orbit::query" -f color="#1f75cb" \
  -f description="Query engine, DSL, compiler, pagination, agent query ergonomics"
```

API facts:

- Label promotion (`PUT /projects/:id/labels/:label_id/promote`) merges same-named
  project labels only in direct child projects. Copies in subgroup projects need a
  manual carrier swap.
- Epic due dates roll up from children. Set `isFixed: true` to pin one.
- GraphQL `count` caps at 1000. Paginate with `pageInfo` for real totals.
- Do not filter out confidential items. The portal badges them.
- Batch up to 10 aliased mutations per request to stay under the complexity limit of 250.

Label ids at `gitlab-org`:

| Label | Id |
|---|---|
| `orbit-roadmap::committed` / `likely` / `exploratory` | 52162908 / 52162910 / 52162911 |
| `orbit::query` / `graph-completeness` / `indexing` | 52179830 / 52180114 / 52180352 |
| `orbit::reliability` / `security` / `dx` / `ux` | 52177930 / 52162078 / 52180353 / 52180354 |
| `orbit::dap-integration` / `monetization` / `analytics` | 52180355 / 52177262 / 52177482 |
| `orbit::integrations` / `code-graph` / `local` / `infra` | 52180356 / 52177075 / 52177716 / 52177717 |
| `priority::1` / `2` / `3` / `4` | 3857370 / 3857523 / 3857529 / 3857543 |
| `type::bug` / `feature` / `maintenance` / `ignore` | 2278648 / 10230929 / 15119514 / 26375975 |
| `Category:Orbit` / `group::context-systems` / `workflow::complete` | 52177066 / 49441857 / 28669354 |
| `customer-zero` / `documentation` | 52179831 / 2278655 |
| `gitlab-com` mirror: `Category:Orbit` / `group::context-systems` | 52206994 / 46682047 |

## Conventions

Follow the host repository's `AGENTS.md` for comment format. Record label and epic
decisions in the portal's `docs/label-taxonomy-and-epic-cleanup.md`. Rename GKG or
Knowledge Graph to Orbit in any title you touch. Keep code identifiers such as
`gkg-webserver` and `GKG_INDEXER` as they are.
