---
name: orbit-planning
description: >-
  Label Orbit work items, epics, and MRs with the canonical taxonomy (orbit::
  area, type::, priority::). Manage public roadmap items, due dates, health,
  and epic hygiene. Use when you label or triage issues or MRs, add or update
  a roadmap item, or draft or repurpose epics.
version: 3.0.0
allowed-tools: Read, Bash(glab *)
---

# Orbit planning

GitLab labels, dates, and health on each item are the source of truth. This
repository is public, so do not copy customer names or other sensitive data
from a confidential item into a public one.

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

Map old labels: `devex` to `orbit::dx`, `P1`/`P2` to `priority::1`/`priority::2`, and
bare `bug`/`feature`/`maintenance` to their `type::` form.

Leave other teams' labels alone: `section::`, `devops::`, `automation:`, `backend`,
`frontend`, `bug::availability`, and R&D priority variants stay on the item.

An item can have only one label per scoped prefix. When you add an area, GitLab removes the old
`orbit::` label. Check which label it removed.

## Where labels live

Define all Orbit labels at the `gitlab-org` group. Orbit work also lives in
`gitlab-org/gitlab`, and lower-level labels cannot reach it. Do not create project labels
or labels at `gitlab-org/orbit`. A project copy of a group label is a defect. Move its
carriers to the group label. Then delete the copy.

The `gitlab-com` group holds a mirror for runbooks, handbook, and www items. Keep its
colors and descriptions in step with `gitlab-org`. A new `orbit::` area needs both labels.

## Roadmap

The public roadmap is built from the Markdown pages in `docs/pages/roadmap/`, one page per
item. The frontmatter sets `theme`, `status` (`shipped`, `in-progress`, `planned`,
`exploring`), `start` and `end` quarters such as `FY27 Q4`, and an optional `tracking`
work item URL. Themes are listed in `roadmap.yaml` in the same folder. GitLab FY runs Feb
to Jan and is named for the year it ends.

The build reads the state, due date, and health status of each `tracking` item. A
confidential tracking item fails the build. Give each unshipped tracking item a due date,
a `workflow::` label, and a health status.

The tracking epic also carries one `orbit-roadmap::<confidence>` label:

| Label | Meaning |
|---|---|
| `orbit-roadmap::committed` | Leadership can quote the date. |
| `orbit-roadmap::likely` | A target quarter with a known dependency. |
| `orbit-roadmap::exploratory` | Intent without a date. |

Only the roll-up epic carries the label, not its
children. A shipped item keeps `orbit-roadmap::committed` and `workflow::complete`.
Workstream containers and planning epics stay off the roadmap.

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

Work one project at a time, so that two people do not change the same item. Enumerate open
issues with `glab api "projects/<path>/issues?state=opened&per_page=100&page=N"`.
Classify each one that misses an axis with the checklist. Apply in batches.
State the plan and counts before you change more than 20 items.

A person approves every new issue and epic. Prepare the draft and labels. Run the
create call only after the user approves in this session. New issues get all three axes
at creation. A new roadmap item also gets a tracking epic with a fixed due date and an
`orbit-roadmap::` label.

## Labels outside the taxonomy

Do not add or create these labels.

- `orbit::customer-zero`, `orbit::GA`, `orbit::backlog`
- `knowledge graph` and its variants, `group::knowledge graph`, `group::context systems`
- `orbit::hackathon`, `orbit_hackathon::*`, `Big Rock::Context`
- `GKG::*`, `ai_review::*`, `design partner`, `beta-blocker`, `devex`

Remove `GKG::*` when you see it. Customer Zero reports get flat `customer-zero`.

## Mechanics

Use label IDs, never names. Many groups have labels with the same name, so a name can match the wrong label.
Label search matches a substring of the name or the description. Search a full name, and check the name of each hit.

```bash
glab api "groups/gitlab-org/labels?search=orbit%3A%3Aquery&only_group_labels=true&include_ancestor_groups=false&per_page=50"
```

The mutation needs a `WorkItem` gid. An issue has an `Issue` gid and a `WorkItem` gid with the same number, and the mutation rejects the `Issue` gid.

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

Sweep by label. `labelName` accepts wildcards, and `includeDescendants: true` reaches
subgroup projects.

```bash
glab api graphql -f query='query { group(fullPath: "gitlab-org") {
  workItems(labelName: ["orbit-roadmap::*"], includeDescendants: true, first: 100) {
    nodes { iid title state webUrl } } } }'
```

Create a new area at `gitlab-org`:

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
- Batch up to 10 aliased mutations per request to stay under the complexity limit of 250.

Label IDs at `gitlab-org`:

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

Follow the host repository's `AGENTS.md` for comment format. Rename GKG or
Knowledge Graph to Orbit in any title you touch. Keep code identifiers such as
`gkg-webserver` and `GKG_INDEXER`.
