# docs-write and docs-review flows

Two GitLab Duo custom flows for this project's docs. Plan and coordination:
[#1494](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494).

- `docs-write.yml` drafts the docs MR for a feature MR, fixes its threads,
  and hands it off.
- `docs-review.yml` reviews every docs MR in the project, whether a person or
  docs-write wrote it, once per head, in its own session. It posts threads
  and never pushes.

The logic lives in two skills in
[`gitlab-org/orbit/team/skills`](https://gitlab.com/gitlab-org/orbit/team/skills),
which both flows fetch at runtime: `docs-write` for the writer, the fixers,
and both flows' step helpers, and `docs-review` for the reviewer. File
shapes and route tokens: `skills/docs-write/references/contracts.md` there.

The YAML files are the reviewable copies. The AI Catalog items are what
runs: after a change here, update the catalog item from the file, then
update this table.

| Setting | Value |
|---|---|
| `SKILLS_REF` in both files | `33046b994845965cf2f8695adbd2b42f58d3a591`, the head of `team/skills` branch `docs-review/flow`, stacked on MR 38 (both unmerged). Re-pin both files to the squash commit on `main` once they merge. |
| `MODE` in both files | `full` |
| `FLOW_USER` in `docs-write.yml` | `44621578`, the user ID of `ai-docs-write-gitlab-org_510eee`, the service account that authors the flow's notes. Under composite identity `GET /user` returns the person who triggered the run, so the skills need it. An ID survives a rename. |
| `REVIEW_USER` in `docs-write.yml` | Empty until the docs-review service account exists, then its user ID. docs-write trusts that account's review threads and waits for its summary note. While it is empty, docs-write finds no review and holds each docs MR in draft after 45 minutes. |
| `DOCS_REVIEWERS` in `docs-write.yml` | `fneill,z_painter`: technical writers who approve and merge the docs MRs |
| AI Catalog item `docs-write` | Item 1020976 in this project, private, enabled. Trigger: **Mention** only during rollout. Service account `ai-docs-write-gitlab-org_510eee`, Reporter in `team/skills`. |
| `FLOW_USER` in `docs-review.yml` | Empty until the service account exists, then its user ID. While it is empty, every docs-review run ends in `setup` with no model call. |
| `MIN_IMPORTANCE` in `docs-review.yml` | `4`: post findings at importance 4 or 5, and every blocker. Live runs showed fixers that followed wording nits made docs worse. |
| `REVIEW_PATHS` in `docs-review.yml` | `docs/docs-gitlab-com/,docs/navigation.yaml`, the user docs. See [Engineering docs](#engineering-docs). |
| AI Catalog item `docs-review` | To be created: private, in this project. Its service account, `ai-docs-review-gitlab-org` or similar, needs the Reporter role in `team/skills`. Trigger: **Mention** during rollout, then **Pipeline**. |

## How docs-write runs

1. `setup` fetches `SKILLS_REPO` at `SKILLS_REF` with the flow's
   `GITLAB_TOKEN`, then runs the gate (threshold 6) and the related-MR check.
   Most MRs end here, with no model call and no GitLab Credits.
1. `MODE=full`, the committed mode, runs the rest. `MODE=shadow` stops after
   the gate and logs one line per MR.
1. A writer agent writes the change in one context. It runs `check.py`, the
   docs linters and the rules a script can check, until the check is clean.
1. `publish` checks the writer report, runs `check.py` again as a hard gate,
   refuses a branch that changes files outside the user docs, pushes, and
   opens or updates the draft docs MR. The description lists what to check.
1. The CI loop polls for up to 15 minutes per step, 90 minutes in all. Docs
   failures go to the fixer.
1. On a green pipeline, `threads` waits for the docs-review summary note on
   the head it pushed: every 30 seconds, up to 14 minutes per step and 45
   minutes per head. With no review by then, it hands off with the reason
   in the description, and the MR stays in draft. docs-write no longer
   reviews its own change.
1. A fresh fixer agent, one per cycle and four at most, fixes the unresolved
   threads from trusted authors in one pass: the flow identity, the
   docs-review account (`REVIEW_USER`), Duo Code Review, the authors and
   reviewers of the feature MRs in the group, the docs MR reviewers, and
   project members with the Developer role or higher. Only MRs from this
   project join the group, never a fork MR. A review thread with a
   suggestion block gets that exact text when the code agrees.
   Replies from anyone else inside a trusted thread are dropped. The flow
   never shows other threads to an agent. They stay open for the person who
   merges and do not hold the handoff. The flow replies once per thread. It
   resolves what it fixed once `check.py` is clean. A thread it cannot fix
   gets a reply and stays open as a reviewer decision.
1. A fix push starts a new pipeline, so docs-review reviews the new head. It
   posts only findings it has not posted before, and docs-write waits for
   that review too.
1. When CI is green, the head has a review, and only decision threads and
   threads from other people are open, the flow removes `Draft:`, adds the
   feature MR author and the `DOCS_REVIEWERS` as reviewers, and posts one
   note. The note lists the decision threads and links the other threads
   without their text. The flow assigns nobody. Otherwise the MR stays in
   draft, with the reason in its description. A person always merges the
   docs MR: one of the `DOCS_REVIEWERS` approves and merges it.

```mermaid
flowchart TD
  T[a mention] --> S[setup: gate, related]
  S -->|no docs, shadow| E[end]
  S --> W[writer agent: docs-write, check.py]
  S -->|mention on a docs MR| TH
  W --> P[publish: check.py gate, push, draft MR]
  P -->|findings| F[fresh fixer agent: docs-write fix.md]
  F --> P
  P --> C[CI loop]
  C -->|docs failure| F
  C -->|green| TH[threads: wait for the docs-review summary on the head]
  TH -->|no summary yet| TH
  TH -->|review and other open threads| F
  TH -->|no review after 45 minutes: hold| H
  TH -->|only decisions left| H[handoff: ready, reviewer, note]
```

## How docs-review runs

1. `setup` fetches the skills like docs-write, then decides with no model
   call. It ends the run when the goal is neither an MR pipeline nor a
   mention; the pipeline is not green (a failed
   `docs-write-merge-order-check` alone counts as green); the MR is closed,
   from a fork, has `~"docs-factory::skip"`, or moved past the pipeline's
   commit; it changes nothing under `REVIEW_PATHS`; its head already has a
   docs-review summary; or `FLOW_USER` is empty. Every code-only MR pipeline
   ends here.
1. Otherwise it checks out the MR head and writes what a human reviewer
   sees to `.docs-write/review/`: the docs diff limited to `REVIEW_PATHS`,
   the changed pages, and the feature code. The feature MRs are the ones in
   a docs-write MR's marker, from this project. Without a marker, an MR that
   also changes code is its own feature MR. A docs-only MR from a person is
   checked against the code at its head.
1. A fresh reviewer agent loads only `docs-review` and runs its checks,
   read-only. It writes findings to a file.
1. `post` discards any reviewer edit, combines the findings, and keeps only
   those at `MIN_IMPORTANCE` or more, and blockers. Each becomes one thread
   on its line: a GitLab suggestion block for a small exact fix (five lines
   at most, quote still in the file), a plain **Docs review** thread that
   says what to fix for any other fix, and a **Needs reviewer decision**
   thread for a question. A finding it posted on an earlier head is not
   posted again. Then one summary note for the head, which docs-write waits
   for. A failed review posts the summary only, with its status.
1. The flow never pushes, resolves, or marks anything ready. On a
   docs-write MR, docs-write's fixers fix the threads. On a person's MR, the
   author does.

```mermaid
flowchart TD
  T[MR pipeline finished, or a mention] --> S[setup: no model call]
  S -->|not an MR pipeline or mention| E[end]
  S -->|pipeline not green| E
  S -->|closed, fork, opted out, or moved on| E
  S -->|no change under REVIEW_PATHS| E
  S -->|head already reviewed| E
  S -->|FLOW_USER empty, or shadow mode| E
  S -->|docs to review| R[reviewer agent: docs-review only, read-only]
  R --> P[post: floor 4, threads, suggestion blocks, summary note]
  P --> E
```

## Both flows together

```mermaid
sequenceDiagram
  actor Dev as Feature MR author
  participant W as docs-write
  participant D as Docs MR
  participant CI as Docs MR pipeline
  participant R as docs-review
  Dev->>W: mention on the feature MR
  W->>D: push docs, open draft docs MR
  D->>CI: pipeline on head 1
  CI-->>R: pipeline event, green
  R->>D: inline threads and a summary for head 1
  W->>D: wait for the head 1 summary, then the fixer fixes the threads
  W->>D: push head 2, reply, resolve fixed threads
  D->>CI: pipeline on head 2
  CI-->>R: pipeline event, green
  R->>D: only new findings, and a summary for head 2
  W->>D: no thread to fix: handoff, ready for review
  Dev->>D: a technical writer reviews and merges
```

## Engineering docs

docs-review has a lighter profile for `docs/pages/engineering/`, off by
default. To turn it on, add the folder to `REVIEW_PATHS` in a reviewed MR
here, then update the catalog item:

```plaintext
REVIEW_PATHS=docs/docs-gitlab-com/,docs/navigation.yaml,docs/pages/engineering/;
```

Engineering pages get only three checks: claims against the code, links and
anchors, and missing runbook steps. No style guide, word list, topic type,
or brevity findings: `post` drops them even when a linter reports them.

## Guardrails and residual risk

Every agent in both flows, the docs-review reviewer included, has
`run_command`, a shell in the same workload as the run's `GITLAB_TOKEN`. Tool options cannot restrict a shell, so an agent can read
the token or call the GitLab API. What stands between text an agent reads
and a write:

- The prompts forbid pushes, API writes, and resolving threads, and treat
  MR, thread, and file text as data.
- Scripts enforce three rules: the docs-only push guard, that the flow
  never resolves a **Needs reviewer decision** thread, and thread intake.
  Thread intake limits who can put thread text in front of a fixer to the
  trusted authors above. Thread markers and the review summary count only
  when the flow identity or the docs-review account wrote them. The gate ends the run on a fork MR, and the related-MR check
  groups only MRs from this project, so a fork MR that names the same issue
  never joins the group.
- A mention on a docs MR runs the threads stage only when its `docs-write/`
  branch is in this project, not a fork, and the flow identity already has
  a note on the MR.
- docs-review has no push step: `post` discards any reviewer commit or edit
  outside `.docs-write/` before it posts. Finding text is neutralized: no
  mentions, quick actions, or HTML comments, and no suggestion block for
  text that has one. It ends with no model call on fork MRs and on MRs with
  `~"docs-factory::skip"`.
- The other rules are prompt rules that flow steps check afterward:
  `publish` runs `check.py` as a gate.

Residual risk: a prompt injection in text the agents do read (the diffs and
code of the feature MRs in the group, which no filter checks, the docs diff
of any MR from this project that docs-review reviews, or a thread from an
allowed author) could make an agent use the token before any check runs.
docs-review threads reach docs-write's fixers as trusted text, so an
injection that steers the reviewer's findings also reaches a fixer. The token has the scope of the person
who triggered the run. The checks cannot undo an API write or a push to
another branch.

## Validate

From a `gitlab-org/orbit/team/skills` checkout, with PyYAML (and `jsonschema`
for the full schema check):

```shell
python3 skills/docs-write/scripts/validate_flow.py \
  <orbit>/.gitlab/duo/catalog/flows/docs-write.yml \
  <orbit>/.gitlab/duo/catalog/flows/docs-review.yml
```

It checks the 40 KiB catalog limit on the raw YAML, the schema fields, that
every router target exists, that every route file has a reader whose router
ends by default, that every component has a toolset, and that `setup` sets
`MODE`. It checks `docs-review.yml` against `review_step.py` and
`docs-write.yml` against `flow_step.py`.

## Set up

Prerequisites:

- The Maintainer or Owner role in the project.
- Custom flows turned on for the top-level group, and a runner for flows.

### Register the flow as a private AI Catalog item

1. In the project, select **AI** > **Flows** > **New flow**.
1. In **Display name**, enter `docs-write` (or `docs-review`).
1. For **Visibility**, select **Private**.
1. Under **Configuration**, select **Flow**, and paste `docs-write.yml` (or
   `docs-review.yml`).
1. Keep `setup` as committed: `MODE=full` and `SKILLS_REF` pinned to a
   `team/skills` commit. Change either one in a reviewed MR here first.
1. Select **Create flow**.

The GraphQL mutations are `aiCatalogFlowCreate` (with `visibility: PRIVATE`
and `release: true`) and `aiCatalogFlowUpdate` (with `versionBump`).

### Enable the flow and give it access to the skill

1. Select **AI** > **Flows** > **Managed**, select the flow, then **Enable**.
1. Select the project. GitLab creates the service account
   `ai-docs-write-<group>`, for example `ai-docs-write-gitlab-org`
   (`ai-docs-review-<group>` for docs-review).
1. Ask an Owner of `gitlab-org/orbit/team/skills` to add that service account
   with the Reporter role. Without it, every run ends at `setup`. The flows
   clone the whole project: each runs scripts from both skills.
1. For docs-review, put the service account's user ID in `FLOW_USER` in
   `docs-review.yml` and in `REVIEW_USER` in `docs-write.yml`, in a reviewed
   MR here, then update both catalog items.

### Create the docs-write trigger

During rollout the trigger is **Mention** only, so the flow runs only where
someone asks for it. In **AI** > **Triggers**, select the flow's service
account and the flow, and add one condition:

| Event | Why |
|---|---|
| **Mention** | On a feature MR: run the gate, and draft the docs MR when it needs one. On a docs MR the flow opened in this project: fix open threads from trusted authors (the flow, Duo Code Review, authors and reviewers of feature MRs from this project, docs MR reviewers, and Developer+ members), then hand off again. |

Leave **Instructions** empty. GraphQL: `aiFlowTriggerCreate` with
`eventTypes: [0]` (mention) and `aiCatalogItemConsumerId` set to the
project's item configuration.

A second trigger keeps docs current. Add it only after this file is on `main`
and `SKILLS_REF` points at a `team/skills` commit on `main` that reads pipeline
goals: older skills fail on every pipeline. Then add **Pipeline** events
(`eventTypes: [3]`) with this filter, and change the trigger row in the
settings table to say **Mention** and **Pipeline**:

```json
{"pipeline_hooks": {"rules": [
  {"field": "object_attributes.status", "operator": "in", "value": ["success"]},
  {"field": "object_attributes.source", "operator": "in", "value": ["merge_request_event", "push"]}
]}}
```

The source rule matters: flow sessions run as pipelines too, and without it
each session would start another. Pipeline runs post no progress note on
the MR. A run on a pipeline that no docs-write MR covers ends in `setup`,
with no model call. See [Drift and merge order](#drift-and-merge-order).
Those runs still cost runner time. In November 2026, this project had about 55 successful
MR and push pipelines a day. Each one starts a session that spends 90 to 100 seconds on setup.

### Create the docs-review trigger

During rollout, the trigger is **Mention** only (`eventTypes: [0]`) with the
docs-review service account: mention it on a docs MR to review its head.

Then switch to **Pipeline** events (`eventTypes: [3]`), like Duo Code
Review, once `FLOW_USER` and `REVIEW_USER` are set and `SKILLS_REF` points
at a `team/skills` commit with `review_step.py`:

```json
{"pipeline_hooks": {"rules": [
  {"field": "object_attributes.status", "operator": "in", "value": ["success", "failed"]},
  {"field": "object_attributes.source", "operator": "in", "value": ["merge_request_event"]}
]}}
```

`failed` is in the filter because a docs-write MR pipeline fails on
`docs-write-merge-order-check` until its feature MRs merge: `setup` reviews
a failed pipeline only when that job is its only failure, and ends on any
other. The source rule keeps MR pipelines only: flow sessions run as
pipelines too, and branch and default-branch pipelines have no MR to
review. Every MR pipeline in the project starts a run, and every run on an
MR that changes no docs ends in `setup`, with no model call, after about
100 seconds of runner time.

### Later triggers for docs-write

To run on every new or ready MR later, add **Merge request** events
(`eventTypes` 6 and 4) in a reviewed MR here first. Filter **Created** with
`{"merge_request": {"rules": [{"field": "action", "operator": "in", "value": ["created"]}]}}`.
Filters are keyed by event type, and **Marked ready** events have no filter
(`Ai::FlowTriggers::FilterEvaluator` in `gitlab-org/gitlab`).

With **Merge request** events on, events that a flow causes start triggers
too, so the docs MR the flow opens starts the flow again. The gate ends that run with no model call: it skips
fork MRs, MRs on a `docs-write/` branch or with the docs-write marker, and
MRs with `~documentation` or `~"docs-factory::skip"`. Each run still spends about
100 seconds of runner time on setup.

## Drift and merge order

- **A push to the feature MR:** when its pipeline succeeds, the flow checks
  whether a docs-write MR covers the MR and whether the feature MR head moved
  since the last docs update. On drift, the writer updates the docs against the
  new diff, docs-review reviews the new head, and fixers fix its threads. The
  docs MR stays ready for review and gets new commits, not a rewrite of the
  history. It posts one drift note per new head.
- **The feature MR merges:** the default-branch pipeline runs the same check.
  With no drift, the flow posts one note that the docs match the merged head,
  and starts a new docs MR pipeline.
- **Merge order:** `docs-write-merge-order-check` runs on every docs MR
  pipeline (`docs-write/mr-*` branches). It fails while any feature MR in the
  docs MR marker is not merged. This project requires a green pipeline to
  merge, so docs never merge before the code. The new pipeline that the flow
  starts at merge, or a retry of the job, turns it green. Both flows count
  this one failure as expected, so docs-review still reviews the docs MR and
  docs-write still hands it off.

## Shadow mode

`MODE=shadow` turns the writes off: use it to test the gate on new trigger
events before they open docs MRs. Each run logs one line in the session
(**AI** > **Sessions**, and the workload job log), and writes nothing:

```plaintext
DOCS-WRITE SHADOW {"mr":123,"gate":{"decision":"small","score":7.5,...},"pages":[...],"related":{...}}
```

Compare these lines with what a technical writer decides for the same MRs.
docs-review's shadow mode logs what it would review, with no model call:

```plaintext
DOCS-REVIEW SHADOW {"base":"...","head":"...","pages":[...],"engineering_pages":[],"feature_mrs":[...],"mr":123,...}
```

## Turn it off

Turn a trigger off in **AI** > **Triggers**, or disable a flow in
**AI** > **Flows**. Disabling removes the service account and its triggers.
With docs-review off, docs-write holds each docs MR in draft after 45
minutes: also empty `REVIEW_USER`, or turn docs-write off. To skip one MR in
both flows, add `~"docs-factory::skip"`.
