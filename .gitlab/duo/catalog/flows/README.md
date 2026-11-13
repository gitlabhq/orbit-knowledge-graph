# docs-write flow

A GitLab Duo custom flow that drafts, reviews, and hands off the docs MR for
a feature MR in this project. Plan and coordination:
[#1494](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494).
The logic lives in two skills in
[`gitlab-org/orbit/team/skills`](https://gitlab.com/gitlab-org/orbit/team/skills),
which the flow fetches at runtime: `docs-write` for the writer and the fixers,
and `docs-review` for the reviewer. File shapes and route tokens:
`skills/docs-write/references/contracts.md` there.

`docs-write.yml` is the reviewable copy. The AI Catalog item is what runs:
after a change here, update the catalog item from the file.

## How the flow runs

1. `setup` fetches `SKILLS_REPO` at `SKILLS_REF` with the flow's
   `GITLAB_TOKEN`, then runs the gate (threshold 6) and the related-MR check.
   Most MRs end here, with no model call and no GitLab Credits.
1. `MODE=shadow` in `setup` stops after the gate and logs one line per MR.
   `MODE=full` runs the rest.
1. A writer agent writes the change in one context. It runs `check.py`, the
   docs linters and the rules a script can check, until the check is clean.
1. `publish` checks the writer report, runs `check.py` again as a hard gate,
   refuses a branch that changes files outside the user docs, pushes, and
   opens or updates the draft docs MR. The description lists what to check.
1. The CI loop polls for up to 15 minutes per step, 90 minutes in all. Docs
   failures go to the fixer.
1. On the first green pipeline, a fresh reviewer agent reviews the change,
   read-only. It loads only `docs-review` and sees only what a human
   reviewer sees: the changed pages, the docs diff, and the feature MR diff
   and code.
1. A script applies the exact fixes and posts the review as one comment,
   `For agent: docs review`, with every finding numbered. Each finding that
   needs a person also gets a thread titled **Needs reviewer decision**.
1. A fresh fixer agent, one per cycle and four at most, fixes the numbered
   items in one pass, and any unresolved thread from Duo Code Review or a
   person. The flow replies once on the review comment and once per thread.
   It resolves what it fixed once `check.py` is clean. A thread it cannot
   fix gets a reply and stays open as a reviewer decision.
1. When CI is green and only decision threads are open, the flow removes
   `Draft:`, assigns the feature MR author as reviewer, and posts one note
   that lists the decision threads. Otherwise the MR stays in draft, with the
   reason in its description. A person always merges the docs MR.

Agents have no GitLab write tool. `run_command` is a shell, so tool options
cannot restrict it: the prompts forbid pushes and writes, and the flow steps
check the result (the push guard, the reviewer read-only check).

```mermaid
flowchart TD
  T[MR created, marked ready, or a mention] --> S[setup: gate, related]
  S -->|no docs, shadow| E[end]
  S --> W[writer agent: docs-write, check.py]
  S -->|mention on a docs MR| TH
  W --> P[publish: check.py gate, push, draft MR]
  P -->|findings| F[fresh fixer agent: docs-write fix.md]
  F --> P
  P --> C[CI loop]
  C -->|docs failure| F
  C -->|first green| R[reviewer agent: docs-review only]
  R --> TH[threads: exact fixes, review comment, decision threads]
  C -->|later green| TH
  TH -->|review items, open threads| F
  TH -->|only decisions left| H[handoff: ready, reviewer, note]
```

## Validate

From a `gitlab-org/orbit/team/skills` checkout, with PyYAML (and `jsonschema`
for the full schema check):

```shell
python3 skills/docs-write/scripts/validate_flow.py \
  <knowledge-graph>/.gitlab/duo/catalog/flows/docs-write.yml
```

It checks the 40 KiB catalog limit on the raw YAML, the schema fields, that
every router target exists, that every route file has a reader whose router
ends by default, that every component has a toolset, and that `setup` sets
`MODE`.

## Set up

Prerequisites:

- The Maintainer or Owner role in the project.
- Custom flows turned on for the top-level group, and a runner for flows.

### Register the flow as a private AI Catalog item

1. In the project, select **AI** > **Flows** > **New flow**.
1. In **Display name**, enter `docs-write`.
1. For **Visibility**, select **Private**.
1. Under **Configuration**, select **Flow**, and paste `docs-write.yml`.
1. In `setup`, set `MODE` (`shadow` first) and `SKILLS_REF`. Pin a commit
   or tag outside the sandbox.
1. Select **Create flow**.

The GraphQL mutations are `aiCatalogFlowCreate` (with `visibility: PRIVATE`
and `release: true`) and `aiCatalogFlowUpdate` (with `versionBump`).

### Enable the flow and give it access to the skill

1. Select **AI** > **Flows** > **Managed**, select the flow, then **Enable**.
1. Select the project. GitLab creates the service account
   `ai-docs-write-<group>`, for example `ai-docs-write-gitlab-org`.
1. Ask an Owner of `gitlab-org/orbit/team/skills` to add that service account
   with the Reporter role. Without it, every run ends at `setup`. The flow
   clones the whole project: `docs-write` runs scripts from `docs-review`.

### Create the trigger

In **AI** > **Triggers**, select the flow's service account and the flow,
and add these conditions:

| Event | Run when | Why |
|---|---|---|
| **Merge request** | **Created** | New MRs. The gate skips drafts. |
| **Merge request** | **Marked ready** | Draft MRs that become ready. |
| **Mention** | | On a feature MR: run on demand. On a docs MR the flow opened: fix its open threads, then hand off again. |

Leave **Instructions** empty. GraphQL: `aiFlowTriggerCreate` with
`eventTypes: [0, 4, 6]` (mention, merge request ready, merge request) and
the filter `{"merge_request": {"rules": [{"field": "action", "operator": "in", "value": ["created"]}]}}`.

Events that a flow causes start triggers too, so the docs MR the flow opens
starts the flow again. The gate ends that run with no model call: it skips
MRs on a `docs-write/` branch or with the docs-write marker, and MRs with
`~documentation` or `~"docs-factory::skip"`. Each run still spends about
100 seconds of runner time on setup.

## Shadow mode (rollout stage 1)

Set `MODE=shadow` in `setup`. Each run logs one line in the session
(**AI** > **Sessions**, and the workload job log), and writes nothing:

```plaintext
DOCS-WRITE SHADOW {"mr":123,"gate":{"decision":"small","score":7.5,...},"pages":[...],"related":{...}}
```

Compare these lines with what a technical writer decides for the same MRs.

## Turn it off

Turn the trigger off in **AI** > **Triggers**, or disable the flow in
**AI** > **Flows**. Disabling removes the service account and its triggers.
To skip one MR, add `~"docs-factory::skip"`.
