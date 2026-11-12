# docs-write flows

GitLab Duo custom flows that draft the docs MR for a feature MR in this
project. Plan and coordination:
[#1494](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494).
The logic lives in the `docs-write` and `docs-review` skills in
[`gitlab-org/orbit/team/skills`](https://gitlab.com/gitlab-org/orbit/team/skills),
which the flow fetches at runtime. File shapes and route tokens:
`skills/docs-write/references/contracts.md` there.

| File | What it is |
|---|---|
| `docs-write.yml` | The full flow: gate, related, writer, precheck, publish, CI loop, review, handoff. |
| `docs-write-shadow.yml` | Rollout stage 1: gate and related only. No model call, no GitLab write. |

These files are the reviewable copy. The AI Catalog item is what runs: after
a change here, update the catalog item from the file.

## How the flow runs

1. `setup` fetches `SKILLS_REPO` (`gitlab-org/orbit/team/skills`) at `SKILLS_REF` with the
   flow's `GITLAB_TOKEN`, puts the skills in `.docs-write/skills/`, and runs
   the gate. The fetch works only when the flow service account and the
   person who triggered the run can both read `team/skills`. Otherwise the
   run ends with no docs MR.
1. Most MRs end at the gate, with no model call and no GitLab Credits.
1. Each step writes `.docs-write/route/<file>`. A `read_file` step reads it,
   and the router branches on the exact token. A missing file ends the flow.
1. One writer agent works in a single context. A check of its
   `writer_report.json` follows: an invalid report gets one fixer pass, and the
   statuses `no_docs` and `blocked` end the run with no docs MR.
1. A fixer handles precheck and CI failures, at most three cycles in all.
1. The writer and the fixer commit; only flow steps push, after the precheck
   passes and after a check that the branch changes only user docs.
1. The CI wait polls for up to 15 minutes per step and loops back to itself,
   up to 90 minutes in all for one docs MR.
1. After the review, the flow waits for CI on the final commit. When the
   gate called the change `small`, the docs MR is marked ready only when CI is
   green, all three reviewers finished, and no finding of importance 4 or 5 is
   open. A `large` change gets one fact-check note for
   the feature MR authors and stays in draft.

No agent has a GitLab write tool. `run_command` is a shell, so tool options
cannot restrict it: the prompts forbid pushes and writes, and the flow steps
check the result (the docs-only push guard, the reviewer read-only check).

## Validate

From a `gitlab-org/orbit/team/skills` checkout, with PyYAML (and `jsonschema`
for the full schema check):

```shell
python3 skills/docs-write/scripts/validate_flow.py \
  <knowledge-graph>/.gitlab/duo/catalog/flows/docs-write.yml \
  <knowledge-graph>/.gitlab/duo/catalog/flows/docs-write-shadow.yml
```

It checks the 40 KiB catalog limit on the raw YAML, the schema fields, that
every router target exists, that every route file has a reader whose router
ends by default, that every component has a toolset, and that the shadow
flow has no model call and no write.

## Set up

Prerequisites:

- The Maintainer or Owner role in the project.
- Custom flows turned on for the top-level group, and a runner for flows.

### Register the flows as private AI Catalog items

1. In the project, select **AI** > **Flows** > **New flow**.
1. In **Display name**, enter `docs-write` (or `docs-write-shadow`).
1. For **Visibility**, select **Private**.
1. Under **Configuration**, select **Flow**, and paste the YAML file from
   this folder.
1. In `setup`, set `SKILLS_REF` to the `team/skills` branch, tag, or commit
   to run. Pin a commit or tag outside the sandbox. For a test without
   `team/skills` access, point `SKILLS_REPO` at a project the service account
   can read, with the skills vendored at `skills/` on that ref.
1. Select **Create flow**.

The GraphQL mutation is `aiCatalogFlowCreate` with `projectId`, `name`,
`description`, `visibility: PRIVATE`, `definition` (the YAML), and
`release: true`.

### Enable the flow and give it access to the skills

1. Select **AI** > **Flows** > **Managed**, select the flow, then **Enable**.
1. Select the project. Remove the default **Mention**, **Assign**, and
   **Assign reviewer** conditions; the trigger below adds what the flow needs.
1. GitLab creates the service account `ai-<flow>-<group>`, for example
   `ai-docs-write-gitlab-org`.
1. Ask an Owner of `gitlab-org/orbit/team/skills` to add that service account
   with the Reporter role. Without it, every run ends at `setup`.

### Create the trigger

In **AI** > **Triggers** > **New trigger**, select the flow's service
account and **Flow or external agent** with the flow, and add these
conditions:

| Event | Run when | Why |
|---|---|---|
| **Merge request** | **Created** | New MRs. The gate skips drafts. |
| **Merge request** | **Marked ready** | Draft MRs that become ready. |
| **Merge request** | **Approved** | Re-check: the flow writes again only when the feature MR changed since the docs update. |
| **Mention** | | Run on demand: mention the service account on a feature MR. |

Leave **Instructions** empty: the flow reads only the MR from the goal.
GraphQL: `aiFlowTriggerCreate` with `eventTypes: [0, 4, 6]` (mention, merge
request ready, merge request) and the filter
`{"merge_request": {"rules": [{"field": "action", "operator": "in", "value": ["created", "approved"]}]}}`.

Events that a flow causes do start triggers, so the docs MR the flow opens
starts the flow again. The gate ends that run with no model call: it skips
MRs on a `docs-write/` branch or with the docs-write marker, and MRs with
`~documentation` or `~"docs-factory::skip"`. Each run still spends about
100 seconds of runner time on setup. Trigger filters on `draft` and
`created_by_flow` are not available, so the gate does this filtering.

## Shadow mode (rollout stage 1)

Register `docs-write-shadow.yml` as its own private flow, enable it, and give
it the same trigger. Do not enable both flows on the same events.

Each run logs one line in the session (**AI** > **Sessions**, and the
workload job log):

```plaintext
DOCS-WRITE SHADOW {"mr":123,"gate":{"decision":"small","score":7.5,...},"pages":[...],"related":{...}}
```

Compare these lines with what a technical writer decides for the same MRs.
Shadow runs read GitLab with GET requests only and create no branch, note,
or MR.

## Turn it off

Turn the trigger off in **AI** > **Triggers**, or disable the flow in
**AI** > **Flows**. Disabling removes the service account and its triggers.
To skip one MR, add `~"docs-factory::skip"`.
