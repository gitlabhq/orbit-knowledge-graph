---
title: Docs-write flow
description: How the docs-write and docs-review flows draft, review, and hand off a docs MR, and how they were tested.
---

The docs-write flow is a GitLab Duo custom flow. When a feature MR in this project is
created or marked ready, it decides whether users need a docs change. If they do, it writes
the change and gets CI green. A separate flow, docs-review, reviews each docs MR head in its
own session and posts inline threads, and docs-write fixes them.
Then it marks the docs MR ready and requests review from the feature MR author and the docs reviewers. It never merges or approves anything.

Status: design v2, built and tested in a private sandbox fork. Not enabled in this project
yet. Plan, decisions, and every result are in
[#1494](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494).

## The flow at a glance

```mermaid
flowchart TD
    accTitle: The docs-write and docs-review flows
    accDescr: A feature MR event or a mention starts docs-write. A script gate and a related-MR check decide whether to write. A writer agent writes the change with a check loop, and a publish script checks it again, pushes, and ends the run. The docs MR pipeline starts the separate docs-review flow. On failed docs jobs, docs-review mentions docs-write to fix CI. On green, it posts one inline thread per finding at importance 4 or more and a summary note that mentions docs-write. Each mention starts a new docs-write run that reads the MR, fixes and pushes, or hands off. After four fix cycles the MR stays in draft.

    A([Feature MR created or marked ready, or a mention]) --> G{Setup: gate and related MRs}
    G -->|no docs, a person's docs MR, or shadow mode| E1([End, no model call])
    G -->|docs needed| W[Writer agent: docs-write skill, check.py loop]
    W --> P[Publish: check.py gate, docs-only push, draft MR]
    P -->|check findings| F[Fixer agent: fresh per cycle]
    F --> P
    P --> E2([End of run])
    E2 -.->|docs MR pipeline finishes| R{docs-review setup}
    R -->|docs jobs failed| M[Mention docs-write: fix CI]
    R -->|green| V[Review agent: docs-review skill, read-only]
    V --> S[Post threads and a summary that mentions docs-write]
    M -.-> N{New docs-write run: read CI and threads}
    S -.-> N
    G -->|mention on a flow docs MR| N
    N -->|docs job failed or open threads| F
    N -->|pipeline running or no review yet| E3([End, the next event continues])
    N -->|only threads for a person left| H([Ready, review requested from feature author])
    N -->|four fix cycles used| D([Stays in draft, reason in description])
```

The writer, fixer, and review agents are model calls. Every other step is a script. Each run ends after a push, and the next event starts a new run, so no run waits for CI or review. Most feature
MRs end at the gate. Each agent loads one skill: the writer and the fixers `docs-write`, the
reviewer only `docs-review`. The reviewer runs in its own flow and session. It sees only the
pages, the docs diff, and the feature code. It reviews every docs MR in the project the
same way, a person's or the flow's.

## Stages

| Stage | What runs | Model call | Goes to |
|---|---|---|---|
| Setup | `needs_docs.py` ends on an MR whose files never need user documentation (website, tests, dependencies, CI configuration, tooling, internal documentation), then scores the diff at threshold 6, `related_mrs.py` groups related MRs, and the docs branch is checked out | No | Write, or end. A mention on a flow docs MR goes to CI: its branch must be in this project, not a fork, and the flow's identity must already have a note on it. |
| Write | One agent follows `write.md`, writes its report, and runs `check.py` until it is clean: the docs linters, structure checks, and words per fact against the report. Each finding gives a reason, a fix, and its limits | Yes | Publish |
| Publish | Checks the writer report and runs `check.py`. Pushes only user docs and only when `check.py` reports no errors. Refuses an MR title that `mr-title-check` would reject, opens the draft docs MR, and posts thread replies. | No | A fixer, or end of the run |
| CI | `ci_loop.py` reads the docs MR pipeline once | No | Threads on green, a fixer on a docs failure, end while it runs |
| Review (docs-review flow) | Starts when the MR pipeline finishes. On failed docs jobs of a docs-write MR it only mentions docs-write to fix CI. On green, One fresh agent with only the `docs-review` skill reads the changed pages, the docs diff, and the feature MR diff and code, and checks claims, missing steps, brevity, the word list, topic type, and links. A script posts each finding at importance 4 or more, and each blocker, as a thread on its line: a suggestion block for a small exact fix, a **Needs reviewer decision** thread for a question. Then one summary note per head, which mentions docs-write on its docs MRs. It never pushes. | Yes | A new docs-write run |
| Threads | Reads the docs-review summary on the head, then lists the open threads from trusted authors | No | A fixer, handoff, or end while no review exists |
| Fix | A fresh agent per cycle, four at most, counted from the flow's pushes on the docs branch. It fixes check or CI findings, or the open threads, review threads included, in one pass. | Yes | Publish, which posts one reply per thread |
| Handoff | When CI is green on the head, the head has a review, and the only open threads are decisions or threads the flow does not take: removes `Draft:`, adds the feature MR author and the docs reviewers as reviewers, and posts one note. The note lists the decision threads and threads the flow did not take. | No | End. Otherwise the MR stays in draft, with the reason in its description. |

The gate ends the run with no model call on a draft feature MR or one that scores below 6.
It also skips a fork MR, an MR with the `documentation` or `docs-factory::skip` label, and
an MR that already changes user docs.
A mention on a feature MR runs the same gate.

Any step that fails to write its route file ends the flow. When in doubt, the flow does
nothing. The flow file ships with `MODE=shadow`, which stops after setup and logs one line per MR.

## What a person does

| Role | What to do |
|---|---|
| Feature MR author | Review the docs MR when the flow requests your review. Settle the **Needs reviewer decision** threads. To skip the flow, add `docs-factory::skip`. |
| Docs reviewer (`DOCS_REVIEWERS` in the flow file: Fiona Neill and Zachary Painter) | Approve and merge the docs MR by hand. Read the docs-review threads and the flow's replies. Check the "What to check" lines in the description, and each claim against the feature code. Add the feature MR as a dependency, then merge after it. |
| Anyone | Mention the flow's service account on a feature MR to run the gate again. Mention it on a docs MR from the flow to fix the open threads. It fixes only threads from the people and bots listed in [Guardrails](#guardrails). |

## Guardrails

Script steps enforce these rules:

- The scripts open docs MRs as drafts and never merge or approve.
- The scripts push only when `check.py` reports no errors and the branch changes nothing but
  user docs. Warnings left after the fix cycles are listed on the MR.
- The flow hands agents only threads and notes from trusted authors: the flow's identity,
  the docs-review flow's identity, Duo Code Review, and docs MR reviewers. Authors and
  reviewers of feature MRs from this project and Developer+ members are trusted too. Other
  threads stay open for the person who merges. The handoff note links them without their text.
- The gate skips fork MRs, and the related-MR check groups only MRs from this project. A fork
  MR ends at the gate and never joins a group, so its diff never reaches the writer.
- The flow resolves only the threads it gave the fixer, and only after the fix is pushed and
  `check.py` is clean. It never resolves a **Needs reviewer decision** thread.
- The threads path runs only on a docs MR with a branch in this project, not a fork. The
  flow's identity must already have a note on it.
  Flow markers count only on notes from the flow's identity. GitLab Duo counts only for the
  run's start note.
- The docs-review flow discards any commit or edit the review agent makes outside
  `.docs-write/`, and never pushes.

Prompts set these rules, and the script steps check the result where they can:

- No agent gets a GitLab write tool, but each has a shell with the run's token. The prompts
  forbid pushes and API writes. A prompt is the only barrier between the text an agent reads
  and that shell. The intake filter limits who can write the thread text an agent reads.
  Feature MR diffs and code reach the agents unfiltered, and the checks cannot undo an API
  write or a push to another branch.
- The reviewer is told not to read the writer's notes in `.docs-write/`. Nothing blocks the read.

Other rules:

- The docs change gets its own MR, as this project's `AGENTS.md` requires.
- A run acts as the flow's service account together with the person who triggered it. When
  that person cannot read the private skills project, the run ends with no docs MR.
- The docs MRs it opens start the flow again, and the gate ends those runs before any model call.

## Where things live

| What | Where |
|---|---|
| Flow definition | [`.gitlab/duo/catalog/flows/docs-write.yml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/main/.gitlab/duo/catalog/flows) in this project, with setup steps in its README. The file is the reviewable copy. The AI Catalog item is what runs, so after a change to the file, update the item from it. |
| Skills and scripts | `docs-write` (writer and fixers) and `docs-review` (reviewer) in [`gitlab-org/orbit/team/skills`](https://gitlab.com/gitlab-org/orbit/team/skills) (team members only). `docs-review` also works by hand on any docs MR. |
| Evaluations and benchmarks | [`gitlab-org/orbit/team/product/orbit-docs-evals`](https://gitlab.com/gitlab-org/orbit/team/product/orbit-docs-evals) (team members only) |
| Test sandbox | A private fork, [`knowledge-graph-docs-sandbox`](https://gitlab.com/gitlab-org/orbit/team/knowledge-graph-docs-sandbox) |

## Why design v2

Round 2 is tasks R1 to R4 on [#1494](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494).
It benchmarked each choice before the build
([decision](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4229149663)).
R4 then reversed two points of that decision. It kept two skills, one per agent, and posts
one review comment instead of a thread per finding
([R4 results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4231244021)).
Later, the review moved to its own flow and posts a thread per finding again. It posts only
importance 4 and up, because fixers that followed wording nits made docs worse.
The benchmarks found:

- **Writer** ([results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4229141228)):
  short instructions with the `check.py` loop matched the old writer on facts, within noise.
  It wrote 48% fewer words, left no check findings, and cost 22% less.
- **Review** ([results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4229040208)):
  one fresh agent in one context tied the old review as Duo can run it, at half the cost.
- **No review depth classifier** ([results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4228292674)):
  at threshold 6, it routed 34 of 35 changes to the full review. Its reasons became the
  "What to check" lines instead.

## How it was tested

**Live, in the sandbox fork** ([results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4231244021)). Each case re-created a past feature MR on
the commit it merged into:

| Case | Trigger | What happened |
|---|---|---|
| Shadow mode | MR created | The gate scored the change `small` and the flow logged one line. No model call, no write. |
| Small CLI change | MR created | The review found 6 items and the script applied 1. One fixer fixed the other 5 and the flow replied once. CI went green, the MR was marked ready, and the flow requested review from the feature author. |
| Large setup change | MR created | The review found 2 items. One fixer fixed both in one pass. CI went green, the MR was marked ready, and the flow requested review from the feature author. |
| CI docs failure | MR marked ready | An injected sandbox docs rule failed the pipeline. A fixer fixed it, CI went green, and the flow handed off. |
| Decision finding | MR created | An injected code comment left a product question open. The review raised it as a **Needs reviewer decision** thread. The flow fixed the 2 other items and handed off with the thread open and listed. |
| Threads from others | A mention on the docs MR | Duo Code Review posted 5 threads and a person posted 1. A fixer fixed the 5, and Duo posted 1 more after the push. A second fixer fixed it. The person's product question stayed open as a decision. |

The first build posted a thread per finding. It also tested the no-docs exit: the writer
found nothing a user needs and ended the run with no docs MR.

Every flow-made docs MR started the flow again, on **Created** and on **Marked ready**. Each
of those runs ended at the gate with no model call. Each event started one session.
The flow's service account could post comments, resolve threads, mark MRs ready, and add
reviewers.

**Ready-to-merge grading**: blind Opus graders judged the v2 docs MRs, the v1 docs MRs, and
the human docs for two features. They did not know which was which.

| Docs change | Verdicts | Blocking scores | Side by side |
|---|---|---|---|
| v2, CLI change | needs changes, twice | 0 | Lost to v1. Lost to the human change. |
| v2, setup change | needs changes, twice | 0 | Lost to v1. Beat the human change. |
| v1, CLI change | needs changes, twice | 0 | Beat the human change, slightly |
| v1, setup change | needs changes, twice | 0 | Beat the human change |
| Human, CLI change | needs changes | 0 | |
| Human, setup change | needs changes | 2 | |

- The graders ask for edits on every item, human ones included, so the verdict alone does not
  separate them. No v2 item has a blocking score.
- v2 is shorter and covers less than v1. The graders preferred v1 for coverage, such as
  first-use indexing for the MCP tools, not for wrong claims.
- The v2 test branches start where each feature merged, so the code matches the docs tree.
  The v1 branches started from a newer default branch.

## Known limits

- The writer covers fewer details than the v1 writer. Next: point the review at missing coverage.
- The review rarely raises a decision on its own. In 10 reviews of real changes, it raised none.
- One review context finds less than separate reviewer contexts: recall is 0.12 to 0.14 lower,
  mostly on brevity and steps
  ([results](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1494#note_4229040208)).
  Duo flows cannot run subagents.
- The gate misses changes whose user impact is not in a CLI, MCP, or query surface.
- The flow cannot set MR dependencies.

## Rollout

1. Shadow mode: the gate and related steps run and log their decision. No model call, no writes.
1. Advisory: a note on the feature MR with the pages and the size.
1. Draft docs MRs, with review requested from the feature MR author.

Each stage starts only after the previous one holds up against what technical writers decide.
