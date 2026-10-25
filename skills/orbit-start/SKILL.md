---
name: orbit-start
description: Walk a user through their first GitLab Orbit session. Check the setup, answer one or two first questions with Orbit, then offer to answer one without Orbit and with Orbit, side by side. Use when the user asks to get started with Orbit, try Orbit, or see what Orbit can do.
version: 0.1.0
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab) and network access to the GitLab instance. Works best in agents that can ask the user multiple-choice questions.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, onboarding, getting-started, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-start
---

# Get started with GitLab Orbit

Guide the user through one or two real questions about their own GitLab data, answered with GitLab Orbit.
Then offer to answer one question twice, once without Orbit and once with it, and show the raw numbers side by side.

Keep the session short and read-only. Do not create issues, merge requests, comments, or commits.

## Ask questions the right way

When this skill asks the user something, use your interactive question tool if you have one, for example
`AskUserQuestion` in Claude Code. Offer two to four options, and put the recommended option first.
If you have no such tool, print the options as a numbered list, then stop and wait for the answer.

## Step 1: Check the setup

1. Run `glab orbit status`.
   If it fails, run `glab orbit skills get orbit references/troubleshooting.md`, follow the first-run setup,
   and run `glab orbit status` again. If it still fails, tell the user what failed and stop.
1. Find the scope. In a clone, read the project from `git remote get-url origin`.
   Otherwise, ask the user for the full path of a group or project that has GitLab Orbit turned on.
1. Load the Orbit skill with `glab orbit skills get orbit`, and follow its query rules for the rest of the session.

## Step 2: Pick a question

Ask the user what they want to see. Offer these options:

- **What a merge request affects**: the callers of the changed code outside the merge request,
  and the projects that import it.
- **Where a vulnerability is**: which projects a CVE or open critical vulnerabilities affect,
  and whether a fix reached production.
- **Why a pipeline fails**: whether a failing job also fails on the default branch, and which open
  merge requests it blocks.
- **What depends on code you want to change**: the subclasses and callers of a class or method.

Then ask only for the inputs that the question needs and that you cannot find yourself.
For example, ask for a merge request number, a CVE ID, a job name, or a class name.
Offer real values from the project as options when you can, such as recent merge requests or recently failed jobs.

## Step 3: Answer with Orbit

1. Run each query with `glab orbit query`.
1. Give the answer, and list each finding with the query result that shows it.
1. Show the queries that you ran, so the user can run them again.

Then ask the user what to do next. Offer these options:

- **Ask another question**: go back to step 2. Offer this only after the first question.
- **Compare with and without Orbit**: go to step 4.
- **Done**: go to step 5.

## Step 4: Compare with and without Orbit

Ask which question to compare: one that you already answered, or a new one from step 2.

Answer the question twice, once without Orbit and once with Orbit, so that both answers are measured the same way.
If you can start subagents, for example with the Agent tool in Claude Code, start both in parallel in one turn.
Give each subagent the question, the scope, the inputs, and its rules from below.
If you cannot start subagents, answer without Orbit first, then with Orbit.

Without Orbit:

- Use only the tools that the user had before Orbit.
  These are `glab` commands, the REST API through `glab api`, and `git` and file reads in the local clone.
- Do not use `glab orbit`, the GitLab Orbit MCP endpoint, or any other Orbit tool.
- Stop after 15 tool calls or 3 minutes, whichever comes first, and keep what you have.

With Orbit:

- Load the Orbit skill with `glab orbit skills get orbit`, and follow its query rules.
- Run each query with `glab orbit query`.

Each answer reports the answer, the number of tool calls and API requests, and the time.
It also lists what it could not find, and what it had to guess.

Show the raw comparison:

| | Without Orbit | With Orbit |
|---|---|---|
| Tool calls and API requests | | |
| Time | | |
| What the answer covers | | |
| Could not find | | |
| Had to guess | | |

In **What the answer covers**, give counts that the user can check, such as projects, files, callers,
or merge requests. Then show both answers in full, one after the other.

Do not add a verdict, a score, or a summary of which answer is better. Let the user compare.

## Step 5: Offer next steps

Ask the user what to do next. Offer these options:

- Ask another question: go back to step 2.
- Set up Orbit for every session: run `glab orbit setup`.
- See more prompts: <https://docs.gitlab.com/orbit/>.
