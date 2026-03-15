---
name: start-issue
description: Pick an issue to work on and set up a clean branch for it. If no issue number is provided, searches for the next issue to work on and presents options with a recommendation.
allowed-tools: Bash(git *), Bash(glab *), Grep, Glob, Read
---

# Start Issue

Pick an issue and prepare a clean branch to work on it.

## Steps

1. Check for uncommitted changes: `git status --short`. If the working tree is dirty, warn the user and ask whether to continue.
2. Fetch latest main: `git fetch origin main`
3. Determine the issue to work on:
   - **If an issue number was provided:** look it up: `glab issue view <number>`
   - **If no issue was provided:** find the next one to work on (see "Finding the next issue" below). Present candidates to the user and ask which to take.
4. Summarise the issue for the user: what the problem is, what the solution requires, and any constraints or linked issues (see "Issue summary" below).
5. Ask the user for a branch name if they haven't chosen one, suggesting one based on the issue title and number.
6. Create and switch to the new branch from `origin/main`: `git checkout -b <branch-name> origin/main`
7. Confirm the branch is up to date with main and report the commit it's based on: `git log -1 --oneline`
8. Plan the fix (see "Planning the fix" below).

## Finding the next issue

List open, unassigned issues:

```bash
glab issue list --assignee=""
```

Present the top candidates with title, labels, and URL. Include your recommendation and reasoning (e.g. highest priority, smallest scope, good first step). Ask the user which one to take.

## Issue summary

After the issue is selected (either provided or chosen from the list), present a concise summary before any planning or branching:

- **Problem:** what is broken or missing and why it matters
- **Solution:** what the issue proposes to fix or build
- **Constraints:** any linked issues, MRs, dependencies, or out-of-scope notes

## Planning the fix

After the branch is set up, explore the codebase to produce an implementation plan before writing any code. Use the Plan subagent if the task is non-trivial.

1. Identify the affected crates and files using `Grep`, `Glob`, and `Read`.
2. Draft a plan covering:
   - Root cause or gap being addressed
   - Files/crates that need to change
   - Ordered implementation steps
   - Tests to add or update
   - How to verify the fix works (e.g. specific test commands, manual steps, query fixtures to run)
3. Present the plan to the user and get sign-off before proceeding.

## Branch naming

Follow the project convention: `type/short-description` (e.g., `fix/258-webserver-health-probes`, `feat/316-add-graph-export`). Use the issue number when available.

## Self-improvement

If you encounter a `glab` flag that doesn't work, remove it from this skill. If the issue discovery process needed adjusting (different labels, different fallback), update the commands. If a step was missing or confusing, fix it here.
