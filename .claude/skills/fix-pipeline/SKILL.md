---
name: fix-pipeline
description: |
  Monitor the latest CI pipeline for the current branch's MR, diagnose
  failures, apply fixes, push, and repeat until the pipeline goes green.
  Asks the user before making large or risky changes.
allowed-tools:
  - Bash
  - Read
  - Edit
  - Write
  - Grep
  - Glob
  - AskUserQuestion
---

# Fix pipeline

CI pipeline repair loop. Check status, read logs, fix, push, repeat until green.

## Procedure

1. **Find the MR** — `glab mr view --web=false`. No MR? Stop.

2. **Poll the pipeline** — several options:
   - `glab ci status` / `glab ci status --wait`
   - `glab api projects/:id/merge_requests/:iid/pipelines` to list pipelines for an MR
   - `glab api projects/:id/pipelines/:pipeline_id` to check a specific pipeline's status
   - `glab api projects/:id/pipelines/:pipeline_id/jobs` to list all jobs and their statuses

   Be smart about waiting. Don't poll the whole pipeline if you only care
   about specific jobs. Use the jobs endpoint to check individual job
   statuses. If a job you previously fixed is still running while other
   jobs have already passed, just wait on that one. If a job with
   `allow_failure: true` fails, ignore it and move on. Only react to jobs
   whose failure blocks the pipeline.

3. **Diagnose failures** — `glab ci trace <job-id>` or `glab api projects/:id/jobs/:job_id/trace` for the raw log. Read the last 80 lines.

4. **Classify the fix**

   Auto-fix (no approval needed):
   - Formatting (`mise run fmt` / `cargo fmt --all`), lint fixes (`mise run lint:fix` / `cargo clippy --fix`)
   - Markdown lint issues (`mise run lint:markdown`), Vale prose style violations (`mise run lint:vale`)
   - Broken links flagged by lychee (`mise run lint:links`)
   - Missing config files, typos in CI config
   - Test assertion updates where new behavior is obviously correct
   - Patch/minor dependency bumps
   - AGENTS.md / CLAUDE.md sync issues (these files must be identical)

   Ask first:
   - Public API or behavior changes
   - Adding/removing dependencies
   - Business logic changes beyond trivial fixes
   - CI job config changes (adding/removing jobs, runners)
   - Helm chart changes
   - Touches more than 3 files
   - Ambiguous root cause
   - Security config changes (`deny.toml` ignores, etc.)

   When asking: state the failure, your proposed fix, and alternatives.

5. **Fix it** — smallest possible change. Verify locally first:
   - Rust: `cargo fmt --all --check`, `cargo clippy --workspace`, `cargo nextest run`
   - Docs: `mise run lint:markdown`, `mise run lint:vale`
   - Helm: `helm lint helm-dev/charts/*`
   - If AGENTS.md or CLAUDE.md changed, copy the edited file to the other (`cp CLAUDE.md AGENTS.md` or vice versa)

6. **Commit and push** — use conventional commit format: `fix: <what you fixed>`. No force-push unless rebase is the fix.

7. **Loop** — back to step 2. Stop when:
   - All jobs pass
   - A failure is beyond your ability to fix (explain and stop)
   - 5 attempts without progress (summarize and stop)
   - Same job fails twice with the same error (ask the user)

8. **Report** — `glab ci view --web` when green. Summarize what you fixed.

## Rules

- Never skip CI checks (`--no-verify`) unless told to.
- Never change test assertions without understanding the failure. If the old assertion looks correct, ask.
- Track what you've fixed across iterations. Summarize at the end.
