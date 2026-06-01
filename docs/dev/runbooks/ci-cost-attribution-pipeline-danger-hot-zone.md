# Runbook: Ci::Pipeline / Ci::Build Danger Hot Zone Guidance

## Summary

The `Ci::Pipeline`, `Ci::Build`, `CommitStatus`, and related CI model files
in `gitlab-org/gitlab` are the most-edited files in MRs that repeatedly
re-fail `danger-review`. Co-locating guidance for refactors touching these
files will reduce the number of MRs that trigger repeated full-suite re-runs
due to Danger rule violations.

This is the fifth recommended fix from the CI cost-attribution analysis
(issue #783), complementing the Danger-Review template fix (MR !1491).

## The hot zone: files and definitions

The following files appear in the diff fingerprint of MRs that most reliably
re-fail `danger-review`:

### Core CI model files

| File | Key definitions |
|---|---|
| `app/models/ci/pipeline.rb` | `latest_successful_for_ref`, `latest_running_for_ref`, `latest_failed_for_ref`, `parse_transition_args`, `git_commit_message`, `archived?`, `cancelable?`, `coverage`, `builds_with_cte`, `freeze_period?`, `set_status`, `variables_builder`, `persisted_variables`, `related_merge_requests`, `builds_in_self_and_project_descendants`, `find_job_with_archive_artifacts`, `latest_builds_with_artifacts`, `has_archive_artifacts?`, `has_erasable_artifacts?`, `top_level_worktree_paths`, `default_branch?`, `merge_request?`, `full_error_messages`, `merge_request_event_type`, `dangling?`, `auto_cancel_on_job_failure`, `add_message` |
| `app/models/ci/build.rb` | Build lifecycle, artifact management, runner assignment |
| `app/models/commit_status.rb` | Status transition state machine |
| `app/models/ci/job_artifact.rb` | Artifact lifecycle |
| `app/models/ci/build_trace_chunk.rb` | Trace streaming |
| `app/models/ci/pipeline_variable.rb` | Variable resolution |
| `app/models/ci/processable.rb` | Shared processable behavior |
| `app/models/ci/pending_build.rb`, `app/models/ci/running_build.rb` | Queue management |

### ActiveRecord patches

- `gems/activerecord-gitlab/lib/active_record/gitlab_patches/**/*.rb`

### Sampled MRs that re-failed danger-review on these files

!226895, !226896, !226898, !226899, !226901, !226905 (all in gitlab-org/gitlab)

## Why these files trigger Danger

The `danger-review` Dangerfile has rules that flag:

1. **CI model changes without a corresponding test change** — any MR touching
   `app/models/ci/*.rb` without a matching `spec/models/ci/*.rb` change
   triggers a Danger warning.
2. **Database migration without a schema migration** — changes to
   `db/migrate/*.rb` without a corresponding `db/schema_migrations/*` entry.
3. **Status transition changes** — changes to `set_status`, `parse_transition_args`,
   or the state machine in `commit_status.rb` trigger a Danger rule requiring
   a changelog entry.
4. **Artifact lifecycle changes** — changes to `has_archive_artifacts?`,
   `has_erasable_artifacts?`, or `job_artifact.rb` trigger a Danger rule
   requiring a security review label.

## Recommended actions

### 1. Add a CODEOWNERS entry for the hot zone

In `.gitlab/CODEOWNERS` (or equivalent), add:

```
app/models/ci/pipeline.rb @gitlab-org/ci-platform
app/models/ci/build.rb @gitlab-org/ci-platform
app/models/commit_status.rb @gitlab-org/ci-platform
app/models/ci/job_artifact.rb @gitlab-org/ci-platform
```

This ensures that MRs touching these files automatically request review from
the CI Platform team, who can pre-validate Danger compliance before the job
runs.

### 2. Add a pre-MR checklist to the CI model files

Add a comment block at the top of `pipeline.rb` and `build.rb`:

```ruby
# CONTRIBUTOR CHECKLIST — changes to this file often trigger Danger rules:
# 1. Add/update spec/models/ci/pipeline_spec.rb for any new methods
# 2. Add a changelog entry if changing status transitions (set_status, etc.)
# 3. Add the ~"security" label if changing artifact lifecycle methods
# 4. Run `bundle exec danger local` before pushing to catch Danger failures early
```

### 3. Document the Danger rules that apply to CI models

Create or update `doc/development/dangerbot.md` with a section listing the
specific Danger rules that apply to CI model changes, so contributors know
what to expect before pushing.

### 4. Add `danger local` to the GDK pre-push hook

For contributors working on CI models, add `bundle exec danger local` to the
GDK pre-push hook so Danger failures are caught locally before triggering a
CI run.

## Verification

After adding the CODEOWNERS entry and checklist comment:
- Monitor `danger-review` failures on MRs touching `app/models/ci/pipeline.rb`
  for 30 days
- Expected: reduction in re-runs caused by Danger rule violations (contributors
  will be aware of the rules before pushing)

## Related

- Issue: `gitlab-org/orbit/knowledge-graph#783`
- MR !1491: Danger-Review CI template audit (the template-level fix)
- Dangerfile: `Dangerfile` in `gitlab-org/gitlab`
- CI cost-attribution analysis: see issue #783 for full methodology and data
