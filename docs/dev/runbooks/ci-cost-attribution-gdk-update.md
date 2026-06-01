# Runbook: gdk-update CI Job — Retry and Cache Optimization

## Summary

The `gdk-update` job is the second-largest CI failure hot spot in
`gitlab-org/*`, with approximately **20,955 failures across 8 projects** in
the 60-day window 2026-03-08 → 2026-05-08. The dominant failure driver is
Bundler resolution against a changing `Gemfile.lock` and database schema
migration churn in `db/structure.sql`.

## Affected projects

Primarily `gitlab-org/gitlab` (the bulk of failures), plus 7 additional
projects that include the same `gdk-update` job via shared CI configuration.

## Root cause: file fingerprints driving failures

The following files, when present in an MR diff, most reliably correlate with
a `gdk-update` re-failure:

**Bundler chain (>50% of sampled MRs):**
- `Gemfile`, `Gemfile.lock`, `Gemfile.checksum`
- `Gemfile.next.lock`, `Gemfile.next.checksum`

**DB schema chain:**
- `db/structure.sql`
- `db/schema_migrations/<timestamp>`
- `db/migrate/<timestamp>_*.rb`
- `db/post_migrate/<timestamp>_*.rb`

**Vendored gems:**
- `vendor/gems/<gem>/...` (e.g. `vendor/gems/nsfw-rb/*`)

**Pipeline config and queues:**
- `.gitlab-ci.yml`
- `app/workers/all_queues.yml`, `config/sidekiq_queues.yml`

**Feature flag YAMLs:**
- `config/feature_flags/**/*.yml`
- `ee/config/feature_flags/**/*.yml`

## Recommended actions

### 1. Add retry to the gdk-update job

In `.gitlab/ci/setup.gitlab-ci.yml` (or wherever `gdk-update` is defined):

```yaml
gdk-update:
  retry:
    max: 2
    when:
      - runner_system_failure
      - stuck_or_timeout_failure
      - script_failure
```

This alone eliminates transient network failures from the failure count.

### 2. Cache Bundler resolution

```yaml
gdk-update:
  cache:
    key:
      files:
        - Gemfile.lock
    paths:
      - vendor/bundle
    policy: pull-push
```

When `Gemfile.lock` hasn't changed, the cache hit avoids a full `bundle
install` and cuts the job runtime significantly.

### 3. Pre-flight Bundler check

Add a pre-flight step that checks whether the Gemfile has actually changed
relative to the GDK's current state before running the full update:

```bash
# Skip full update if Gemfile.lock is unchanged
if git diff --name-only HEAD~1 | grep -qE 'Gemfile|Gemfile\.lock'; then
  gdk update
else
  echo "No Gemfile changes detected, skipping full gdk-update"
  gdk update --skip-bundle-install
fi
```

### 4. Scope `gdk-update` to relevant file changes

Use GitLab CI `rules:changes` to skip the job entirely when none of the
trigger files are in the MR diff:

```yaml
gdk-update:
  rules:
    - changes:
        - Gemfile
        - Gemfile.lock
        - Gemfile.checksum
        - Gemfile.next.lock
        - Gemfile.next.checksum
        - db/structure.sql
        - db/schema_migrations/**/*
        - db/migrate/**/*
        - db/post_migrate/**/*
        - .gitlab-ci.yml
        - app/workers/all_queues.yml
        - config/sidekiq_queues.yml
        - config/feature_flags/**/*
        - ee/config/feature_flags/**/*
        - vendor/gems/**/*
```

This is the highest-impact change: MRs that don't touch any of these files
skip `gdk-update` entirely, eliminating the bulk of the ~21k failures.

## Verification

After deploying the `rules:changes` scoping, monitor the `gdk-update` failure
rate for 14 days. Expected outcome: >70% reduction in total failures (the
majority of MRs in `gitlab-org/gitlab` do not touch Bundler or DB schema
files).

## Sampled failing MRs (for reference)

!221476, !222735, !221475, !226481, !225182, !226179, !226788, !229605,
!232445, !232943, !233286, !233498, !231149, !232091 (all in gitlab-org/gitlab)

## Related

- Issue: `gitlab-org/orbit/knowledge-graph#783`
- CI config: `.gitlab/ci/setup.gitlab-ci.yml` in `gitlab-org/gitlab`
- CI cost-attribution analysis: see issue #783 for full methodology and data
