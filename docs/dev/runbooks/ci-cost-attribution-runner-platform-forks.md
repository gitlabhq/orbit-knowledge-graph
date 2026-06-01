# Runbook: Runner-Platform GitLab Forks — Pipeline Scope Reduction

## Summary

Five `prod-engineering runners-platform` forks of `gitlab-org/gitlab` are
running **full GitLab pipelines** — including expensive jobs like
`compile-production-assets` and `gdk-update` — despite their purpose being
runner-platform validation, not full GitLab development. This generates an
estimated **>10,000 recurrent pipeline failures** in 60 days that are
unrelated to actual GitLab development work.

## Affected forks

| Fork | Alias |
|---|---|
| `gitlab-org-forks/gl-vn` | `gl-vn` |
| `gitlab-org-forks/gl-kf` | `gl-kf` |
| `gitlab-org-forks/gitlab-org-forks-gvisor` | `gvisor` |
| `gitlab-org-forks/gitlab-org-forks-kata` | `kata` |
| `gitlab-org-forks/gl-gv` | `gl-gv` |

## Failure volume (60-day window)

| Job | Failures across 5 forks |
|---|---|
| `compile-production-assets` | ~4,562 |
| `gdk-update` | ~3,400 |
| Other full-pipeline jobs | ~2,000+ |

These failures are not actionable for the runner-platform team — they are
artifacts of running a full GitLab pipeline against code that is only
minimally modified from upstream.

## Recommended actions

### 1. Scope pipeline triggers to runner-relevant jobs only

In each fork's `.gitlab-ci.yml` (or the shared include), add a top-level
`workflow:rules` block that restricts which jobs run:

```yaml
workflow:
  rules:
    # Only run runner-platform-relevant jobs
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
      variables:
        PIPELINE_PROFILE: runner-platform
    - if: '$CI_PIPELINE_SOURCE == "push" && $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH'
      variables:
        PIPELINE_PROFILE: runner-platform

# Then gate expensive jobs:
compile-production-assets:
  rules:
    - if: '$PIPELINE_PROFILE != "runner-platform"'

gdk-update:
  rules:
    - if: '$PIPELINE_PROFILE != "runner-platform"'
```

### 2. Use a dedicated slim CI configuration

Create a `.gitlab/ci/runner-platform.gitlab-ci.yml` that includes only the
jobs relevant to runner-platform validation (e.g. runner integration tests,
executor smoke tests) and have the forks include that instead of the full
pipeline.

### 3. Reduce pipeline trigger frequency

If the forks are mirroring upstream `main` continuously, consider:
- Triggering pipelines only on explicit MRs (not every push to `main`)
- Using scheduled pipelines at a lower frequency (e.g. nightly instead of
  per-commit)

## Verification

After scoping the pipeline, monitor the failure counts for the 5 forks over
14 days. Expected outcome: elimination of `compile-production-assets` and
`gdk-update` failures from these forks (estimated >7,000 failures/60d
eliminated).

## Related

- Issue: `gitlab-org/orbit/knowledge-graph#783`
- CI cost-attribution analysis: see issue #783 for full methodology and data
- Affected forks: `gl-vn`, `gl-kf`, `gvisor`, `kata`, `gl-gv` under
  `gitlab-org-forks/`
