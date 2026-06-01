# Runbook: Danger-Review CI Template Audit

## Summary

The `danger-review` job is the single largest cross-project CI failure hot spot
in `gitlab-org/*`, failing in **25+ distinct projects** over the 60-day window
2026-03-08 → 2026-05-08, with approximately **6,150+ total failures**.

Because the failure originates in a shared CI template
(`Danger-Review.gitlab-ci.yml` via `gitlab-org/danger-review`), a single fix
propagates across all 25 projects simultaneously — making this the highest-ROI
intervention in the CI cost-attribution analysis.

## Affected projects (sample)

gitlab, terraform-provider-gitlab, gitaly, AI Gateway, client-go, Pajamas,
gitlab-runner, customers-gitlab-com, gitlab-agent (KAS), renovate-gitlab-bot,
triage-ops, gitlab-lsp, Duo UI, GLQL, omnibus-gitlab, ci-alerts,
release-tools, dx-infrastructure, jetbrains-plugin, secrets-manager-container,
security/gitlab, GDK, GitLab Advanced SAST, license-exporter, cli, GitLab
Docs, Dependency Scanning, Charts, Updater, Zoekt, ES indexer, semgrep,
gemnasium, BootstrapVue Component Usage.

## Root cause areas to investigate

1. **Base image staleness** — the `danger-review` job pulls a Ruby image that
   may have diverged from the Gemfile constraints in the template. Check
   whether the pinned image digest in `Danger-Review.gitlab-ci.yml` matches
   the current `danger` gem requirements.

2. **Network/registry flakiness** — `danger-review` fetches gems at runtime.
   If the upstream registry is intermittently unavailable, the job fails with
   a non-retryable exit code. Add `retry: 2` to the template job definition.

3. **Dangerfile rule drift** — rules that reference CI variables or API
   endpoints that have changed since the template was last updated will fail
   silently or with cryptic errors. Audit `Dangerfile` against the current
   GitLab API surface.

4. **Token scope** — `danger-review` requires a `DANGER_GITLAB_API_TOKEN`
   with `api` scope. Projects that rotated tokens without updating the CI
   variable will fail every run.

## Recommended actions

```yaml
# In Danger-Review.gitlab-ci.yml — add retry and a timeout guard
danger-review:
  retry: 2
  timeout: 10 minutes
  # Pin the image to a digest that matches current danger gem constraints
  image: ruby:3.2-slim@sha256:<verified-digest>
```

1. Open an MR against `gitlab-org/components/danger-review` to add `retry: 2`
   and a `timeout` guard to the template job.
2. Audit `Dangerfile` rules that call `gitlab.api` — replace any deprecated
   endpoint calls.
3. Verify `DANGER_GITLAB_API_TOKEN` is set and scoped correctly in the 25+
   affected projects (use the GitLab Admin → CI/CD → Group variables panel).
4. Pin the base image to a verified digest and set up Renovate to keep it
   current.

## Verification

After deploying the template change, monitor the `danger-review` failure rate
across the affected projects for 7 days. A successful fix should reduce the
cross-project failure count from ~6,150/60d to near zero for
infrastructure-caused failures (token issues, network flakes, image drift).

## Related

- Issue: `gitlab-org/orbit/knowledge-graph#783`
- Template source: `gitlab-org/components/danger-review`
- CI cost-attribution analysis: see issue #783 for full methodology and data
