# Runbook: Gate test-storybook and compile-test-assets vue3 on Change Scope

## Summary

The `test-storybook` and Vue3 compile/lint family of jobs are the third and
fourth largest CI failure hot spots in `gitlab-org/*`, collectively accounting
for **~35,000+ failures across 15+ projects** in the 60-day window
2026-03-08 → 2026-05-08.

| Job family | Failures (60d) | Projects |
|---|---|---|
| `test-storybook` | ~14,729 | 6 |
| `compile-test-assets vue3` + `jest-build-cache-vue3-ensure-compilable-sfcs` + `lint:markdown` + `lint:docs-freshness` + `docs-lint markdown` | ~21,030 | 15 |

Both originate in shared frontend CI templates
(`.gitlab/ci/frontend.gitlab-ci.yml`, `.gitlab/ci/docs.gitlab-ci.yml`) and
can be scoped to run only when relevant files change.

## Root cause: file fingerprints driving test-storybook failures

The following file trees, when present in an MR diff, most reliably correlate
with a `test-storybook` re-failure:

- `locale/gitlab.pot` (recurs in every i18n-touching MR)
- `ee/app/assets/javascripts/security_orchestration/**/*.vue` (dominant)
- `ee/app/assets/javascripts/analytics/**/*.vue`
- `app/assets/javascripts/**/*.vue`
- `**/__snapshots__/*.snap`, `spec/frontend/**/*.js`, `ee/spec/frontend/**/*.js`
- `package.json`, `yarn.lock`, `eslint.config.mjs`, `.eslint_todo/*.mjs`
- `app/assets/javascripts/**/*.graphql`, `public/-/graphql/introspection_result.json`

The EE security-orchestration and analytics trees are the dominant hot spot:
MR !226828 alone touches 300+ Vue files and failed `test-storybook` repeatedly.

## Recommended actions

### 1. Gate test-storybook on frontend file changes

In `.gitlab/ci/frontend.gitlab-ci.yml`:

```yaml
test-storybook:
  rules:
    - changes:
        - "app/assets/javascripts/**/*.vue"
        - "ee/app/assets/javascripts/**/*.vue"
        - "app/assets/javascripts/**/*.js"
        - "ee/app/assets/javascripts/**/*.js"
        - "**/__snapshots__/*.snap"
        - "spec/frontend/**/*.js"
        - "ee/spec/frontend/**/*.js"
        - "package.json"
        - "yarn.lock"
        - "locale/gitlab.pot"
        - "app/assets/javascripts/**/*.graphql"
        - "public/-/graphql/introspection_result.json"
```

### 2. Gate compile-test-assets vue3 on frontend file changes

Apply the same `rules:changes` pattern to:
- `compile-test-assets vue3`
- `jest-build-cache-vue3-ensure-compilable-sfcs`

These jobs are expensive (multi-minute compile steps) and should not run for
backend-only MRs.

### 3. Gate docs lint jobs on docs file changes

For `lint:markdown`, `lint:docs-freshness`, `docs-lint markdown`:

```yaml
lint:markdown:
  rules:
    - changes:
        - "doc/**/*"
        - "**/*.md"
        - ".markdownlint.yml"
        - ".vale.ini"
        - "vale/**/*"
```

### 4. Prioritize the EE security-orchestration tree

The `ee/app/assets/javascripts/security_orchestration/**` and
`ee/app/assets/javascripts/analytics/**` trees are the single largest
contributors to `test-storybook` failures. Consider:

- Adding Storybook story coverage requirements for these trees so snapshot
  drift is caught earlier (pre-commit or in a lighter job)
- Running a dedicated `test-storybook:security-orchestration` job scoped
  only to that tree, separate from the full Storybook run

### 5. Propagate to the 15-project template

The Vue3 compile/lint family affects 15 projects via a shared template. Once
the `rules:changes` fix is validated in `gitlab-org/gitlab`, propagate it to
the shared template so all 15 projects benefit simultaneously.

## Verification

After deploying `rules:changes` scoping:
- Monitor `test-storybook` failure rate for 14 days
- Expected: >60% reduction (most MRs in `gitlab-org/gitlab` are backend-only
  and do not touch Vue files)
- Cross-check with the MR Pipelines tab for a sample of backend MRs to
  confirm the job is being skipped

## Sampled failing MRs (for reference)

!223475, !226035, !226255, !225553, !226547, !225762, !226575, !226828
(all in gitlab-org/gitlab; !226828 is the EE security-orchestration MR
touching 300+ Vue files)

## Related

- Issue: `gitlab-org/orbit/knowledge-graph#783`
- CI config: `.gitlab/ci/frontend.gitlab-ci.yml`, `.gitlab/ci/docs.gitlab-ci.yml`
- CI cost-attribution analysis: see issue #783 for full methodology and data
