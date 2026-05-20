---
title: "GKG ADR 013: SOX Scoping for Billing Event Emission"
creation-date: "2026-05-15"
last-updated: "2026-05-15"
authors: [ "snachnolkar" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-05-15

## Context

[MR !937](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937) added Snowplow billing event emission to GKG's `ExecuteQuery` path. Every successful query now emits an `orbit_workflow_completion` event conforming to the `iglu:com.gitlab/billable_usage/jsonschema/1-0-2` schema, which flows through the Data Insights Platform to CustomersDot for usage-based billing. GKG also issues quota pre-checks against CustomersDot for MCP and REST queries (`crates/gkg-billing/src/quota/`).

Because GKG now emits its own billable usage events, the work falls under SOX IT General Controls (ITGC). The compliance team's guidance is that two scoping approaches are possible:

- **Whole-repo scope.** The path taken by AI Gateway. Every change to the repository becomes a SOX-controlled change.
- **Limited scope (folder / crate level).** Restrict SOX controls to a specific code surface.

The narrow framing in the [!937 review thread](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937#note_3268652539) noted that a naive folder boundary fails for GKG: `BillingObserver` plugs into the generic `PipelineObserver` / `MultiObserver` in `crates/query-engine/pipeline/`, and billing payload fields are populated from JWT claims parsed upstream of the billing module. A change in either could silently drop, duplicate, or corrupt billing events without touching billing code.

Whole-repo scope on GKG was rejected by Knowledge Graph engineering: it would make every ontology change, every query-engine refactor, every parser tweak a SOX-controlled MR, with material velocity cost.

### Current state (post-MR !937 cleanup)

The MR's original folder layout (`crates/gkg-server/src/billing/`) has since been refactored into an isolated crate. As of this ADR:

- All billing-specific code lives in `crates/gkg-billing/` (`license = "LicenseRef-EE"`).
- The only data crossing into the billing crate is `BillingInputs` and `QuotaInputs`, both constructed exclusively in `crates/gkg-server/src/billing_adapter.rs` from `auth::Claims`.
- The adapter's header comment already declares it "the single permitted gkg-server↔gkg-billing seam" and references SOX boundary policy.

This makes a *crate-level* limited scope feasible: the crate is small, its inputs flow through one declared seam, and the remaining hook points that can influence billing correctness are enumerable.

## Decision

**Adopt crate-level SOX scope with explicitly enumerated extended hook points.** The auditable SOX surface for GKG billing comprises:

1. The entire `crates/gkg-billing/` crate.
2. The `billing_adapter.rs` seam in `gkg-server`.
3. A short list of files outside `gkg-billing` whose behavior can silently corrupt billing emission (the "extended hook points" below).

Every path in scope is locked down with required-reviewer `CODEOWNERS` rules. Operational and platform-inherited controls (access reviews, deprovisioning, SSO, audit-log review) are documented and managed out-of-band.

## In-scope surface

### Primary scope

- `crates/gkg-billing/**` — `BillingObserver`, `BillingTracker`, `BillingInputs`, `QuotaInputs`, constants, metrics, the `quota/` submodule, and all tests within the crate.
- `crates/gkg-server/src/billing_adapter.rs` — the single declared seam between `auth::Claims` and the billing crate's input structs.

### Extended hook points

These files live outside `gkg-billing` but can change billing correctness without touching billing code. Each is in scope for SOX review.

| Path | Why in scope |
|---|---|
| `crates/query-engine/pipeline/src/observer.rs` | `PipelineObserver` trait, `MultiObserver` composition, and the `finish()` / `record_error()` semantics that determine whether a billing event is emitted for a given query outcome. A silent change to dispatch order, error propagation, or the success/error gate would drop or duplicate events without touching `gkg-billing`. |
| `crates/gkg-server/src/auth/claims.rs` | `Claims` struct definitions. The billing payload's `realm`, `organization_id`, `subject`, `instance_id`, `unique_instance_id`, `instance_version`, `global_user_id`, `host_name`, `root_namespace_id`, `deployment_type`, `feature_qualified_name`, and `feature_enablement_type` are all sourced from this struct via the adapter. Renaming or dropping any of these silently nulls the corresponding billing field. |
| `crates/gkg-server/src/auth/validator.rs` | JWT validation gate. Determines whether `Claims` are constructed at all for a request and therefore whether a billable call can reach the pipeline. |
| `crates/gkg-server/src/auth/authz.rs` | Authorization gate. If a query is rejected before reaching the pipeline, `BillingObserver::finish()` is never called — the file directly controls the emission gate. |
| `crates/gkg-server/src/grpc/service.rs` | Where `QueryPipelineService` is constructed and where `with_billing(...)` is wired into the pipeline. Removing or reordering the wiring silently stops emission. |
| `crates/gkg-server/src/main.rs` | Tracker startup. Constructs `SnowplowBillingTracker` from `BillingConfig`, sets `batch_size`, and wires it into the gRPC service. Misconfiguration here silently loses events. |
| `crates/gkg-server/src/pipeline/stages/security.rs` | Pipeline security stage. Influences whether the pipeline proceeds far enough to call `finish()`. |
| `crates/gkg-server-config/src/billing.rs` | `BillingConfig` struct. The `enabled: bool` and `collector_url` fields gate emission entirely. |
| `config/default.yaml` (the `billing:` and `quota:` sections only) | Default config for the above. Out-of-tree environment overrides (K8s secrets, `GKG_BILLING__*` env vars) are themselves controlled by infrastructure access policy, but the in-tree defaults are SOX-scoped. |

### Out of scope (intentionally)

The rest of the repository — ontology, query compiler, code graph, indexer, gitaly bindings, formatters other than billing-relevant output, integration testkit, fuzz harness, xtask — is not SOX-scoped. The crate-level seam is what makes this defensible: nothing outside the listed paths can reach the billing crate's emission path except through the adapter's `From<&Claims>` impls.

## ITGC control mapping

Mapping in-scope IT General Controls from the GitLab ITGC matrix to GKG implementation. Source matrix: confidential Google Sheet (linked under References).

| Control | Implementation in GKG |
|---|---|
| **LA.1 Access Provisioning** (preventive, as-needed) | Required-reviewer `CODEOWNERS` rules on all in-scope paths. Only members of the SOX-reviewer group can approve changes; new approvers added only through the same controlled-merge process. |
| **LA.2 Access Termination** (preventive, as-needed) | Inherited from the GitLab platform's deprovisioning script (off-boarding flow). No GKG-specific implementation; documented as inherited. |
| **LA.3 User Access Review** (detective, quarterly) | Quarterly review of the SOX-reviewer group membership (the GitLab group named in the default `CODEOWNERS` rule and in any per-path overrides). Owned by the GKG maintainers; cadence tracked in the operational runbook under `docs/dev/runbooks/`. |
| **LA.4 Privileged Access Management** (preventive, as-needed) | Privileged access for SOX paths = the ability to approve `CODEOWNERS` changes themselves. Restricted to project Owner/Maintainer roles. The `CODEOWNERS` file is itself listed as a SOX-scoped path (any change to it requires the same controlled review). |
| **LA.5 Authentication** (preventive, as-needed) | Inherited from GitLab platform: SSO via Okta + the GitLab.com organization. No local sign-in option for project access. Documented as inherited. |
| **PC.1 Access to Migrate** (preventive, as-needed) | Production migration = merging to `main` and the subsequent release pipeline. Merge access to `main` for SOX paths is gated by `CODEOWNERS` (same mechanism as LA.1). |
| **PC.2 Change Approval** (preventive, as-needed) | `CODEOWNERS` rules on in-scope paths require approval from the SOX-reviewer group before merge. No optional/soft section markers (`^[...]`) on SOX paths — these are hard requirements. |
| **PC.3 Change Review** (detective, monthly) | Monthly audit-log review of changes touching SOX-scoped paths in this project, mirroring the existing AI Gateway cadence. Evidence captured in the shared Audit Events Review spreadsheet. Performed by the SOX-reviewer group lead. |

Out-of-scope controls from the matrix: **CO.1** (Access to Modify Jobs) and **CO.2** (Job Monitoring). All operational jobs live in-project and go through the same MR process as code changes — CO.1 collapses into PC.1/PC.2. CO.2 monitoring is satisfied by the GKG observability stack (`gkg-observability`); not SOX-scoped per the matrix.

## Implementation

### CODEOWNERS

A new `.gitlab/CODEOWNERS` file is added with two kinds of rules:

1. **Default rule** — assigns the GKG maintainers group as default owners of the entire repository.
2. **SOX-scoped rules** — required-reviewer entries (no `^` optional-section prefix) for the primary scope and each extended hook point.

The `CODEOWNERS` file itself is listed as a SOX-scoped path so changes to the reviewer set require the same controlled-merge approval (satisfies LA.4).

### Adapter header comment

`crates/gkg-server/src/billing_adapter.rs` already declares itself "the single permitted gkg-server↔gkg-billing seam" under SOX boundary policy. The header comment is updated to reference this ADR by number so the in-code declaration and the policy doc stay linked.

### Cross-references in agent-facing docs

`AGENTS.md` and `CLAUDE.md` are extended with a row in the "Where to find things" table pointing at this ADR, so agents and contributors touching the in-scope paths are nudged toward the policy before changing emission-relevant code.

### Architecture test (proposed)

To make the boundary self-enforcing, a CI check is proposed that fails any MR where a file outside the adapter or the enumerated hook points imports types from `gkg_billing`. Options under consideration:

- A clippy lint via `cargo-deny` or a workspace-level `disallowed_methods` configuration.
- A bespoke integration test under `crates/integration-tests` that walks the workspace `Cargo.toml` graph and rejects unexpected `gkg-billing` dependents.
- A lightweight pre-commit / CI script (similar to `scripts/check-response-schema-version.sh`) that greps the source tree.

The bespoke integration test is the most defensible from a SOX evidence standpoint (test run is recorded in CI logs and can be cited as control evidence). Sizing this and choosing the mechanism is captured as a follow-up issue and is not part of this ADR's implementation.

## Why not the alternatives

**Whole-repo SOX scope.** Matches the AI Gateway precedent and removes any ambiguity about the surface. Rejected because Knowledge Graph engineering pushed back on the velocity cost during the !937 review: the GKG repo's surface includes the ontology, the query compiler, code-graph parsers, the indexer, and dozens of supporting crates that have no influence on billing correctness. Subjecting them all to SOX merge gates would slow non-billing work without any compliance benefit.

**File-level scope (`BillingObserver` only).** Tighter than crate-level, but the rest of the billing crate (tracker setup, quota client, metrics, constants normalization) carries equal billing-correctness risk. Splitting the crate's contents into "SOX file" and "non-SOX file" is artificial and fragile.

**Tag billing as a label on MRs rather than CODEOWNERS.** Considered as a lower-friction alternative. Rejected because compliance evidence requires *automated enforcement of reviewer requirements* (PC.2), not human-applied labels.

**Move JWT claim parsing into `gkg-billing`.** Would eliminate the `claims.rs` hook point dependency. Rejected: claims are consumed by the entire authorization layer, not just billing. Moving them would invert a much larger dependency relationship for the sake of a single auditable boundary.

## Consequences

What improves:

- A single, declared SOX surface (one crate + one adapter + a short hook-point list) that compliance can audit without reading every file in the repo.
- Engineering velocity outside the billing surface is unaffected by SOX merge requirements.
- The boundary is named and discoverable: ADR + adapter header comment + CODEOWNERS rules all point at the same set of files.

What gets harder:

- The hook-point list is a maintenance liability. Any future code change that introduces a *new* path through which billing correctness can be silently affected has to be added to the list, both in this ADR and in CODEOWNERS. Reviewers of in-scope code need to recognize when a refactor expands the hook-point surface.
- Cross-crate refactors that touch the pipeline observer trait now require SOX-reviewer approval, even if the refactor's intent is unrelated to billing.
- The proposed architecture test adds a CI step. Cost is small but non-zero.

What stays the same:

- Day-to-day work on ontology, query compilation, code-graph indexing, and unrelated server features.
- The current emission and quota-check code paths — the ADR is policy, not behavior.

## Out of scope

- **Tightening the JWT claims schema.** Several billing-relevant fields are `Option<...>` with `#[serde(default)]`. Making them required is tracked separately (the MR !937 follow-up list calls this out) and is not part of the SOX scoping decision.
- **The CustomersDot quota path's specific control surface.** Quota inputs flow through the same adapter and live in `crates/gkg-billing/src/quota/`, so they are automatically in scope; specific control evidence for quota responses (logging, error handling) is a quota-implementation concern, not a scoping concern.
- **Workhorse and Rails-side controls.** The Rails JWT-minting code and the Workhorse proxy in front of GKG are governed by their own repositories' SOX scope and CODEOWNERS.

## References

- Issue: [#507](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/507)
- Implementing MR: [!937](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937) (merged)
- Quota check MR: [b3f415cf — feat(billing): add CustomersDot usage quota checks for mcp/rest queries](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/commit/b3f415cf5b)
- Rails JWT claim additions: [gitlab!232123](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/232123)
- ITGC matrix: confidential Google Sheet (request access through the compliance team)
- AI Gateway billing events reference: [`ai-assist/lib/billing_events/`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/tree/main/lib/billing_events)
- AI Gateway billing events docs: [`ai-assist/docs/billing_events.md`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/blob/main/docs/billing_events.md)
- Billing schema (Iglu): [`com.gitlab/billable_usage/jsonschema/1-0-2`](https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/billable_usage/jsonschema/1-0-2)
- Related GKG work items: [#488 observability](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/488), [#471 `root_namespace_id`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/471)
- Adapter source: `crates/gkg-server/src/billing_adapter.rs`
- Billing crate source: `crates/gkg-billing/`
