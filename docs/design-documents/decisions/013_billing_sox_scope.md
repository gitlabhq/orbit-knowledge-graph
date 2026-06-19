---
title: "GKG ADR 013: SOX Scoping for Billing Event Emission"
creation-date: "2026-05-15"
last-updated: "2026-06-19"
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

### Current state (post-MR !937 cleanup)

The MR's original folder layout (`crates/gkg-server/src/billing/`) has since been refactored into an isolated crate. As of this ADR:

- All billing-specific code lives in `crates/gkg-billing/`.
- The only data crossing into the billing crate is `BillingInputs` and `QuotaCheckInputs`, both constructed exclusively in `crates/gkg-server/src/billing_adapter.rs` from `auth::Claims`.

This makes a *crate-level* (folder-level) limited scope feasible: the crate is small, its inputs flow through one declared seam, and the remaining hook points that can influence billing correctness are enumerable.

## Decision

**Adopt crate-level SOX scope with explicitly enumerated extended hook points.** The auditable SOX surface for GKG billing comprises:

1. The entire `crates/gkg-billing/` crate.
2. The `billing_adapter.rs` seam in `gkg-server`.
3. A short list of files outside `gkg-billing` whose behavior can impact billing (the "extended hook points" below).

Every path in scope is locked down with required-reviewer `CODEOWNERS` rules.

## In-scope surface

### Primary scope

- `crates/gkg-billing/**` — `BillingObserver`, `BillingTracker`, `BillingInputs`, `QuotaCheckInputs`, `QuotaService`, constants, metrics, the `quota/` submodule, and all tests within the crate.
- `crates/gkg-server/src/billing_adapter.rs` — the single declared seam between `auth::Claims` and the billing crate's input structs.

### Extended hook points

These components live outside `gkg-billing` but can change billing correctness without touching billing code. Each is in scope for SOX review. The exact file paths are enforced via `.gitlab/CODEOWNERS`; update both this table and CODEOWNERS when the hook-point surface changes.

| Hook point | Why in scope |
|---|---|
| **Pipeline observer interface** | Defines `finish()` / `record_error()` semantics and `MultiObserver` dispatch order. Changes here determine whether billing events fire at all for a given query outcome. |
| **JWT claims struct** | Source of all billing payload fields (`realm`, `root_namespace_id`, `instance_id`, etc.). Renaming or removing a field silently nulls it in emitted events. |
| **JWT validation gate** | Determines whether claims are constructed at all for a request, and therefore whether a billable call can reach the pipeline. |
| **Authorization gate** | Controls whether a query reaches the pipeline. If a request is rejected here, no billing event is emitted. |
| **Billing observer construction** | Where `BillingObserver` is instantiated and `BillingInputs` are populated from claims. The actual point where billing data is assembled and attached to the pipeline. |
| **Pipeline billing wiring** | Where the billing tracker is wired into the query pipeline. Removing or reordering this silently stops emission. |
| **Tracker startup** | Constructs the Snowplow billing tracker and `QuotaService` from config and wires them into the service. Misconfiguration here silently loses all events or quota enforcement. |
| **Pipeline security gate** | Controls whether the pipeline proceeds far enough to reach `finish()`. |
| **Billing config struct** | Contains the `enabled` flag and `collector_url`. These fields gate emission entirely. |

### Out of scope (intentionally)

The rest of the repository — ontology, query compiler, code graph, indexer, gitaly bindings, formatters other than billing-relevant output, integration testkit, fuzz harness, xtask — is not SOX-scoped. The crate-level seam is what makes this defensible: nothing outside the listed paths can reach the billing crate's emission path except through `crates/gkg-server/src/billing_adapter.rs`.

## Implementation

### CODEOWNERS

A new `.gitlab/CODEOWNERS` file is added with two kinds of rules:

1. **Default rule** — assigns the GKG maintainers group as default owners of the entire repository.
2. **SOX-scoped rules** — required reviewer entries for the primary scope and each extended hook point.

The `CODEOWNERS` file itself is listed as a SOX-scoped path so changes to the reviewer set require the same controlled-merge approval.

### Adapter header comment

`crates/gkg-server/src/billing_adapter.rs` already declares itself "the single permitted gkg-server -> gkg-billing seam" under SOX boundary policy. The header comment is updated to reference this ADR by number so the in-code declaration and the ADR stay linked so that AI agents refer to it and comply when making changes.

### Cross-references in agent-facing docs

`AGENTS.md` and `CLAUDE.md` are extended with a row in the "Where to find things" table pointing at this ADR, so agents and contributors touching the in-scope paths are nudged toward the policy before changing billing emission or quota check related code.

### Architecture test

To make the boundary self-enforcing, [MR !1372](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1372) added `crates/integration-tests/tests/billing_boundary.rs` — an integration test that walks the workspace `Cargo.toml` graph and fails if any crate outside the permitted list (`gkg-server`) declares a dependency on `gkg-billing`. A failing run in CI is control evidence that the billing crate's dependency surface has not silently expanded.

## Why not the alternatives

**Whole-repo SOX scope.** Matches the AI Gateway precedent and removes any ambiguity about the surface. Rejected because this impacts the engineering velocity across any changes across the whole repo even when they have no influence on billing correctness. Subjecting entire repo to SOX compliance would slow non-billing work without any compliance benefit.

## Consequences

What improves:

- A single, declared SOX surface (one crate + one adapter + a short hook-point list) that compliance can audit without reading every file in the repo.
- Engineering velocity outside the billing surface is unaffected by SOX merge requirements.
- The boundary is named and discoverable by humans and AI agents: ADR + adapter header comment + CODEOWNERS rules all point at the same set of files.

What gets harder:

- The hook-point list is a maintenance liability. Any future code change that introduces a *new* path through which billing correctness can be silently affected has to be added to the list, both in this ADR and in CODEOWNERS. Reviewers of in-scope code need to recognize when a refactor expands the hook-point surface.
- Cross-crate refactors that touch the pipeline observer trait now require SOX-reviewer approval, even if the refactor's intent is unrelated to billing.
- The proposed architecture test adds a CI step. Cost is small but non-zero.

## References

- Issue: [#507](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/507)
- Implementing MR: [!937](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937)
- SOX scoping discussion on !937: [review thread](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937#note_3268652539)
- Quota check MR: [CustomersDot usage quota checks for mcp/rest queries](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1304)
- Rails JWT claim additions: [gitlab!232123](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/232123)
- AI Gateway billing events reference: [`ai-assist/lib/billing_events/`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/tree/main/lib/billing_events)
- AI Gateway billing events docs: [`ai-assist/docs/billing_events.md`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/blob/main/docs/billing_events.md)
- Billing schema (Iglu): [`com.gitlab/billable_usage/jsonschema/1-0-2`](https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/billable_usage/jsonschema/1-0-2)
- Related GKG work items: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/488
