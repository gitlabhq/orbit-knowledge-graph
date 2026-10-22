---
title: "Orbit ADR 020: SOX Scoping for Billing Event Emission"
creation-date: "2026-05-15"
last-updated: "2026-10-15"
authors: [ "@snachnolkar" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-05-15

## Context

[MR !937](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937) added Snowplow billing event emission to Orbit's `ExecuteQuery` path. Eligible successful graph queries enqueue an `orbit_workflow_completion` event conforming to the `iglu:com.gitlab/billable_usage/jsonschema/1-0-2` schema when tracking is enabled and the required attribution is valid. Schema responses do not emit billing events. Missing or unrecognized realm values, or missing SaaS namespace attribution, prevent event construction. Delivery through the Data Insights Platform to CustomersDot is asynchronous; successful execution does not guarantee delivery.

Orbit also issues quota pre-checks against CustomersDot before query execution. The gate supports admin-token and license-checksum authentication. Requests without a license checksum in license-checksum mode, and SaaS requests without namespace attribution, skip the check. Other quota-check failures deny the request.

Builds compiled with `ORBIT_BILLING_ENFORCED=true` validate configuration at startup. They refuse to start unless billing and quota checks are enabled, endpoints match an approved environment pair, and authentication modes are compatible. Builds without this flag can disable billing and quota checks through configuration.

Because Orbit emits its own billable usage events, the work falls under SOX IT General Controls (ITGC). The compliance team's guidance is that two scoping approaches are possible:

- **Whole-repo scope.** The path taken by AI Gateway. Every change to the repository becomes a SOX-controlled change.
- **Limited scope (folder / crate level).** Restrict SOX controls to a specific code surface.

### Current state (post-MR !937 cleanup)

The original server-local billing module has since been refactored into an isolated crate. The crate and its host server were later renamed from `gkg-billing` / `gkg-server` to `orbit-billing` / `orbit-server`. As of this ADR:

- Billing event construction, delivery integration, and quota logic live in `orbit-billing`.
- Production conversion of `auth::Claims` into `BillingInputs` and `QuotaCheckInputs` is centralized in the claims-conversion adapter in `orbit-server`.
- Configuration, delivery credentials, and pipeline observer callbacks also cross the crate boundary. They do not pass through the claims-conversion adapter.

This makes a *crate-level* (folder-level) limited scope feasible, provided the supporting hook points are also controlled. The adapter centralizes claim mapping; it does not isolate every dependency that can influence billing correctness.

## Decision

**Adopt crate-level SOX scope with explicitly enumerated extended hook points.** The auditable SOX surface for Orbit billing comprises:

1. The entire `orbit-billing` crate.
2. The claims-conversion adapter in `orbit-server`.
3. A short list of files outside `orbit-billing` whose behavior can impact billing (the "extended hook points" below).

Every path in scope is locked down with required-reviewer `CODEOWNERS` rules.

## In-scope surface

### Primary scope

- **Billing crate:** event construction and tracking, quota checks and caching, delivery authentication integration, startup enforcement, constants, metrics, and tests.
- **Claims-conversion adapter:** the single declared conversion point between `auth::Claims` and the billing crate's input structs.

### Extended hook points

These components live outside `orbit-billing` but can change billing correctness without touching billing code. Each is in scope for SOX review. The exact file paths are maintained in `.gitlab/CODEOWNERS`. Update those paths when a hook point is added or moved. When a new category is introduced, update this table in a separate docs MR.

| Hook point | Why in scope |
|---|---|
| **Pipeline observer interface** | Defines `finish()` / `record_error()` semantics and `MultiObserver` dispatch order. Changes here determine whether billing events fire at all for a given query outcome. |
| **JWT claims struct** | Source of billing identity and attribution fields, including realm, root namespace, and instance identity. Changes can alter attribution, prevent emission, or reject a request. |
| **JWT validation gate** | Determines whether claims are constructed at all for a request, and therefore whether a billable call can reach the pipeline. |
| **Authorization gate** | Controls whether a query is allowed to complete. Rejection before successful completion prevents billing emission. |
| **Billing observer construction** | Instantiates the observer with inputs supplied by the claims-conversion adapter and attaches it to the pipeline. |
| **Pipeline billing wiring** | Where the billing tracker is wired into the query pipeline. Removing or reordering this silently stops emission. |
| **Tracker and quota startup** | Validates configuration, constructs the tracker and quota service, wires them into the service, and drains queued events at shutdown. Changes can disable emission or quota enforcement, prevent startup, or lose queued events. |
| **Pipeline security gate** | Controls whether the pipeline proceeds far enough to reach `finish()`. |
| **Billing configuration and defaults** | Define emission and quota enablement, endpoint selection, authentication modes, and other settings that affect billing delivery and quota enforcement. |
| **Billing delivery token cache** | Controls caching and refresh of the Cloud Connector token used to authenticate billing-event delivery. Changes can prevent delivery without modifying the billing crate. |

### Out of scope (intentionally)

Paths not covered by the SOX section in CODEOWNERS are outside this declared scope. This is a maintained review boundary, not proof that other code cannot affect billing. Changes that introduce or move a billing hook point must update the scoped paths. A new hook-point category also needs a separate docs MR that updates this table. The controls below reduce, but do not eliminate, the risk of an unrecognized dependency expanding the surface.

## Implementation

### CODEOWNERS

The CODEOWNERS file defines a `[SOX Billing]` section owned by `@gitlab-org/orbit/team`. It covers the billing crate, claims-conversion adapter, and extended hook points. Changes to these paths require code-owner approval on the protected `main` branch. There is no repository-wide default ownership rule.

The CODEOWNERS file and billing dependency-boundary test are also included in this section, so changes to these controls require the same approval. Because ownership is file-based, unrelated changes within a scoped file also require approval.

### Adapter header comment

The adapter declares itself the single claims-conversion seam between `orbit-server` and `orbit-billing`. Its header references this ADR and acknowledges the extended hook points. It does not claim that configuration, credentials, or observer callbacks pass through claim conversion.

### Cross-references in agent-facing docs

`AGENTS.md` and `CLAUDE.md` point at this ADR under "Where to find things". This nudges agents and contributors touching the in-scope paths toward the policy before they change billing emission or quota-check code.

### Architecture test

[MR !1372](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1372) added the `billing-boundary-check` CI job. It checks workspace package dependency declarations and fails if any crate outside the permitted list (`orbit-server`) declares a direct dependency on `orbit-billing`. A passing run is evidence that this direct dependency surface has not expanded. It does not check new imports within the permitted crate, re-exports, or changes to existing supporting dependencies.

### Automated review guidance

[MR !1468](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1468) added Duo review instructions that flag new billing importers, re-exports, claim-mapping bypasses, and direct event emission outside the billing crate. These are advisory checks, not a merge gate. Human reviewers remain responsible for recognizing new hook points and updating CODEOWNERS.

### Access approval

Adding members to the SOX approver group requires recorded manager approval. A formal maintainer nomination is optional, as clarified in the [compliance discussion](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1358#note_3413138557). Confirming the operational process and retaining approval evidence remain follow-up work. CODEOWNERS and CI checks do not establish that this access process is in place.

## Why not the alternatives

**Whole-repo SOX scope.** Matches the AI Gateway precedent and removes any ambiguity about the surface. Rejected because this impacts the engineering velocity across any changes across the whole repo even when they have no influence on billing correctness. Subjecting entire repo to SOX compliance would slow non-billing work without any compliance benefit.

## Consequences

What improves:

- A single, declared SOX surface (one crate + one adapter + a short hook-point list) that compliance can audit without reading every file in the repo.
- Engineering velocity outside the billing surface is unaffected by SOX merge requirements.
- The boundary is named and discoverable by humans and AI agents: ADR + adapter header comment + CODEOWNERS rules all point at the same set of files.

What gets harder:

- The hook-point list is a maintenance liability. A future code change can introduce a *new* path through which billing correctness is silently affected. Add the path to CODEOWNERS, and update this ADR in a separate docs MR only if it introduces a new hook-point category. Reviewers of in-scope code need to recognize when a refactor expands the hook-point surface.
- Cross-crate refactors that touch the pipeline observer trait now require SOX-reviewer approval, even if the refactor's intent is unrelated to billing.
- The architecture test adds a CI step. Cost is small but non-zero.

## References

- Issue: [#507](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/507)
- Implementing MR: [!937](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937)
- SOX scoping discussion on !937: [review thread](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/937#note_3268652539)
- Quota check MR: [CustomersDot usage quota checks for mcp/rest queries](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1304)
- Rails JWT claim additions: [GitLab !232123](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/232123)
- AI Gateway billing events reference: [`ai-assist/lib/billing_events/`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/tree/main/lib/billing_events)
- AI Gateway billing events docs: [`ai-assist/docs/billing_events.md`](https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/blob/main/docs/billing_events.md)
- Billing schema (Iglu): [`com.gitlab/billable_usage/jsonschema/1-0-2`](https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/billable_usage/jsonschema/1-0-2)
- Related Orbit work item: [#488](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/488)
