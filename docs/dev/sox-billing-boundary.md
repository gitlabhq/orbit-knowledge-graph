# SOX billing boundary — authoring rules

This document captures the SOX-billing authoring rules for the GitLab
Orbit repository. It applies to humans writing code and
to AI coding agents working on this repository (Claude Code, opencode,
Codex, etc.).

If you are about to touch billing-related code, or your task hands data
into the billing path, read this first.

## Why this boundary exists

The `orbit-billing` crate emits the Snowplow billing events that GitLab
fulfillment systems use to bill customers for Orbit usage. Changes to the
billing-emission code path are in scope for SOX (Sarbanes-Oxley) controls.
The events the system emits must accurately reflect billable activity. The
surface that produces them must be auditable.

See ADR 020 (`docs/design-documents/decisions/020_billing_sox_scope.md`)
for the formal scope definition and audit context.

## Architecture in one paragraph

`orbit-billing` is the only crate that builds and emits billing events.
Inside `orbit-server`, the file `crates/orbit-server/src/billing_adapter.rs`
is the sole conversion point from `Claims` into `BillingInputs` and
`QuotaCheckInputs`. Billing identity and attribution claims flow through
this adapter; configuration, delivery credentials, and observer callbacks
are separate inputs. A small number of
other files in `orbit-server` invoke those conversions at the call site
(e.g. `billing_adapter::billing_inputs(&claims, …)`). They hold references to
`BillingTracker` / `QuotaService` constructed at startup. They do not
map claims into billing input structs themselves. Event construction and
execution metadata assembly live in `orbit-billing`.

Two hard gates protect this boundary:

- **CODEOWNERS** (`.gitlab/CODEOWNERS`). The `[SOX Billing]` section routes
  the billing crate, adapter, and related paths to `@gitlab-org/orbit/team`.
  Changes to those paths require code-owner approval on protected `main`.
- **The `billing-boundary-check` CI job.** It runs
  `crates/integration-tests/tests/billing_boundary.rs` and fails when a workspace crate
  other than `orbit-server` declares a direct dependency on `orbit-billing`.
  It does not detect new imports within `orbit-server` or re-exports.

This document defines the additional content-level rules that these
gates cannot see. AI agents should respect them at
authoring time.

## Rules

### R1. New `orbit-billing` importers are flagged

Do not add a `use orbit_billing` or `orbit_billing::` reference to a file that
did not previously import `orbit-billing`. The legitimate importers already
exist as part of the intentional wiring. A new importer expands the
SOX-audited surface and must be reviewed by someone on the SOX-billing
CODEOWNERS group.

### R2. No `pub use orbit_billing` re-exports from `orbit-server`

Do not add `pub use orbit_billing::...` (or equivalent re-export) anywhere
in `orbit-server`. A re-export silently widens the SOX scope through the
public API of `orbit-server`. Every crate that depends on `orbit-server`
would gain access without being on the explicit allowlist.

### R3. Claims conversion stays in `billing_adapter.rs`

Map claims into `BillingInputs` and `QuotaCheckInputs` only in
`billing_adapter.rs`. Call sites use the adapter's conversions instead of
constructing these input structs or duplicating claim mappings themselves.
Configuration, delivery credentials, and observer callbacks are separate
inputs and do not pass through this conversion seam. New hook points for
those inputs still require SOX review and CODEOWNERS coverage.

### R4. Billing emission goes through `BillingTracker` / `BillingObserver`

Do not reference `labkit_events::BillingEvent` or call
`Tracker::track_billing_event` from any file outside `crates/orbit-billing/`.
Billing-event emission is encapsulated by `BillingTracker` and
`BillingObserver`; direct use of the underlying labkit-events API
bypasses the audited path.

### R5. No new billing/usage telemetry types outside `orbit-billing`

Do not add a new type, trait, or function whose name or doc-comment
suggests its purpose is to emit billing or usage telemetry. New
abstractions for billing emission belong in `orbit-billing`. If you need
a new emission shape, design it inside `orbit-billing` and expose it
through the existing `BillingTracker` / `BillingObserver` contract.

## How to comply if you need to touch billing

If your task genuinely requires expanding the billing path:

1. Prefer routing the change through `billing_adapter.rs`. New fields that
   should populate billing events go through the `Claims → BillingInputs`
   conversion in that file.
2. If the change cannot fit in `billing_adapter.rs`, request explicit
   review from someone on the SOX-billing CODEOWNERS group before merging.
3. Call out the SOX impact in the MR description.

## For AI coding agents

If a task you are given would require breaking any rule above, do not
silently break it. Stop and surface the conflict to the human owner of
the task. The right answer is almost always one of:

- Move the change into `billing_adapter.rs`.
- A new file may touch billing and be unable to live in
  `billing_adapter.rs`. In that case, add the new path to the
  `[SOX Billing]` section in `.gitlab/CODEOWNERS` in the same MR. It then
  falls under the existing hard gate from the start.
- Surface the concern and ask for explicit approval from someone on the
  SOX-billing CODEOWNERS group.

## Maintaining these rules

When you change the rules in this document, also update the corresponding
inline rules in `.gitlab/duo/mr-review-instructions.yml`. GitLab Duo cannot
follow file references from its custom review instructions. So the YAML
file carries its own copy of the rules. That copy must be kept in sync with
this document.
