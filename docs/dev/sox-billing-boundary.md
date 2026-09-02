# SOX billing boundary — authoring rules

This document captures the SOX-billing authoring rules for the GitLab
Orbit repository. It applies to humans writing code and
to AI coding agents working on this repository (Claude Code, opencode,
Codex, etc.).

If you are about to touch billing-related code, or your task hands data
into the billing path, read this first.

## Why this boundary exists

The `orbit-billing` crate emits the Snowplow billing events that GitLab's
fulfillment systems use to bill customers for Orbit usage. Changes to the
billing-emission code path are in scope for SOX (Sarbanes-Oxley) controls:
the events the system emits must accurately reflect billable activity, and
the surface that produces them must be auditable.

See ADR 013 — `docs/design-documents/decisions/013_billing_sox_scope.md` —
for the formal scope definition and audit context.

## Architecture in one paragraph

`orbit-billing` is the only crate that builds and emits billing events.
Inside `orbit-server`, the file `crates/orbit-server/src/billing_adapter.rs`
is the sole `Claims → BillingInputs` conversion point — every field that
ends up in a billing event flows through this file. A small number of
other files in `orbit-server` invoke those conversions at the call site
(e.g. `billing_adapter::billing_inputs(&claims, …)`) and hold references to
`BillingTracker` / `QuotaService` constructed at startup. They do not
define what data crosses into a billing event — that logic lives only
in `billing_adapter.rs`.

The existing hard gate around this boundary is **CODEOWNERS**
(`.gitlab/CODEOWNERS`), which routes `/crates/orbit-billing/`,
`/crates/orbit-server/src/billing_adapter.rs`, and a small set of related
paths to the SOX-billing approver group. Changes to those paths require
explicit SOX approval to merge.

This document defines the additional content-level rules that the
path-based CODEOWNERS gate cannot see. AI agents should respect them at
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
public API of `orbit-server` — every crate that depends on `orbit-server`
would gain access without being on the explicit allowlist.

### R3. Billing-relevant data only flows through `billing_adapter.rs`

Do not wire a new call site that hands billing-relevant data to anything
outside `billing_adapter.rs`. Billing-relevant data is any field this
change would add to the data populating `BillingInputs`. The current set
of fields is defined in `crates/orbit-billing/src/inputs.rs`; treat that
struct as the authoritative list and let it evolve there. If you are
unsure whether a value is billing-relevant, treat it as one and route
the change accordingly.

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
- If the change must introduce a new file that touches billing and that
  file cannot live in `billing_adapter.rs`, add the new path to the
  `[SOX Billing]` section in `.gitlab/CODEOWNERS` in the same MR so it
  falls under the existing hard gate from the start.
- Surface the concern and ask for explicit approval from someone on the
  SOX-billing CODEOWNERS group.

## Maintaining these rules

When you change the rules in this document, also update the corresponding
inline rules in `.gitlab/duo/mr-review-instructions.yml`. GitLab Duo cannot
follow file references from its custom review instructions, so the YAML
file carries its own copy of the rules and must be kept in sync with this
document.
