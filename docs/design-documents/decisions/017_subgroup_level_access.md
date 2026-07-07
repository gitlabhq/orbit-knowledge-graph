---
title: "GKG ADR 017: Subgroup-level Reporter+ access"
creation-date: "2026-07-07"
authors: [ "@dgruzd" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-07-07

## Context

Orbit requires Reporter+ membership on a **top-level group** (root namespace) to use
the Knowledge Graph. A user who holds Reporter+ only on a subgroup is denied access
even though the data for that subgroup is already indexed and the query engine is
already subgroup-aware.

The ask is to let a Reporter+ **subgroup** member use Orbit scoped to that subgroup,
without requiring top-level membership.

### Why the query engine is already subgroup-aware

The three-layer authorization model documented in
[`docs/design-documents/security.md`](../security.md) is prefix-based on
`traversal_path` and does not distinguish between a top-level group path and a subgroup
path:

- **Layer 1 (tenant segregation):** the compiler injects
  `startsWith(traversal_path, ?)` for each authorized path. A subgroup path produces a
  longer, stricter prefix.
- **Layer 2 (traversal-ID filtering):** Rails passes `{path, access_level}` tuples in
  the JWT. The security pass drops paths below an entity's `required_role`. No depth
  check.
- **Layer 3 (Rails redaction):** `Ability.allowed?` per resource, independent of hierarchy.

`SecurityContext` validates path **format** (`^(\d+/)+$`), not depth. Existing unit
tests exercise subgroup paths (`1/22/`, `1/33/`). Partition pruning keys on
`segments[1]` (the top-level namespace ID), which any subgroup path still contains.
ClickHouse already holds subgroup data because indexing dispatches per enabled root
namespace and stores the full traversal hierarchy.

**The "top-level only" restriction is not a query or authorization constraint.** It is a
Rails-side enablement, licensing, and eligibility policy deliberately keyed on the
root namespace. Four Rails gates enforce it:

| # | Gate | Location | What it enforces |
|---|------|----------|------------------|
| 1 | **Enablement** | `enabled_namespace.rb` | Validation: "Only top-level groups can be indexed" |
| 2 | **API access** | `data.rb` → `AuthorizationContext` | Reduces Reporter+ groups to their root and checks `EnabledNamespace` for that root |
| 3 | **Licensing** | `OrbitLicense.available_for?` | Iterates `user.authorized_groups.top_level` and checks `:orbit` license + enrollment |
| 4 | **Billing** | `GoverningNamespaceFinder` | Resolves the billing namespace from the user's root groups on a paid plan |

Gate 2 already passes for a subgroup member whose root is enrolled
(`traversal_ids.first` resolves to the root). The gap surfaces when the user holds no
direct top-level membership, so gates 3 and 4 deny them.

`Search::GroupsFinder` already returns subgroups the user is a direct or linked member
of. It does not expand to ancestors. The JWT publisher
(`jwt_auth.rb`) transmits `{path, access_levels}` tuples without a top-level
assumption, so subgroup paths would flow naturally if the surrounding gates allowed the
request.

For the full research, see [dgruzd/tasks#3110](https://gitlab.com/dgruzd/tasks/-/work_items/3110)
and the supporting analysis in the droid-workspace `task/3110/` artifacts.

## Decision

**Relax only the Rails access and licensing gates so a Reporter+ subgroup member can
query the already-indexed subgroup slice. Keep root-level indexing and enrollment
unchanged. No GKG (Rust) code change.**

Concretely:

1. **Leave gate 1 (enablement/indexing) untouched.** Subgroup data is already indexed
   under the enrolled root namespace. No re-indexing required.
2. **Rework gate 3 (`OrbitLicense.available_for?`)** so a subgroup member is entitled
   when their subgroup's root namespace is licensed and enrolled. The user's traversal
   path scopes them to their subgroup subtree through the existing prefix filter.
3. **Reconcile gate 4 (governing/billing namespace)** so billing attribution can be
   satisfied by a subgroup membership under an enrolled and licensed root. This
   requires a product and billing decision (see [Open questions](#open-questions)).
4. **Gate 2 needs no change** for users whose root is enrolled (it already resolves
   correctly). If the product later wants subgroups usable under a non-enrolled root,
   gate 2 would need rework.

### Effort

Small to Medium (approximately 3-6 engineering days), dominated by Rails changes.

| Workstream | Effort | Notes |
|------------|--------|-------|
| GKG (Rust) core | 0 days | Already subgroup-aware |
| GKG isolation integration test | 0.5 day | Sibling-subgroup isolation for a subgroup-only path set |
| Rails `OrbitLicense` rework | 1-2 days | Entitle subgroup member through root's license + enrollment |
| Rails gate reconciliation | 1-2 days | Single source of truth for "usable + scope" across gates 2/3/4 |
| Rails tests | 1 day | Subgroup-member specs across all gates |
| Documentation (`security.md`, SOX boundary note) | 0.5 day | Reflect subgroup access |
| Product / Billing / Security decision | Not eng-days | The real critical path |

### Alternatives considered

**Option B: per-subgroup enrollment (change `EnabledNamespace`).** Allow enrolling
individual subgroups and dispatch indexing per enrolled subgroup. Rejected as
higher-surface for little benefit: the data is already present in ClickHouse under the
root's traversal hierarchy, so per-subgroup enrollment multiplies the
billing/enrollment surface without unlocking new data. It also requires new migrations,
extends the SOX review surface, and enlarges the test matrix. Option B only becomes
relevant if product wants subgroups usable under a *non-enrolled* root.

**Do nothing (keep top-level only).** Preserves the current model but blocks use cases
where a user has Reporter+ on a subgroup without top-level membership. The status quo
remains the fallback if the billing/SOX questions cannot be resolved.

### Open questions

This ADR is Proposed specifically to align the team on these questions before
implementation proceeds.

1. **Billing/SOX attribution.** The primary open question. Orbit billing is
   root-namespace and paid-plan oriented
   ([`docs/dev/sox-billing-boundary.md`](../../dev/sox-billing-boundary.md)). How is
   usage attributed for a subgroup-only user? The governing namespace finder
   resolves from top-level groups on a paid plan, so a subgroup-only user has no
   top-level group to resolve. This requires explicit sign-off from Product, Billing/SOX, and Security
   before implementation.

2. **Sibling-subgroup data isolation.** Granting access to `1/100/200/` must never
   leak data from `1/100/300/`. The GKG prefix filter (`startsWith`) already enforces
   this, and existing unit tests exercise subgroup paths. However, an explicit
   **integration test** covering the subgroup-only path set is required before shipping.
   This revisits the stance in [`docs/design-documents/security.md`](../security.md)
   that states *"No sparse permissions in V1"*. Subgroup-level access is a form of
   sparse permission in the namespace hierarchy. The ADR acknowledges this
   departure and proposes it as a controlled relaxation with test coverage.

3. **Enablement vs. entitlement drift.** If a subgroup is usable but its root is not
   enrolled or licensed, gates 2, 3, and 4 can disagree. The implementation must
   maintain a single source of truth for "can this user use Orbit and against what
   scope."

4. **Global entities (User, Runner).** These have no `traversal_path` and rely on
   Rails redaction. They are only reachable through edge table joins that carry
   `traversal_path`, so subgroup users should not gain broader visibility. The
   integration test must confirm this.

5. **Cache correctness (`OrbitLicense` per-user cache).** `OrbitLicense` caches a
   per-user boolean. If the entitlement logic changes to consider subgroup membership,
   the cache key and invalidation strategy must reflect subgroup membership changes.

### Consequences

What improves:

- Subgroup-level Reporter+ members gain Orbit access scoped to their subgroup subtree,
  without requiring top-level group membership.
- No GKG (Rust) change, no re-indexing, no new data pipelines. The change is
  contained in the Rails monolith.
- The existing three-layer security model is preserved. Subgroup access is a
  natural extension of the prefix-based traversal path model.

What gets harder:

- Billing attribution gains a new case (subgroup-only user) that the current
  governing namespace finder does not handle.
- The "No sparse permissions in V1" stance in `security.md` needs revision.
  Subgroup-level access is a controlled form of sparse permission.
- The `OrbitLicense` cache may need finer-grained invalidation if subgroup membership
  changes should immediately affect Orbit access.
- Traversal path count per user may increase. A user with Reporter+ on many scattered
  subgroups produces more distinct prefixes than one holding a single top-level group,
  because the trie compaction cannot merge siblings the user does not hold. Monitor
  the existing `gkg.rails.traversal_ids_computed` metric and the >100-prefix alert
  documented in `security.md`.
