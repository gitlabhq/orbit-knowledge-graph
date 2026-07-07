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

On **SaaS**, Orbit requires Reporter+ membership on a **top-level group** (root
namespace) to use the Knowledge Graph. A user who holds Reporter+ only on a subgroup
is denied access even though the data for that subgroup is already indexed and the
query engine is already subgroup-aware.

On **GitLab Self-Managed**, this restriction does not exist.
`OrbitLicense.available_for?` returns early with
`::License.feature_available?(:orbit)` when `gitlab_com_subscriptions` is unavailable,
performing no top-level group check. `GoverningNamespaceFinder` likewise returns
`Group.none` off-SaaS. GitLab Self-Managed subgroup-only users can already use Orbit
end-to-end, which serves as existing evidence that the GKG query engine handles
subgroup paths correctly in production.

The ask is to extend this capability to **SaaS**: let a Reporter+ subgroup member use
Orbit scoped to that subgroup, without requiring top-level membership.

### Why the query engine is already subgroup-aware

The three-layer authorization model documented in
[`docs/design-documents/security.md`](../security.md) is prefix-based on
`traversal_path` and does not distinguish between a top-level group path and a subgroup
path. `security.md` itself documents this explicitly in the Layer 2 example:

> User has Reporter+ on subgroup `[100, 200]` - Can access resources with
> traversal_ids starting with `[100, 200]`, but NOT resources under sibling group
> `[100, 300]`.

The three layers:

- **Layer 1 (tenant segregation):** the compiler injects
  `startsWith(traversal_path, ?)` for each authorized path. A subgroup path produces a
  longer, stricter prefix.
- **Layer 2 (traversal-ID filtering):** Rails passes `{path, access_level}` tuples in
  the JWT. The security pass drops paths below an entity's `required_role`. No depth
  check.
- **Layer 3 (Rails redaction):** `Ability.allowed?` per resource, independent of
  hierarchy.

`SecurityContext` validates path **format** (`^(\d+/)+$`), not depth. Existing unit
tests exercise subgroup paths (`1/22/`, `1/33/`). Partition pruning keys on
`segments[1]` (the top-level namespace ID), which any subgroup path still contains.
ClickHouse already holds subgroup data because indexing dispatches per enabled root
namespace and stores the full traversal hierarchy.

**The "top-level only" restriction is not a query or authorization constraint.** It is
a SaaS-side Rails policy in the enablement, licensing, and billing gates, deliberately
keyed on the root namespace. The security model in `security.md` already supports
subgroup-scoped access; the "No sparse permissions in V1" stance there refers
specifically to project-level and item-level access (for example, access to a single
project without group access), not to subgroup-level group access.

Four SaaS-side Rails gates enforce the top-level restriction:

| # | Gate | Location | What it enforces |
|---|------|----------|------------------|
| 1 | **Enablement** | `enabled_namespace.rb` | Validation: "Only top-level groups can be indexed" |
| 2 | **API access** | `data.rb` / `AuthorizationContext` | Reduces Reporter+ groups to their root and checks `EnabledNamespace` for that root |
| 3 | **Licensing** | `OrbitLicense.available_for?` | On SaaS: iterates `user.authorized_groups.top_level`, checks `:orbit` license + `namespace_enrollable_or_enrolled?` (the `orbit_enroll_namespace` feature flag OR enrolled) |
| 4 | **Billing** | `GoverningNamespaceFinder` | Resolves the billing namespace from the user's root groups on a paid plan; returns `Group.none` off-SaaS |

Gate 2 already passes for a subgroup member whose root is enrolled
(`traversal_ids.first` resolves to the root). Crucially, gate 2 only checks
**enablement of the root**; the path set sent to GKG remains the user's actual
subgroup path, not a root path inferred during the enablement check. The gap surfaces
when the user holds no direct top-level membership, so gates 3 and 4 deny them on
SaaS.

`Search::GroupsFinder` already returns subgroups the user is a direct or linked member
of. It does not expand to ancestors. The JWT publisher (`jwt_auth.rb`) transmits
`{path, access_levels}` tuples without a top-level assumption, so subgroup paths would
flow naturally if the surrounding gates allowed the request.

For the full research, see [dgruzd/tasks#3110](https://gitlab.com/dgruzd/tasks/-/work_items/3110)
and the supporting analysis in the droid-workspace `task/3110/` artifacts.

## Decision

**Relax only the SaaS-side Rails access and licensing gates so a Reporter+ subgroup
member can query the already-indexed subgroup slice. Keep root-level indexing and
enrollment unchanged. No GKG (Rust) code change.**

Concretely:

1. **Leave gate 1 (enablement/indexing) untouched.** Subgroup data is already indexed
   under the enrolled root namespace. No re-indexing required.
2. **Rework gate 3 (`OrbitLicense.available_for?`)** so a subgroup member is entitled
   when their subgroup's root namespace has the `:orbit` licensed feature and satisfies
   `namespace_enrollable_or_enrolled?`. The user's traversal path scopes them to their
   subgroup subtree through the existing prefix filter.
3. **Reconcile gate 4 (governing/billing namespace)** so billing attribution can be
   satisfied by a subgroup membership under an enrolled and licensed root. This
   requires a product and billing decision (see [Open questions](#open-questions)).
4. **Gate 2 needs no change** for users whose root is enrolled (it already resolves
   correctly). If the product later wants subgroups usable under a non-enrolled root,
   gate 2 would need rework.

### Effort

Code effort: approximately 4-6 engineering days, dominated by Rails changes.
Cross-functional billing and product decisions are a separate blocking dependency with
unknown timeline.

| Workstream | Effort | Notes |
|------------|--------|-------|
| GKG (Rust) core | 0 days | Already subgroup-aware |
| GKG isolation integration test | 0.5 day | Sibling-subgroup isolation for a subgroup-only path set |
| Rails `OrbitLicense` rework | 1-2 days | Entitle subgroup member through root's license + enrollment |
| Rails gate reconciliation | 1-2 days | Single source of truth for "usable + scope" across gates 2/3/4 |
| Rails tests | 1 day | Subgroup-member specs across all gates |
| Documentation (`security.md`, SOX boundary note) | 0.5 day | Reflect subgroup access |
| **Product / Billing / Security decision** | **Blocking, unknown** | The real critical path; not engineering days |

### Alternatives considered

**Option B: per-subgroup enrollment (change `EnabledNamespace`).** Allow enrolling
individual subgroups and dispatch indexing per enrolled subgroup. Rejected as
higher-surface for little benefit: the data is already present in ClickHouse under the
root's traversal hierarchy, so per-subgroup enrollment multiplies the
billing/enrollment surface without unlocking new data. It also requires new migrations,
extends the SOX review surface, and enlarges the test matrix. Option B only becomes
relevant if product wants subgroups usable under a *non-enrolled* root.

**Option C: derive the enrolled root as the governing namespace.** A variant of Option
A where the gate-3 predicate resolves the subgroup's containing enrolled root and uses
that root as the governing namespace for billing attribution. This makes the billing
trade-off explicit (all subgroup usage bills to the enrolled root) without requiring
per-subgroup enrollment. Worth evaluating as the concrete implementation of Option A's
gate-4 reconciliation.

**Do nothing (keep top-level only on SaaS).** Preserves the current model but blocks
use cases where a SaaS user has Reporter+ on a subgroup without top-level membership.
The status quo remains the fallback if the billing/SOX questions cannot be resolved.

### Open questions

This ADR is Proposed specifically to align the team on these questions before
implementation proceeds.

1. **Billing/SOX attribution.** The primary open question. Orbit billing is
   root-namespace and paid-plan oriented
   ([`docs/dev/sox-billing-boundary.md`](../../dev/sox-billing-boundary.md);
   [ADR 007](007_monetization_engineering.md) section 1.3 on governing namespace
   selection). How is usage attributed for a subgroup-only user? The governing
   namespace finder resolves from top-level groups on a paid plan, so a subgroup-only
   user has no top-level group to resolve. This requires explicit sign-off from
   Product, Billing/SOX, and Security before implementation.

   A specific sub-case: `Search::GroupsFinder` includes linked/shared/invited groups at
   `LEAST(link access, member access)`. A user from company B invited through a
   group-share into a subgroup under company A's enrolled root would, under the
   proposed gate-3 predicate, be entitled through a root they have no membership in,
   with usage billed to company A. The gate-3 entitlement predicate must define
   whether "their subgroup's root" means direct membership only, or any
   `GroupsFinder`-visible path including shared and invited groups. Pending and expired
   invitations and access-level downgrades add further edge cases.

2. **Sibling-subgroup data isolation.** Granting access to `1/100/200/` must never
   leak data from `1/100/300/`. The GKG prefix filter (`startsWith`) already enforces
   this, and existing unit tests exercise subgroup paths. However, an explicit
   **integration test** covering the subgroup-only path set is required before
   shipping. The test must also pin the trailing-slash invariant: `startsWith` is safe
   from both `1/100/300/` and `1/100/2000/` only because the path format guarantees a
   trailing `/` after each segment (the ID-prefix-collision case `200` vs `2000` is
   the actual leak shape if the invariant breaks). The integration test must also cover
   confidential items under a subgroup-only scope (verifying Layer 3 redaction still
   fires) and global-entity (User, Runner) joins.

3. **Product policy: "No sparse permissions in V1" scope.** `security.md`'s "No sparse
   permissions in V1" refers to project-level and item-level access. The security model
   already documents subgroup-scoped access in Layer 2. This ADR relaxes the Rails
   *usage policy*, not the authorization model. However, V1 now supports sparse
   *namespace-hierarchy* permissions (a user can access a subgroup without holding the
   top-level group), while still excluding project-level and item-level access. The
   team should confirm this product-contract shift and `security.md` should be updated
   to reflect the revised scope.

4. **Enablement vs. entitlement drift.** If a subgroup is usable but its root is not
   enrolled or licensed, gates 2, 3, and 4 can disagree. The implementation must
   maintain a single source of truth for "can this user use Orbit and against what
   scope."

5. **Global entities (User, Runner).** These have no `traversal_path` and rely on
   Rails redaction. They are only reachable through edge table joins that carry
   `traversal_path`, so subgroup users should not gain broader visibility. The
   integration test must confirm this.

6. **Cache correctness (`OrbitLicense` per-user cache).** `OrbitLicense` caches a
   per-user boolean. If the entitlement logic changes to consider subgroup membership,
   the cache key and invalidation strategy must reflect subgroup membership changes.

7. **Mutable namespace topology (group transfers).** Traversal paths like
   `1/100/200/` are mutable: a group transfer changes `traversal_path`. JWT path
   updates immediately on the next request, but indexed ClickHouse rows carry the old
   prefix until CDC and re-indexing catch up. If a subgroup leaves an enrolled root,
   entitlement and billing flip. This likely fails safe (stale rows sit under a prefix
   the user no longer holds), but the implementation must verify that stale rows
   cannot remain queryable to the wrong sibling or root during the convergence window.

### Consequences

What improves:

- Subgroup-level Reporter+ members gain Orbit access scoped to their subgroup subtree,
  without requiring top-level group membership on SaaS.
- No GKG (Rust) change, no re-indexing, no new data pipelines. The change is
  contained in the Rails monolith.
- The existing three-layer security model is preserved. Subgroup access is a
  natural extension of the prefix-based traversal path model, already documented in
  `security.md` and already exercised on GitLab Self-Managed.

What gets harder:

- Billing attribution gains a new case (subgroup-only user) that the current
  governing namespace finder does not handle. The shared/invited-group case
  compounds this.
- V1 now supports sparse namespace-hierarchy permissions on SaaS, while still
  excluding project-level and item-level access. The `security.md` access model
  section should be updated to reflect this revised scope.
- The `OrbitLicense` cache may need finer-grained invalidation if subgroup membership
  changes should immediately affect Orbit access.
- Traversal path count per user may increase. A user with Reporter+ on many scattered
  subgroups produces more distinct prefixes than one holding a single top-level group,
  because the trie compaction cannot merge siblings the user does not hold. Monitor
  the existing `gkg.rails.traversal_ids_computed` metric and the >100-prefix alert
  documented in `security.md`. Beyond the alert, the `MAX_TRAVERSAL_IDS = 500` cap in
  `AuthorizationContext` silently truncates to the first 500 sorted paths, causing
  scope loss rather than an error. Scattered subgroup-only grants raise the likelihood
  of hitting this cap.
- Group transfers change `traversal_path`, creating a convergence window where JWT
  paths and indexed ClickHouse rows disagree.
