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

On **SaaS**, a user who holds Reporter+ only on a subgroup is denied Orbit access
even though the data for that subgroup is already indexed, the query engine is already
subgroup-aware, and the permission model already scopes the JWT to the user's actual
subgroup path.

On **GitLab Self-Managed**, this denial does not occur. `OrbitLicense.available_for?`
returns early with `::License.feature_available?(:orbit)` when
`gitlab_com_subscriptions` is unavailable, performing no top-level group check.
`GoverningNamespaceFinder` likewise returns `Group.none` off-SaaS.
GitLab Self-Managed subgroup-only users can already use Orbit end-to-end, which serves as
existing production evidence that the GKG query engine handles subgroup-scoped JWT
paths correctly.

The ask is to extend this capability to **SaaS**: let a Reporter+ subgroup member use
Orbit scoped to that subgroup, without requiring top-level group membership.

### Precise diagnosis: entitlement-check scoping, not a permission or licensing gap

Verified against the `gitlab-org/gitlab` default branch as of this document's
creation date.

**Permissions already work for subgroups.** `AuthorizationContext#reporter_plus_traversal_ids`
calls `Search::GroupsFinder(min_access_level: REPORTER)` and publishes the user's
actual traversal paths (including subgroup paths like `1/22/200/`) verbatim into the
JWT (`jwt_auth.rb`: `payload.merge!(context.reporter_plus_traversal_ids)`). There is
no reduction to root, no depth check, and no top-level filter anywhere in the
permission-scoping path. A subgroup-only Reporter already produces a correctly
subgroup-scoped JWT. The DSL permission `read_knowledge_graph` is a flat
`boundary_type: :user` permission and is not hierarchical.

**Enablement already passes for a subgroup-only user whose root is enrolled.**
`AuthorizationContext#has_enabled_namespaces?` computes
`reporter_plus_root_namespace_ids = reporter_plus_group_rows.filter_map { |row| row[:traversal_ids].first }.uniq`,
which extracts the root of each authorized path. For a subgroup member of
`[1, 22, 200]`, `traversal_ids.first` is `1` (the root). It then checks
`EnabledNamespace.for_root_namespace_id(...)`. This asks "is Orbit turned on for this
tenant?", not "does the user have top-level membership?", and it passes when root `1`
is enrolled.

**The sole blocker on SaaS is the entitlement check** in
`OrbitLicense.available_for?`. On SaaS it evaluates
`user.authorized_groups.top_level.any? { |g| g.licensed_feature_available?(:orbit) && namespace_enrollable_or_enrolled?(g) }`.
The key is `user.authorized_groups`: in `app/models/user.rb`, `authorized_groups`
unions direct groups and their **descendants**, shared groups and their descendants,
and project-authorized groups and their ancestors, but it does **not** include
ancestors of directly-authorized groups. For a user with Reporter on subgroup `200`
(under root `1`): `authorized_groups` contains `200` and its descendants but not
root `1`; therefore `authorized_groups.top_level` (`parent_id IS NULL`) is empty;
therefore the entitlement check returns `false`, even though root `1` is licensed
and enrolled. `OrbitLicense` documents this awareness in its own comment: "Self-managed
delegates to the instance license … users with no top-level group memberships … would
otherwise be denied on a fully licensed instance."

### Where root-level assumptions live

The three-layer authorization model documented in
[`docs/design-documents/security.md`](../security.md) is prefix-based on
`traversal_path` and does not distinguish between a top-level group path and a subgroup
path. `security.md` itself documents subgroup-scoped access explicitly in the Layer 2
example:

> User has Reporter+ on subgroup `[100, 200]` - Can access resources with
> traversal_ids starting with `[100, 200]`, but NOT resources under sibling group
> `[100, 300]`.

The "No sparse permissions in V1" stance in `security.md` refers specifically to
project-level and item-level access (for example, access to a single project without
group access), not to subgroup-level group access.

Four SaaS-side Rails touchpoints reference the root namespace. Only the third is the
blocker:

| # | Touchpoint | Location | Role | Subgroup-only user? |
|---|------------|----------|------|---------------------|
| 1 | **Enablement** | `enabled_namespace.rb` | Tenant on/off: "Only top-level groups can be indexed" | **Passes**: subgroup data is indexed under the enrolled root |
| 2 | **Enablement re-check** | `AuthorizationContext#has_enabled_namespaces?` | Checks `EnabledNamespace` for the root of each authorized path | **Passes**: `traversal_ids.first` resolves to the enrolled root |
| 3 | **Entitlement** | `OrbitLicense.available_for?` | Checks `:orbit` license + `namespace_enrollable_or_enrolled?` on `user.authorized_groups.top_level` | **Fails**: `authorized_groups` excludes ancestors of direct memberships, so `top_level` is empty |
| 4 | **Billing attribution** | `GoverningNamespaceFinder` | Resolves the billing namespace from the user's root groups on a paid plan; returns `Group.none` off-SaaS | **Dependent decision**: requires product/SOX alignment |

`SecurityContext` validates path **format** (`^(\d+/)+$`), not depth. Existing unit
tests exercise subgroup paths (`1/22/`, `1/33/`). Partition pruning keys on
`segments[1]` (the top-level namespace ID), which any subgroup path still contains.
ClickHouse already holds subgroup data because indexing dispatches per enabled root
namespace and stores the full traversal hierarchy.

### Validation

The diagnosis was reproduced empirically in a local development environment
against the `gitlab-org/gitlab` default branch. Two users were created under a
single top-level group: one holding Reporter only on a subgroup, the other
holding Reporter on the top-level group itself.

- The subgroup-only user's `authorized_groups.top_level` was **empty**, while
  the top-level member's contained the root group. This is the exact set that
  the SaaS branch of `OrbitLicense.available_for?` iterates, so the entitlement
  check fails for the subgroup-only user.
- For the same subgroup-only user, resolving the root via each authorized path's
  `traversal_ids.first` **did** yield the root namespace — confirming that the
  root is reachable and that broadening the entitlement predicate along this path
  would recognize it.
- `AuthorizationContext#has_enabled_namespaces?` returned **true** for the
  subgroup-only user (it resolves the root via `traversal_ids.first`, not via
  `authorized_groups.top_level`), confirming that enablement (touchpoint 2) is
  not the blocker.

This isolates the limitation to the single entitlement predicate and confirms it
is neither a permission-model nor an enablement limitation. GitLab Self-Managed is
unaffected: its branch delegates to the instance license and does not inspect
group membership.

## Decision

**Broaden the SaaS entitlement check (`OrbitLicense.available_for?`) so entitlement is
satisfied when a user holds Reporter+ on any path whose root namespace is
licensed and enrolled, not only when they directly belong to that top-level group. Keep
root-level indexing and enrollment unchanged, keep the permission and traversal-path
model untouched (no GKG/Rust change), and treat billing attribution as the dependent
product/SOX decision.**

Concretely:

1. **Leave enablement (touchpoints 1 and 2) untouched.** Both already pass for a
   subgroup-only user whose root is enrolled.
2. **Rework touchpoint 3 (`OrbitLicense.available_for?`)** to check entitlement
   against each authorized path's root (for example, by resolving
   `traversal_ids.first` for each Reporter+ path and checking the `:orbit` licensed
   feature and `namespace_enrollable_or_enrolled?` on that root) instead of iterating
   `user.authorized_groups.top_level`. The user's traversal path continues to scope
   them to their subgroup subtree through the existing prefix filter. No new
   permission is granted.
3. **Reconcile touchpoint 4 (governing/billing namespace)** so billing attribution
   can be satisfied by a subgroup membership under an enrolled and licensed root.
   This requires a product and billing decision
   (see [Open questions](#open-questions)).

### Effort

Code effort: approximately 4-6 engineering days, dominated by the Rails entitlement
rework. Cross-functional billing and product decisions are a separate blocking
dependency with unknown timeline.

| Workstream | Effort | Notes |
|------------|--------|-------|
| GKG (Rust) core | 0 days | Already subgroup-aware; no change |
| GKG isolation integration test | 0.5 day | Sibling-subgroup isolation for a subgroup-only path set |
| Rails `OrbitLicense` rework | 1-2 days | Broaden entitlement check to resolve each path's root |
| Rails gate reconciliation | 1-2 days | Single source of truth for "entitled + scope" across touchpoints 3/4 |
| Rails tests | 1 day | Subgroup-member specs across entitlement and billing |
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

**Option C: derive the enrolled root as the governing namespace.** A variant of the
proposed decision where the entitlement predicate resolves the subgroup's containing
enrolled root and uses that root as the governing namespace for billing attribution.
This makes the billing trade-off explicit (all subgroup usage bills to the enrolled
root) without requiring per-subgroup enrollment. Worth evaluating as the concrete
implementation of the touchpoint-4 reconciliation.

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
   proposed entitlement predicate, be entitled through a root they have no membership
   in, with usage billed to company A. The entitlement predicate must define whether
   "the path's root" means direct membership only, or any `GroupsFinder`-visible path
   including shared and invited groups. Pending and expired invitations and
   access-level downgrades add further edge cases.

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
   already documents subgroup-scoped access in Layer 2. This ADR broadens the
   *entitlement policy*, not the authorization model. However, V1 now supports sparse
   *namespace-hierarchy* entitlements (a user can be entitled through a subgroup
   without holding the top-level group), while still excluding project-level and
   item-level access. The team should confirm this product-contract shift and
   `security.md` should be updated to reflect the revised scope.

4. **Enablement vs. entitlement drift.** If a subgroup is usable but its root is not
   enrolled or licensed, the entitlement check and billing attribution can disagree.
   The implementation must maintain a single source of truth for "can this user use
   Orbit and against what scope."

5. **Global entities (User, Runner).** These have no `traversal_path` and rely on
   Rails redaction. They are only reachable through edge table joins that carry
   `traversal_path`, so subgroup users should not gain broader visibility. The
   integration test must confirm this.

6. **Cache correctness (`OrbitLicense` per-user cache).** `OrbitLicense` caches a
   per-user boolean with differentiated TTLs (30 minutes positive, 2 minutes
   negative). If the entitlement logic changes to consider subgroup membership, the
   cache must correctly reflect subgroup membership changes in the negative-TTL
   window.

7. **Mutable namespace topology (group transfers).** Traversal paths like
   `1/100/200/` are mutable: a group transfer changes `traversal_path`. JWT path
   updates immediately on the next request, but indexed ClickHouse rows carry the old
   prefix until CDC and re-indexing catch up. If a subgroup leaves an enrolled root,
   entitlement and billing flip. This likely fails safe (stale rows sit under a prefix
   the user no longer holds), but the implementation must verify that stale rows
   cannot remain queryable to the wrong sibling or root during the convergence window.

### Consequences

What improves:

- Subgroup-level Reporter+ members gain Orbit entitlement on SaaS scoped to their
  subgroup subtree, without requiring top-level group membership.
- No GKG (Rust) change, no re-indexing, no new data pipelines. The change is a
  single-predicate rework in the Rails entitlement check.
- The existing three-layer security model and traversal-path permission scoping are
  preserved. Subgroup access is a natural extension of the prefix-based model, already
  documented in `security.md` and already exercised on GitLab Self-Managed.

What gets harder:

- Billing attribution gains a new case (subgroup-only user) that the current
  governing namespace finder does not handle. The shared/invited-group case
  compounds this.
- V1 now supports sparse namespace-hierarchy entitlements on SaaS, while still
  excluding project-level and item-level access. The `security.md` access model
  section should be updated to reflect this revised scope.
- The `OrbitLicense` cache may need adjusted invalidation if subgroup membership
  changes should immediately affect Orbit entitlement.
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
