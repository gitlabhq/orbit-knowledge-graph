---
title: "GKG ADR 013: Entity and Attribute-Level Channel Gating"
creation-date: "2026-07-28"
authors: [ "@TBD" ]
status: "Draft / RFC — not yet reviewed"
toc_hide: true
---

# Context

Today, Orbit's authorization stack (see [Security](../security.md)) filters purely on **who the user is**:

1. Tenant/org segregation via `traversal_path`.
2. Group-level Reporter+ scope, tightened per entity via `redaction.required_role` (e.g. `Vulnerability`, `Finding`, and other security-domain entities require `security_manager`).
3. Final Rails `Ability.allowed?` redaction for anything row-level filtering can't catch (confidential issues, etc.).

Separately, [ADR 006](./006_dap_integration.md) and the Orbit pricing model differentiate **how a request reaches Orbit** — DAP is zero-rated, external agents (MCP/CLI/REST) meter as GitLab Credits — but that differentiation is billing-only. A user with `security_manager` sees identical `Vulnerability` data whether they ask through DAP or query `glab orbit remote` directly.

**The business problem this ADR actually responds to is broader than security.** Orbit's graph is more granular and more freely composable than several existing paid GitLab surfaces built on overlapping data — the Security/Vulnerability dashboards, dependency and SBOM tooling, Value Stream Analytics, and others. A user who is already authorized to see the underlying rows isn't the concern; the concern is that handing that data out as a general-purpose, queryable graph through direct API/CLI/external-agent access lets a customer (or a third party building on our API) reconstruct the packaged product themselves, on top of Orbit, instead of buying it. That's a monetization and cannibalization risk, not an authorization gap — today's role/tenant stack is working exactly as designed even in the scenario we want to prevent.

At the same time, GitLab wants the opposite constraint to hold internally: Orbit should stay fully available to power DAP, foundational agents, and future GitLab-native features without every new internal use case waiting on a monetization review. Those two goals — restrict what ships broadly through direct customer-facing access, while keeping internal feature development unconstrained — are in tension under a model that only gates by role. Channel gating is the mechanism that lets both hold at once: `internal_only` and `dap`-scoped `channel_allowlist` values keep an entity available for building GitLab-native features immediately, while whether and when it's exposed to `external_agent` becomes a deliberate, per-entity (and, as of this revision, per-attribute) product decision instead of an all-or-nothing default that ships the day an entity is indexed.

The [Orbit 19.0 DoD](https://gitlab.com/gitlab-org/gitlab/-/work_items/593790) lists "Entity-level RBAC for sensitive data e.g. vulnerabilities" as out of scope for public beta — but the *existing* `required_role` mechanism already does role-based entity gating. What's missing, and what this ADR addresses, is a **second, independent dimension**: gating an entity — and now individual attributes on that entity — by which channel is asking, orthogonal to the user's role, and motivated as much by product/monetization strategy as by data sensitivity.

Motivating scenario: an entity type should be usable to power a GitLab-native feature (e.g. surfaced only through DAP or internal tooling) but withheld from a customer calling the REST API or an external MCP agent directly with the same credentials — not because their GitLab role doesn't permit it, but because GitLab hasn't yet decided to offer that data as a general-purpose queryable surface outside its own products. The same reasoning applies at finer grain: an entity might be broadly available while one specific field on it (say, an internal severity heuristic) stays restricted.

**This applies identically on GitLab.com, Self-Managed, and Dedicated.** Channel gating has nothing to do with data tenancy or organization boundaries — `traversal_path` and `organization_id` already own that, unchanged by this ADR. It's about restricting the *surface area* through which any of Orbit's data — the customer's own, already-tenant-scoped data — can be pulled out directly, regardless of who operates the instance.

# Decision

Add a **channel allowlist** (`channel_allowlist`) to both entities and, optionally, individual attributes. Rather than enforcing this with a runtime compiler pass, precompute a **channel-scoped ontology variant** for each raw channel, and select the matching variant before any query compiles. Channel gating can only narrow what role-based access already permits, never widen it — that composition is unchanged from earlier drafts of this ADR; what's changed is *how* the narrowing happens.

## 1. Ontology addition: entities and attributes

```yaml
# config/ontology/nodes/vulnerability.yaml
redaction:
  required_role: security_manager
  channel_allowlist: [internal_only, dap]
attributes:
  cve_id:
    type: string
  exploit_maturity:
    type: string
    redaction:
      channel_allowlist: [internal_only]   # narrower than the entity's own [internal_only, dap]
```

`channel_allowlist` is a list at both levels. Each entry is either a raw channel name (`external_agent`, `dap_internal`, `core_feature`, `frontend`) or a channel group — a named alias for a set of channels. Two groups ship at launch:

| Group | Expands to | Intended use |
|---|---|---|
| `all_interfaces` | `external_agent`, `dap_internal`, `core_feature`, `frontend` | No restriction. Explicit opt-in to "everyone," not an accidental default. |
| `internal_only` | `core_feature` | Backs internal GitLab features/tooling. Not exposed to any agent-facing surface — DAP included. |

There's no dedicated group for "DAP only" — that's just `channel_allowlist: [dap_internal]`, since groups exist purely as a convenience for sets that recur, not as the only way to reference a channel.

**Entity-level default: fails closed.** An entity with an empty or absent `channel_allowlist` resolves to no allowed channels — nobody sees it, not even `core_feature`. Visibility must be explicitly opted into. This is the same posture `required_role` could have taken but didn't (it defaults to `reporter`, i.e. fails open); channel gating deliberately chooses the stricter default, because an unassigned `channel_allowlist` is far more likely to mean "nobody's decided yet" than "no restriction intended."

**Attribute-level default: inherits the entity, and is bounded by it.** An attribute's *effective* channel set is the intersection of its own declared `channel_allowlist` (if any) with its entity's resolved set — an attribute can never be more visible than the row it lives on. If an attribute declares nothing, it simply inherits the entity's resolved set as-is. This is deliberately the opposite default from the entity level: fail-closed at the attribute level would mean annotating every field of every entity before shipping anything, which is a far bigger migration than the entity-level one was. The entity's own gate is already the safety net; per-attribute declarations exist for the exceptional case — one sensitive field inside an otherwise-open entity — not as a second mandatory pass over the whole ontology. The tradeoff this accepts is named explicitly in Consequences below.

`frontend` is deliberately **not** included in `internal_only`, and this is a resolved decision — see Section 9 for the mechanism this implies for GitLab UI features that need `internal_only` data. `channel_allowlist` values and group definitions are **GitLab-defined, not per-org configurable** — an org admin can't loosen or tighten an entity's or attribute's `channel_allowlist` any more than they can today grant themselves a role `required_role` doesn't recognize.

## 2. Channel derivation (Rails-side, trusted, never client-supplied)

Unchanged from the original design: the channel claim is derived by Rails from the auth mechanism itself, never asserted by the caller.

| Channel | Derived from |
|---|---|
| `dap_internal` | Token has `ai_workflows` scope + composite identity (service account + user) |
| `external_agent` | User-authenticated request that isn't DAP or the frontend session — either an OAuth token from an MCP/CLI flow, or a personal access token (`PRIVATE-TOKEN`) on a direct REST call |
| `core_feature` | System JWT, internal Rails service → GKG |
| `frontend` | User JWT (HS256), frontend session |

Rails stamps this into the JWT payload already carrying traversal tuples, exactly as before.

## 3. Channel-scoped ontology variants (replaces a runtime `ChannelPass`)

Earlier drafts of this ADR proposed a compiler pass (`ChannelPass`) that ran per query, mirroring `SecurityPass`, compiling disallowed aliases to `Bool(false)`. That works, but it's the wrong shape for this specific dimension, for a reason worth stating precisely: **channel is static and small, unlike role.** `required_role` needs a runtime pass because it depends on per-user, per-request data (traversal paths, group membership) that's unbounded and can't be precomputed. `channel_allowlist` doesn't have that problem — there are exactly four raw channels, the mapping is GitLab-defined rather than per-org configurable, and it's known the instant the JWT is validated, before any traversal-path resolution happens.

So instead of a runtime pass, precompute **one ontology variant per raw channel** — four total — each containing only the entities and attributes whose *effective* `channel_allowlist` includes that channel. Edges are pruned transitively: any edge whose source or target type isn't present in a given variant is also absent from that variant, so a join can't smuggle access to an excluded node any more than a direct reference could.

At request time, the channel claim on the JWT selects which precomputed variant to compile against — before DSL resolution starts. Nothing about role-based filtering changes: `SecurityPass` still runs, still per-request, still narrowing rows within whatever's addressable. The two dimensions compose as they always did — variant selection narrows the *addressable schema* first (coarse, static), `SecurityPass` narrows *rows* within it (fine-grained, dynamic) — the AND relationship is unchanged, only the channel half moved from filtering to selection.

Variants are computed in-memory at service startup / ontology-reload, not checked into git as generated files — that avoids creating a second "generated artifact must match source" drift check for Section 10 to police. They're derived fresh every reload, so there's nothing to go stale.

This closes the channel-dimension aggregation oracle **by construction** rather than by mitigation — see Section 5.

## 4. Query-time enforcement semantics

However a restricted entity or attribute is reached — a direct `SELECT`, a `WHERE` clause, a join across an edge, a named-query template resolving at runtime — the outcome is now uniform: it isn't part of the ontology being compiled against, so any reference to it fails as an **unknown identifier**, indistinguishable from a genuine typo. `SELECT *` follows the same logic without needing an error: wildcard expansion runs against the caller's variant, so a restricted attribute is simply absent from the expanded column list.

This is a meaningful simplification over the runtime-pass design, which needed two different-shaped outcomes depending on how the restriction was reached (a rejected-as-unknown case for direct references the schema could see coming, and a compiles-to-`Bool(false)`/empty-result case for anything reached indirectly, past static validation). Two different outcomes for two different paths is itself a signal a determined caller could learn to read; collapsing to one outcome for every path removes that distinction rather than just making it harder to notice.

**The response must be identical; the telemetry does not have to be.** Externally, a restricted-field query and a genuinely-nonexistent-field query return byte-identical errors — same code, same message, same timing characteristics. Internally, nothing stops telemetry from tagging *why* an identifier was rejected (channel-restricted vs. actually unknown), and it should: that distinction is valuable for spotting an external agent that's systematically probing for restricted field names, which is a worthwhile alerting signal precisely because the caller can't tell their probing produced a different kind of "no."

This invariant is worth locking down as an explicit test rather than trusting future refactors of error-handling code not to drift it apart — see Section 10.

## 5. Aggregation oracle

`security.md` already documents closing this once for `required_role`: `count(Vulnerability) group_by Project` could leak signal through Reporter-level access before row redaction ran, which is why edge-only aggregation lowering stays disabled for any entity with a `required_role`, keeping the node table in `FROM` so the runtime pass has an alias to filter.

Channel gating doesn't need an equivalent carve-out anymore. A restricted entity or attribute isn't in the compiled variant's ontology at all, so it can never appear in `FROM`, `GROUP BY`, or anywhere else for that channel — there's no lowering decision to make, because it was never a valid target in the first place. This is stronger than the runtime-pass mitigation: that approach *prevented* the leak by disabling an optimization; this approach makes the leak *structurally unreachable*. The `required_role` carve-out is unchanged and still necessary, since role-based filtering is still a runtime concern.

## 6. Schema discovery

`get_graph_schema` / `GetQueryDsl` now return the precomputed schema for the caller's variant directly — there's no per-request filtering to do, so Section 5's original requirement ("must reflect channel visibility per caller") is satisfied by construction rather than by an extra step that could be forgotten. As before, this is a straight omission with no "restricted" annotation: the caller should not be able to tell that gating is happening at all, only that certain identifiers don't exist for them.

## 7. Named queries

With channel handled by variant selection, a named query is compiled against a specific channel's variant from the start — referencing a field outside that variant is simply invalid at compile time, per Section 4, the same as any other field. This mostly supersedes the original motivation for a `current_channel` `$binding` (letting templates apply their own channel-based filtering), since that filtering now happens by construction. `current_channel` can still exist as a server-resolved binding for templates that want to branch on channel for reasons *other* than raw visibility — different formatting or defaults per surface, for instance — but it's no longer load-bearing for enforcement.

## 8. Observability

There's no longer a per-query "channel filter applied" event to count, since channel doesn't filter — it selects. Replace the originally-proposed `gkg.query.channel_filter_applied` counter with:

- `gkg.query.channel_variant` — tagged by which variant compiled the query, a usage metric more than a security one.
- An internal-only tag on unknown-identifier errors distinguishing `channel_restricted` from `genuinely_unknown` (per Section 4) — never exposed externally, used for alerting on probing behavior.

Audit logging continues to record channel alongside traversal prefixes for role-based redactions, unchanged from the original design. The `orbit-analytics` dashboard gets a tile for the internal-only restricted-identifier tag so Product/Security can see how often probing happens and against which fields.

## 9. Frontend access to `internal_only` data

Unchanged from the original design. `frontend`'s JWT is browser-held and inspectable — visible in devtools/network inspection, copyable — unlike `core_feature`'s system JWT or `dap_internal`'s composite-identity token. If `internal_only` included `frontend`, a user could lift that token and query directly, collapsing `internal_only` down to "anyone with a browser."

So the rule stands: **the frontend never holds a token capable of querying `internal_only`-gated entities or attributes directly.** GitLab UI features that need `internal_only` data go through a purpose-built Rails REST or GraphQL endpoint that queries Orbit server-side via `core_feature`, then returns the processed result to the frontend over existing session auth — the same shape as the existing `POST /api/v4/orbit/query` vs. dashboard split already implied by ADR 003.

## 10. CI safeguards: presence/validity linter and widening review gate

Two CI checks, extended to cover attributes now that they carry their own optional `channel_allowlist`.

**Presence/validity linter.** Fails the build if any ontology node has a missing, empty, or invalid `channel_allowlist` (unchanged from the original design), and additionally fails if any *attribute's* declared `channel_allowlist` is not a subset of its entity's resolved set — a declared attribute list wider than its parent entity is a config error, not a policy choice, since the intersection rule in Section 1 would silently clamp it anyway; better to reject it loudly than let the YAML imply a permission that was never actually granted.

**Widening review gate.** A CI job resolves each entity's *and each attribute's* effective channel set on both sides of the MR diff and compares them. If any resolved set **gains** channels — an entity moving `internal_only` → `all_interfaces`, or an attribute's inherited set widening because its parent entity widened, or an attribute's own declared list gaining a channel — the pipeline requires a dedicated approval label restricted to a small product/security approver group. Narrowing never triggers it.

This closes a specific gap the inherit-by-default attribute rule opens: because an unannotated attribute silently rides along with whatever its entity resolves to, an entity-level widening could quietly widen every inheriting attribute at once, including ones nobody had reason to think about when reviewing the entity-level change. The gate has to compute and diff the full resolved attribute set, not just watch for explicit attribute-level YAML edits, or this exact case slips through unreviewed. This is layered on top of file-level CODEOWNERS on `config/ontology/nodes/**`, which can see that the file changed but not what changed inside it.

**New test, per Section 4:** an integration test asserting that querying a channel-restricted identifier and querying a genuinely nonexistent identifier produce byte-identical error responses (code, message, timing shape). This is easy to get right on day one and easy to drift apart as error-handling code gets refactored later — worth locking down explicitly rather than trusting code review to catch a wording change.

# Alternatives considered

**A. Gate at the Rails routing layer only (Seam A/B in [Duo/Orbit prompt routing](../duo_orbit_prompt_routing.md)).** Rejected as insufficient: those seams are binary per-surface (Orbit reachable or not for this flow) and already serve a different purpose (feature rollout, not entity-level data control). They can't express "this entity, or this one attribute, for this channel."

**B. Post-hoc filtering at Workhorse/REST, after GKG returns results.** Rejected: duplicates redaction logic outside the ontology-driven compiler, invites drift between what the compiler thinks is authorized and what the edge layer strips, and does nothing to prevent the aggregation oracle.

**C. Maintain a separate "public" ontology/schema subset for external channels.** Rejected on the same grounds the named-queries design already rejected client-authored DSL strings: two schemas drift by construction. One ontology, annotated per-entity and per-attribute, precompiled into variants, is the version that can't fall out of sync with itself.

**D. Let the client declare its own channel via a request header.** Rejected outright — this breaks the core trust invariant that authorization inputs must be server-derived, never client-supplied, the same principle already codified in the `$binding` vs. `$param` split for named queries.

**E. A runtime `ChannelPass` compiler pass (this ADR's own earlier draft), instead of precomputed variants.** Superseded rather than rejected outright — it's a correct, workable design, just not the best one available once attribute-level gating is in scope. Two concrete costs it carries that variants don't: it needs the aggregation-oracle carve-out from Section 5 repeated at attribute granularity (keeping restricted attributes reachable enough to filter, which is itself a small structural exposure), and it needs the two-shaped error semantics from Section 4 (direct references rejected as unknown, indirect ones compiled to empty results) rather than one uniform shape. Both of those complications disappear when the restricted element simply isn't in the ontology being compiled against. Runtime filtering remains the right tool for `required_role`, where per-request variability makes precomputation impossible — this alternative is specific to the channel dimension, which is static enough to precompute.

# Consequences

**Positive:** Fine-grained entity *and attribute* control without a runtime compiler pass on the hot path; schema discovery and the aggregation-oracle closure both fall out of the same mechanism "by construction" rather than needing separate enforcement; query-time restriction now has one uniform shape instead of two, which is simpler to reason about and to test; the Section 10 CI safeguards give this a concrete, enforced guardrail against silent misconfiguration and unreviewed widening at both levels.

**Negative / risks:**
- This changes an existing customer-facing promise. Current messaging (design-partner materials, RFP responses) states Orbit surfaces "the same data" regardless of access path, gated only by GitLab role. Channel gating means a Security Manager querying via CLI could see *less* than the same person asking Duo — a real behavior change, not just plumbing, and it needs Product/Legal sign-off before any entity or attribute actually sets `channel_allowlist`.
- **The fail-closed entity default makes this a breaking migration, not an additive one.** The moment channel-scoped variants are generated, every entity with an empty or missing `channel_allowlist` is absent from every variant — including `core_feature`'s, meaning even internal Rails-to-GKG calls would see nothing. This requires a full audit and explicit `channel_allowlist` population across the existing ontology *before* variant generation is enabled, not after. Section 10's presence/validity linter catches this at build time. A reasonable default sweep for the initial migration: set every currently-unrestricted entity's `channel_allowlist` to `[all_interfaces]` explicitly, so the audit is "confirm this is intentional," not "decide from scratch."
- **The inherit-by-default attribute rule is a deliberate softer default, and it has a corresponding blind spot.** An attribute that never declares its own `channel_allowlist` rides along silently with whatever its entity resolves to, including future widening of that entity. The Section 10 widening gate is specifically designed to catch this by diffing *resolved* sets rather than raw YAML edits, but that only works if the gate is actually watching every entity, all the time — a gap in that gate's coverage would let a sensitive field slip open with no attribute-level change to review at all.
- Variant generation moves cost from per-query to per-deploy (regenerate on ontology reload). Given `channel_allowlist` is GitLab-defined and not per-org configurable, the variant count stays fixed at four regardless of ontology size — this optimization specifically depends on that earlier scoping decision and would not hold if channels ever became per-org configurable.
- Adds a second gating dimension to reason about alongside `required_role`, now expressed as a build-time artifact (variants) rather than a runtime pass — different complexity than before, not less complexity, just relocated to a point where it's cheaper to verify.

**Open questions:** none remaining from the original draft — schema-discovery behavior, per-org configurability, and Self-Managed/Dedicated applicability were resolved in an earlier revision; this revision's replacement of the runtime pass with precomputed variants and the addition of attribute-level gating are both fully specified above rather than left open.

# References

- [Security](../security.md) — role floor mechanism this extends
- [ADR 006: Orbit + Duo Agent Platform integration](./006_dap_integration.md) — existing channel/auth/billing table
- [Duo / Orbit prompt routing architecture](../duo_orbit_prompt_routing.md) — the upstream, binary routing layer this composes with
- [ADR 011: Agent command surface](./011_agent_command_surface.md) — schema/command discovery contract affected by Section 6
- [Orbit 19.0 DoD](https://gitlab.com/gitlab-org/gitlab/-/work_items/593790) — "Entity-level RBAC for sensitive data" out-of-scope item this ADR responds to
- [Orbit pricing model](https://gitlab.com/gitlab-org/gitlab/-/work_items/602210) — existing channel-based billing distinction (not data distinction)