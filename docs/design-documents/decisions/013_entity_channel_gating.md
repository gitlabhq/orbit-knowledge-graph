---
title: "GKG ADR 013: Entity-Level Channel Gating"
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

At the same time, GitLab wants the opposite constraint to hold internally: Orbit should stay fully available to power DAP, foundational agents, and future GitLab-native features without every new internal use case waiting on a monetization review. Those two goals — restrict what ships broadly through direct customer-facing access, while keeping internal feature development unconstrained — are in tension under a model that only gates by role. Channel gating is the mechanism that lets both hold at once: `internal_only` and `dap`-scoped `channel_allowlist` values keep an entity available for building GitLab-native features immediately, while whether and when it's exposed to `external_agent` becomes a deliberate, per-entity product decision instead of an all-or-nothing default that ships the day an entity is indexed.

The [Orbit 19.0 DoD](https://gitlab.com/gitlab-org/gitlab/-/work_items/593790) lists "Entity-level RBAC for sensitive data e.g. vulnerabilities" as out of scope for public beta — but the *existing* `required_role` mechanism already does role-based entity gating. What's missing, and what this ADR addresses, is a **second, independent dimension**: gating an entity by which channel is asking, orthogonal to the user's role, and now motivated as much by product/monetization strategy as by data sensitivity.

Motivating scenario: an entity type should be usable to power a GitLab-native feature (e.g. surfaced only through DAP or internal tooling) but withheld from a customer calling the REST API or an external MCP agent directly with the same credentials — not because their GitLab role doesn't permit it, but because GitLab hasn't yet decided to offer that data as a general-purpose queryable surface outside its own products.

# Decision

Add a **channel allowlist** alongside the existing role floor, enforced by a new compiler pass that mirrors `SecurityPass` exactly, and composed with it via AND — channel gating can only narrow what role-based access already permits, never widen it.

## 1. Ontology addition

```yaml
# config/ontology/nodes/vulnerability.yaml
redaction:
  required_role: security_manager
  channel_allowlist: [dap_internal]   # raw channel name — DAP-only, no group needed
```

`channel_allowlist` is a list. Each entry can be either a **raw channel name** (`external_agent`, `dap_internal`, `core_feature`, `frontend`) or a **channel group** — a named alias for a set of channels, defined once and reused across entities. The list resolves to the union of everything its entries expand to. Two groups ship at launch:

| Group | Expands to | Intended use |
|---|---|---|
| `all_interfaces` | `external_agent`, `dap_internal`, `core_feature`, `frontend` | No restriction. Explicit opt-in to "everyone," not an accidental default. |
| `internal_only` | `core_feature` | Backs internal GitLab features/tooling. Not exposed to any agent-facing surface — DAP included. |

There's no dedicated group for "DAP only" — that's just `channel_allowlist: [dap_internal]`, a single raw channel name, since groups exist purely as a convenience for sets that come up repeatedly, not as the only way to reference a channel. Mixing is valid too: `channel_allowlist: [internal_only, dap_internal]` is equivalent to `channel_allowlist: [core_feature, dap_internal]`.

`frontend` is deliberately **not** included in `internal_only`, and this is a resolved decision rather than an open question — see Section 8 for the mechanism this implies for GitLab UI features that need `internal_only` data. The group→channel-set mapping is a small, centrally maintained table rather than something duplicated per entity, so adding a new group later means adding one row here, not touching every entity that references an existing group.

**Fails closed:** an entity with an empty or absent `channel_allowlist` resolves to no allowed channels — nobody sees it, not even `core_feature`. Visibility must be explicitly opted into per entity, whether via raw channel names, a group, or both. This is the same posture `required_role` could have taken but didn't (it defaults to `reporter`, i.e. fails open); channel gating deliberately chooses the stricter default, because an unassigned `channel_allowlist` is far more likely to mean "nobody's decided yet" than "no restriction intended."

## 2. Channel derivation (Rails-side, trusted, never client-supplied)

The channel claim must be derived by Rails from the auth mechanism itself, the same way `{path, access_level}` tuples are derived today — never asserted by the caller. ADR 006's auth table already implies the mapping:

| Channel | Derived from |
|---|---|
| `dap_internal` | Token has `ai_workflows` scope + composite identity (service account + user) |
| `external_agent` | User-authenticated request that isn't DAP or the frontend session — either an OAuth token from an MCP/CLI flow, or a personal access token (`PRIVATE-TOKEN`) on a direct REST call |
| `core_feature` | System JWT, internal Rails service → GKG |
| `frontend` | User JWT (HS256), frontend session |

`external_agent` is deliberately broad: it's "a customer calling in on their own, however they authenticated," not "specifically MCP." A script hitting `/api/v4/orbit/query` with `PRIVATE-TOKEN` and a Claude Code session hitting the MCP server with OAuth land in the same bucket, because both are the thing this ADR's motivating scenario cares about restricting — direct customer access outside DAP. If a future need arises to distinguish PAT-authenticated scripts from MCP agents specifically, that's a candidate for splitting `external_agent` into two channels (e.g. `external_agent` and `direct_api`) and updating the `all_interfaces` group accordingly — not attempted here since nothing in scope yet needs that resolution.

Rails stamps this into the same JWT payload already carrying traversal tuples:

```json
{
  "traversal_paths": [{"path": "1/100/", "access_level": 20}],
  "channel": "external_agent"
}
```

## 3. Compiler: `ChannelPass`

Runs alongside `SecurityPass`. For each `gl_*` alias, resolves the target entity's `channel_allowlist` (expanding any group entries into raw channels, unioned with any raw channels listed directly) and checks the JWT's `channel` claim for membership in that resolved set. If the channel isn't in it — including the case where `channel_allowlist` is empty or absent, which resolves to the empty set — the alias compiles to `Bool(false)`, reusing the exact mechanism already used when no traversal path clears an entity's `required_role`. Concretely: an entity ships invisible to every channel, including `core_feature`, until someone deliberately populates its `channel_allowlist`.

`CheckPass` is extended to verify every channel-gated alias resolved a valid predicate before codegen, matching its existing behavior for `startsWith` filters.

## 4. Aggregation oracle (same fix as `required_role`)

`security.md` already documents closing this once: `count(Vulnerability) group_by Project` could leak signal through Reporter-level access before row redaction ran. Channel gating needs the identical treatment — edge-only aggregation lowering stays disabled for any entity with a `channel_allowlist`, keeping the node table in `FROM` so the pass has an alias to filter, exactly as done for `required_role` today in `lower.rs`.

## 5. Schema discovery

`get_graph_schema` / `GetQueryDsl` must reflect channel visibility per caller, not return a static schema. Open question (see below): omit gated entities entirely, or annotate them as present-but-restricted. Either is better than today's implicit behavior of returning `Bool(false)` rows with no explanation — an external agent burning calls against an entity it can never see is a discoverability bug, not just a security one.

## 6. Named queries

`current_channel` becomes a second server-resolved `$binding`, alongside the existing `current_user_id`, for templates that want to branch on channel without trusting a client-supplied value.

## 7. Observability

Mirror the existing `gkg.query.traversal_filter_applied` counter with `gkg.query.channel_filter_applied`. Extend audit logging to record channel alongside traversal prefixes. Add a tile to the `orbit-analytics` dashboard, which already buckets by source type (REST/MCP/DWS/frontend) — channel-allowlist denials should be visible there so Product can see how often this fires and against whom.

## 8. Frontend access to `internal_only` data

`frontend`'s JWT is fundamentally different in trust posture from `core_feature`'s system JWT or `dap_internal`'s composite-identity token: it's issued to and held by the user's browser, visible in devtools/network inspection, and copyable. If `internal_only` included `frontend`, a user could lift that token and issue Orbit queries directly against the query API, indistinguishable from a legitimate frontend request — collapsing `internal_only` down to "anyone with a browser," which defeats the point of the group.

So the rule is: **the frontend never holds a token capable of querying `internal_only`-gated entities directly, full stop.** If a GitLab UI feature needs to surface `internal_only` data, the frontend doesn't call Orbit itself — it calls a purpose-built Rails REST or GraphQL endpoint, and *that* endpoint queries Orbit server-side using the `core_feature` channel (the system JWT, never sent to the browser), then returns the processed result to the frontend over the existing session auth. The frontend consumes a curated API GitLab controls, not Orbit's general-purpose query surface.

This means every new frontend feature that wants `internal_only` data requires a small amount of net-new Rails API surface — deliberately, since that's the checkpoint where GitLab decides what shape of the data reaches the browser (aggregated counts vs. raw rows, for instance), rather than handing the frontend a general traversal/aggregation capability it could be tricked or coaxed into misusing. This is the same shape as the existing `POST /api/v4/orbit/query` vs. dashboard split already implied by ADR 003 — the dashboard doesn't get raw MCP-style query access either, it goes through Rails-defined endpoints.

# Alternatives considered

**A. Gate at the Rails routing layer only (Seam A/B in [Duo/Orbit prompt routing](../duo_orbit_prompt_routing.md)).** Rejected as insufficient: those seams are binary per-surface (Orbit reachable or not for this flow) and already serve a different purpose (feature rollout, not entity-level data control). They can't express "this entity only, for this channel."

**B. Post-hoc filtering at Workhorse/REST, after GKG returns results.** Rejected: duplicates redaction logic outside the ontology-driven compiler, invites drift between what the compiler thinks is authorized and what the edge layer strips, and does nothing to prevent the aggregation oracle.

**C. Maintain a separate "public" ontology/schema subset for external channels.** Rejected on the same grounds the named-queries design already rejected client-authored DSL strings: two schemas drift by construction. One ontology, annotated per-entity, is the only version that can't fall out of sync with itself.

**D. Let the client declare its own channel via a request header.** Rejected outright — this breaks the core trust invariant that authorization inputs must be server-derived, never client-supplied, the same principle already codified in the `$binding` vs. `$param` split for named queries.

# Consequences

**Positive:** Fine-grained entity control without re-architecting the compiler; reuses a mechanism already proven for `required_role`; auditable via existing telemetry patterns.

**Negative / risks:**
- This changes an existing customer-facing promise. Current messaging (design-partner materials, RFP responses) states Orbit surfaces "the same data" regardless of access path, gated only by GitLab role. Channel gating means a Security Manager querying via CLI could see *less* than the same person asking Duo — a real behavior change, not just plumbing, and it needs Product/Legal sign-off before any entity actually sets `channel_allowlist`.
- **The fail-closed default makes this a breaking migration, not an additive one.** The moment `ChannelPass` ships, every entity in `config/ontology/nodes/**` with an empty or missing `channel_allowlist` goes dark for all channels — including `core_feature`, meaning even internal Rails-to-GKG calls would see nothing. This requires a full audit and explicit `channel_allowlist` population across the existing ontology *before* `ChannelPass` is enabled, not after. Rollout needs a safeguard (feature-flagged pass, or a required-in-CI lint that fails the build if any node has an empty `channel_allowlist` or references an unknown channel/group name) to prevent an entity silently going blind because someone forgot the field — the same risk `security.md` already calls out for `required_role`, but fail-closed makes the blast radius "everyone" instead of "unprivileged roles." A reasonable default sweep for the initial migration: set every currently-unrestricted entity's `channel_allowlist` to `[all_interfaces]` explicitly, so the audit is "confirm this is intentional," not "decide from scratch."
- Schema-discovery UX is unresolved (see below) and matters for agent usability — doubly so now, since a fail-closed default means *newly added* entities will be invisible-by-default too, and schema responses need to make that legible rather than look like a bug.
- Adds a second gating dimension to reason about alongside `required_role` — compiler and audit complexity both increase.

**Open questions:**
- Should `get_graph_schema` omit channel-gated entities for a restricted caller, or show them with a "requires internal channel" annotation? Omitting is cleaner for agents (no dead-end tool calls) but weakens discoverability/transparency for legitimate debugging.
- Does `channel_allowlist` need to be configurable per-org (namespace-level policy), or is a fixed, GitLab-defined value per entity sufficient for launch?
- How does this interact with self-managed/Dedicated, where "internal feature" vs. "external" may map differently onto the deployment's own auth model?
- What's the build-time safeguard against silent omission? The named-queries system already fails the build when a template drifts from the DSL/ontology (compiled and validated against `config/schemas/named_query.schema.json`); `channel_allowlist` likely needs the same treatment — a CI check that every ontology node has a non-empty `channel_allowlist` whose entries all resolve to a known channel or group name, so "forgot to add it" or "typo'd a channel name" produces a loud build failure rather than a quietly blind (or wrongly open) entity in production.

# References

- [Security](../security.md) — role floor mechanism this extends
- [ADR 006: Orbit + Duo Agent Platform integration](./006_dap_integration.md) — existing channel/auth/billing table
- [Duo / Orbit prompt routing architecture](../duo_orbit_prompt_routing.md) — the upstream, binary routing layer this composes with
- [ADR 011: Agent command surface](./011_agent_command_surface.md) — schema/command discovery contract affected by Section 5
- [Orbit 19.0 DoD](https://gitlab.com/gitlab-org/gitlab/-/work_items/593790) — "Entity-level RBAC for sensitive data" out-of-scope item this ADR responds to
- [Orbit pricing model](https://gitlab.com/gitlab-org/gitlab/-/work_items/602210) — existing channel-based billing distinction (not data distinction)