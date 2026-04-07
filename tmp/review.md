Overall, I think this is a **good design direction** and a much stronger document than the initial version. The core architectural choices feel right for this system:

- indexer-owned rather than webserver-owned,
- reconciliation-based rather than startup-blocking,
- durable truth in ClickHouse with NATS used for coordination,
- and explicit separation between additive preparation, convergence, and destructive finalization.

I also think the document improved meaningfully in response to feedback. The new material on ClickHouse non-OLTP semantics, the `graph.sql`/migration authoring contract, ontology co-evolution, runtime compatibility modes, and watermark/checkpoint interaction all make the proposal substantially more grounded.

That said, I still would not call the design fully implementation-ready. My recommendation is **directionally approve, but keep this in “needs discussion / tighten before build” territory** rather than treating it as done.

## What is strong

### 1. The ownership boundary is correct

Making this **indexer-owned** is the right choice. The indexer already owns:

- ClickHouse writes,
- distributed work execution,
- NATS-based coordination,
- and the operational reality of reindex/backfill.

Trying to push this into startup hooks or the webserver would create worse failure modes and muddier ownership.

### 2. Reconciliation is the right execution model

Not tying schema progression to pod startup/readiness is a very important choice. This avoids a whole class of rollout failures where temporary ClickHouse/NATS issues wedge deploys. For a distributed, eventually convergent system, “continuously reconcile durable state toward desired state” is a much more realistic model than “run migration once and assume the world is now correct.”

### 3. The phasing is sensible

The V1 → V2 → V3 decomposition is one of the strongest parts of the proposal.

- **V1** stays focused on additive DDL and durable tracking.
- **V2** handles the genuinely hard part: distributed convergence/backfill.
- **V3** keeps destructive cleanup explicitly separate.

That is the right decomposition both technically and operationally.

### 4. The document now has a much better compatibility story

The newly added runtime compatibility contract is helpful. Having an explicit notion of `Legacy` / `DualWrite` / `ReadNew` / `NewOnly` is much better than leaving reader/writer behavior implicit. Likewise, moving finalization toward standalone migrations by default is the clearer operational model.

## Main design concerns that still remain

### 1. The ClickHouse control-plane model is better explained now, but still not fully convincing

The new control-plane semantics section is a clear improvement, but I still think it understates how careful the design must be when using ClickHouse as a migration ledger.

What is good now:

- it explicitly says ClickHouse is not being treated as an OLTP store;
- it explains the design in terms of append-only writes + single-writer serialization + idempotency;
- it at least acknowledges the `FINAL` / `argMax` tradeoff.

What still feels under-specified:

- The doc still uses `FINAL` as the default authoritative read path in several places. That is probably acceptable for `gkg_migrations`, but the design should be firmer about where `FINAL` is acceptable, where projection/aggregation is preferred, and which query shapes are normative.
- `_version = now64(6)` is probably good enough under single-writer assumptions, but it is still a wall-clock token standing in for logical sequencing. That is okay only because the design is constraining writers so hard. I would like that limitation stated even more explicitly.
- The V2 scope table inherits the same semantics but introduces many more writes and more operational querying. Saying “we can add an `argMax` projection if needed” still sounds a bit like an escape hatch rather than a deliberate design.

My take: this is probably workable, but the doc should be clearer that this is a **small-control-plane-on-ClickHouse** design with carefully constrained access patterns, not a generic mutable state store.

### 2. The lock/lease model is improved, but still not fully future-proof

The lease-refresh explanation is much better than before, especially the part about compare-and-swap on revision and forcing the reconciler to stop before its next operation if refresh fails.

But I still think the design is relying on an important assumption without boxing it in hard enough:

- that every lock-protected operation is safe under duplicate or stale execution,
- and that no future migration work will accidentally violate that assumption.

For V1 additive DDL, that seems fine.
For V2/V3, I am less comfortable.

Examples:

- convergence declarations,
- state transitions with operational meaning,
- finalization approval / destructive cleanup,
- any future operation that is not purely idempotent metadata DDL.

The document says this lock is leader election rather than strong fenced mutual exclusion, which is the right honest framing. But if that is the contract, I think the doc should go one step further and state that **the framework only permits operations whose effects remain correct under duplicated or delayed execution**, otherwise fencing must be introduced.

Right now that constraint is implied, but it should be elevated to a first-class invariant.

### 3. V2 still carries the most unresolved correctness risk

This is still the part of the design that feels least settled.

The addition of checkpoint `schema_version` is a good improvement, and I agree with making produced-data schema versioning a core V2 requirement rather than a follow-up. But I do not think the current doc fully proves that “scope converged” means what it needs to mean.

A few places where the design still feels soft:

- **Scope discovery completeness.** The doc now explicitly calls out the risk that checkpoint-derived discovery may miss graph data with stale/missing checkpoints. That is good, but this is important enough that I’m not sure it belongs only in open questions. If checkpoint completeness is a hard prerequisite, say so. If not, define the fallback.
- **Correctness source of truth.** The design says a scope is converged when `converge_scope()` completes and the checkpoint `schema_version` is updated. That is a reasonable operational signal, but it is still a proxy. The doc should be candid that this assumes handlers update checkpoint version only after the relevant writes are durably complete and representative of the whole scope.
- **Scope taxonomy.** Namespace for SDLC and project+branch for code might be sufficient for initial migrations, but the doc itself now hints at migrations that are table-specific or global-but-batchable. That suggests the current abstraction may not be the whole story.
- **Backfill idempotence and duplicate dispatch.** The examples are better than before, especially replacing the enum sentinel with `Nullable`, but the framework-level guarantees for repeated dispatch / partial progress still feel more asserted than demonstrated.

I think V2 is directionally correct, but I would still describe it as **partially designed** rather than fully nailed down.

### 4. Runtime compatibility is clearer, but the cutover contract still needs one more pass

The compatibility mode section is one of the most useful additions. It makes the design much easier to reason about.

My remaining concern is that the contract is still framed mostly in terms of migration status transitions, without fully addressing who guarantees those transitions are safe with respect to the actual rollout state of binaries.

For example:

- `ReadNew` assumes all data is at target version;
- `NewOnly` assumes all serving binaries understand the post-finalization schema;
- finalization assumes rollout state is compatible with the cleanup.

The docs acknowledge this, but the mechanism is still somewhat procedural (“deploy new release, then reconciler runs”) rather than encoded strongly enough in the control plane. That may be acceptable, but if so, the operational dependency should be made more explicit as a deliberate tradeoff.

### 5. Finalization is conceptually right, but still feels a little inconsistent in detail

I agree with the decision to prefer standalone finalization migrations. That is the clearest model.

What still confused me a bit:

- the trait still carries inline finalization support, even while the text discourages it;
- there is a naming inconsistency between `requires_manual_finalization()` and `requires_manual_approval()` in the prose/example;
- the approval mechanism is still hand-wavy (“NATS KV or ClickHouse record”) for something that governs destructive actions.

I do not think those are fatal, but for a design document, V3 would benefit from a cleaner “default path vs exceptional path” statement.

## Document quality

Overall, the docs are good:

- the structure is logical,
- the phase split is easy to follow,
- and a new team member would understand the broad architecture.

The best parts from a readability perspective are:

- the top-level README,
- the explicit lifecycle/state diagrams,
- the concrete migration examples,
- and the operational/runbook sections.

Where the document could still improve:

- There is some repetition between README/open questions and the phase docs.
- A few sections mix “design decision” and “possible implementation” in a way that makes it harder to tell what is firm versus illustrative.
- Some claims have a stronger tone than the underlying design maturity justifies, especially in V2.

So: clear and useful, but I would still tighten the distinction between **settled invariants** and **working assumptions**.

## Discussion threads

I read all the discussion threads, and overall I think the author’s responses were thoughtful and improved the MR.

### ClickHouse non-OLTP semantics

This was the strongest response. It directly addresses the reviewer’s concern and led to meaningful document improvements. I still want the doc a little firmer about authoritative query patterns and scaling assumptions, but the response itself is good.

### KV watch vs consumer

The answer is reasonable: if the webserver only needs a fan-out invalidation signal and ClickHouse remains the durable source of truth, KV watch is the simpler fit. I agree with that conclusion.

What is still missing is a sharper articulation of the downside of KV beyond “extra overhead is unnecessary” for consumers. For example: what delivery guarantees matter here, what happens on watcher reconnect, and whether missed notifications are harmless because refresh is always re-derived from ClickHouse. The response implies this, but does not fully spell it out.

### Keeping `graph.sql` and migrations in sync

This was also a solid response. The authoring contract + paired CI checks are exactly the right kind of guardrail for the dual-source-of-truth risk. Of the thread responses, this one feels the most complete.

### Ontology + migration co-evolution

The response is good in distinguishing ontology-only changes from schema-affecting changes, and in naming ontology-driven migrations as future work rather than pretending to solve them now.

I do think the reviewer’s meta-point still stands, though: this is not just a documentation issue, it is a product/design boundary the team should align on. So I would treat this as “adequately answered for this MR, but still worth team discussion.”

### Code indexing vs SDLC indexing differences

The response is useful and correctly surfaces the main differences: `_deleted`, reindexing cost, checkpoint model, scope granularity. But it is more of a problem inventory than a design answer.

That is okay if the intent is “this remains open,” but then I would avoid giving the impression that V2 is already broadly settled. This thread, to me, reinforces that parts of V2 still need another design pass.

### Watermark/checkpoint interaction

The new section is a meaningful improvement and addresses an important gap. The key distinction between event-stream watermarking and convergence backfill is well stated.

My remaining hesitation is about whether the code-path reuse for code indexing creates any operational gotchas when convergence tasks and normal indexing tasks interleave. The response is directionally good, but I still think there is room to specify queueing / fairness / duplicate-dispatch behavior more explicitly.

## Open questions: what should stay open vs what should be decided now

I think the document does a better job now of separating resolved questions from still-open ones. That said, I would push for a few items to move out of “open question” status before implementation starts:

1. **Authoritative control-plane read semantics** for both migration-level and scope-level state.
2. **The exact invariant for checkpoint `schema_version` updates** — specifically when a handler is allowed to advance it.
3. **The safety contract for lease expiry / stale lock-holder behavior** — framed as a hard framework invariant, not just implementation commentary.
4. **The finalization approval source of truth** if destructive migrations are actually in scope for V3.

By contrast, I think these are fine to remain open for now:

- self-managed deployment differences,
- whether ontology should eventually drive migrations,
- whether additional scope kinds beyond namespace/project+branch are needed.

## Risks and gaps not fully covered yet

The main ones I still see are:

- **Control-plane drift between “recorded converged” and “actually safe to read new.”**
- **Checkpoint incompleteness or corruption causing stale scopes to be missed.**
- **Interleaving of convergence work and normal indexing work**, especially for code indexing where the same machinery is reused.
- **Operational ambiguity around destructive steps**, particularly if approval and rollout state are not represented in one durable place.
- **Longer-term lock model erosion**, where future migrations add actions that are less obviously idempotent than the current design assumes.

## Recommendation

My overall recommendation is:

- **approve the architecture and direction**,
- but **request follow-up changes / discussion before calling the design implementation-ready**.

If I were summarizing the bar to clear before implementation, it would be:

1. tighten the control-plane semantics section so the ClickHouse ledger/query model is unambiguous;
2. elevate the lease/idempotency constraint into a hard framework invariant;
3. either narrow V2 claims or further specify the correctness model for convergence;
4. clean up V3 details so the destructive-step contract is more consistent.

With those adjustments, I think this would be a solid foundation, especially for a narrow V1.
