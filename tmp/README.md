# MR !783 review summary

## What the MR proposes

This MR proposes a phased schema migration framework for the GKG indexer so schema evolution stops being a manual deployment concern and becomes an indexer-owned, version-tracked reconciliation process.

The design is split into three phases:

- **V1** introduces a Rust migration registry, a ClickHouse-backed migration ledger, and a NATS KV lock so additive DDL can be applied automatically and durably tracked.
- **V2** adds per-scope convergence tracking so migrations that require backfill or reindexing can progress over time, with scheduler-integrated dispatch and runtime compatibility modes for writers/readers.
- **V3** separates destructive cleanup/finalization from convergence so old schema structures can be removed only after a soak window and explicit safety checks.

The supporting operational document covers deployment behavior, observability, failure handling, and manual recovery procedures.

## Overall assessment

This is a strong design direction. The ownership boundary is correct, the phased rollout is sensible, and the expand/migrate/contract model fits the problem much better than startup-blocking or one-shot migrations. The document has also improved materially in response to reviewer feedback: it now does a better job explaining ClickHouse's non-OLTP semantics, the authoring/CI contract around `config/graph.sql`, runtime compatibility expectations, and watermark/checkpoint interaction.

That said, I do **not** think the design is fully implementation-ready yet. The remaining issues are not cosmetic; they are mostly about proving correctness at the control-plane boundaries and making the operational contract unambiguous.

The biggest remaining gaps are:

- the ledger/query model still leans too casually on `FINAL` and does not fully specify the authoritative read/write patterns for control-plane state at scale;
- the NATS KV lock/lease story is better than before, but still stops short of clearly defining what happens around lease expiry, stale lock-holders, and any future non-idempotent work;
- V2 still leaves too much of the convergence truth model to later implementation detail, especially around scope discovery completeness and how checkpoint `schema_version` interacts with real data correctness;
- deployment compatibility and finalization safety are improved, but the design still relies on assumptions about rollout ordering that are not encoded strongly enough in the framework itself.

## Recommendation

**Needs discussion**.

I would not reject the direction; I think the architecture is fundamentally sound and V1 in particular looks like a good foundation. But before treating this as accepted/implementation-ready, I would want the design tightened around control-plane semantics, convergence correctness, and rollout/finalization safety.

If the team wants to move forward incrementally, my suggestion would be:

1. accept the broad architecture and phased decomposition;
2. tighten the V1 semantics section further so the durable-state and lock guarantees are explicit rather than implicit;
3. treat parts of V2 as still requiring a follow-up design pass instead of presenting them as fully settled.
