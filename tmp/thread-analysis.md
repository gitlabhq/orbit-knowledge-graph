# Thread analysis for MR !783

## Overall read

There are six substantive reviewer-question threads. On the whole, the author responded constructively and, in most cases, improved the document rather than only replying in-thread. None of the replies are evasive. The remaining issue is less “the author ignored feedback” and more “some answers identify the right direction but still leave meaningful design work open.”

---

## 1. ClickHouse non-OLTP semantics for the migration ledger

**Reviewer question**
- Does ClickHouse support atomic writes here like an OLTP database?

**Author response summary**
- Explicitly says no: ClickHouse is not being used as an OLTP store.
- Frames safety as append-only writes + single-writer serialization + idempotent operations.
- Explains `ReplacingMergeTree` + `_version` + `FINAL`.
- Notes ClickHouse `INSERT` durability once acknowledged.
- Adds a control-plane semantics section to the document.

**Was the question answered?**
- **Mostly yes.** This is the strongest thread response and clearly addresses the original concern.

**What improved in the doc**
- The V1 doc now has a dedicated control-plane semantics section.
- It distinguishes OLTP transactions from the actual required invariants.
- It acknowledges the `FINAL` / `argMax` tradeoff.

**What still feels open**
- The authoritative query model is still not fully crisp for larger scope tables.
- The design still depends on fairly constrained access patterns; that constraint should be stated even more explicitly.
- `_version` as wall-clock ordering is probably acceptable here, but only because of the single-writer assumption.

**Assessment**
- **Adequately answered, with remaining tightening desirable in the design doc.**

---

## 2. NATS KV watch vs JetStream consumer tradeoffs

**Reviewer question**
- Could we use a consumer instead of KV?
- What are the cons of KV?

**Author response summary**
- Says both could work.
- Argues KV watch is simpler because the webserver only needs a “something changed, reload” signal.
- Notes JetStream consumer overhead: stream/consumer management, acks, and consumer-group semantics that do not match the “every pod should refresh” requirement.
- Emphasizes that ClickHouse remains the durable source of truth and the notification is only a trigger.

**Was the question answered?**
- **Partially yes.** The response gives a plausible preference and the core rationale is sound.

**What improved in the doc**
- Mostly in-thread clarification; the underlying architectural distinction is clearer.

**What still feels open**
- The reply does not fully articulate KV’s downsides.
- It would help to say more explicitly that missed or duplicated notifications are harmless because reload always re-derives state from ClickHouse.
- Watcher reconnect semantics and whether an initial snapshot is sufficient are implied, not fully argued.

**Assessment**
- **Reasonable answer, but somewhat under-argued.** Good enough if this remains an implementation choice, not if the team wants a stronger standardization decision.

---

## 3. Keeping `graph.sql` and migrations in sync

**Reviewer question**
- How do we ensure the canonical full schema and migrations stay in sync?

**Author response summary**
- Points to a new authoring contract and CI enforcement section.
- Requires every schema-changing MR to update both `graph.sql` and the migration registry.
- Adds two complementary CI checks:
  1. apply `graph.sql`, then run migrations;
  2. apply only migrations and compare resulting schema with `graph.sql`.

**Was the question answered?**
- **Yes.** This is one of the most complete answers.

**What improved in the doc**
- The risk is now acknowledged explicitly.
- The solution includes both policy and verification.

**What still feels open**
- Very little conceptually. Implementation details remain, but the design answer is good.
- The only optional refinement would be deciding whether `graph.sql` should be represented as migration version 0 in the ledger model.

**Assessment**
- **Adequately answered.** This thread is in good shape.

---

## 4. Ontology + migration co-evolution

**Reviewer question**
- Since ontology is effectively part of the schema used by both indexer and webserver, shouldn’t ontology and migrations evolve together?
- Is this something the team should discuss explicitly?

**Author response summary**
- Adds a README section on the relationship between ontology and migrations.
- Distinguishes ontology-only changes from schema-affecting changes.
- Says schema-affecting changes require ontology + `graph.sql` + migration in the same MR.
- Names ontology-driven migration generation as an interesting but out-of-scope future direction.

**Was the question answered?**
- **Mostly yes for this MR’s scope.** The response clarifies the intended contract.

**What improved in the doc**
- The ontology/migration relationship is now stated explicitly instead of left implicit.

**What still feels open**
- The team-level design question still exists.
- The answer explains current process, but it does not fully resolve whether ontology is just an adjacent artifact or should become the primary schema declaration over time.

**Assessment**
- **Adequately answered for the current proposal, but still strategically open.**

---

## 5. Code indexing vs SDLC indexing migration differences

**Reviewer question**
- Will there be differences and gotchas between code indexing and SDLC indexing for migrations?

**Author response summary**
- Expands the open questions section.
- Calls out differences in storage model (`_deleted`), reindexing cost, checkpoint models, and scope granularity.
- Says V2 should handle both, but implementation details will differ.

**Was the question answered?**
- **Only partially.** The response identifies the main differences, but it is more of a catalog of issues than a design resolution.

**What improved in the doc**
- The README now does a better job acknowledging that “scope” is not uniform across indexing modes.

**What still feels open**
- This is still one of the biggest areas of unresolved design risk.
- The framework does not yet prove that the same convergence abstractions fit both code and SDLC paths equally well.
- The cost and operational behavior of code reindexing in particular could change the practical rollout model.

**Assessment**
- **Not fully answered; correctly left open.** The thread improves the doc by making the uncertainty explicit, but the concern remains materially unresolved.

---

## 6. Watermark/checkpoint interaction during convergence backfill

**Reviewer question**
- What happens to writer behavior with respect to the watermark/checkpoint model, especially given prior work in !446 and !564?

**Author response summary**
- Adds a dedicated V2 section on watermark and checkpoint interaction.
- States the key invariant that convergence backfill must not disrupt normal indexing watermarks.
- Distinguishes SDLC backfill from normal CDC progression.
- Explains that code convergence reuses the normal code indexing task path, so checkpoint updates are expected there.
- Clarifies that dual-write does not affect watermark advancement.

**Was the question answered?**
- **Mostly yes.** This was a meaningful gap and the new section addresses it directly.

**What improved in the doc**
- The distinction between stream progression and convergence work is now much clearer.
- The code vs SDLC handling differences are spelled out.

**What still feels open**
- The operational interaction between convergence-dispatched code reindex tasks and ordinary code indexing tasks could still use more specificity.
- The doc does not yet say much about fairness, queue pressure, or duplicate dispatch handling in mixed workloads.

**Assessment**
- **Adequately answered at the design level, with some operational detail still open.**

---

## Bottom line on the threads

### Fully or mostly adequately answered
- ClickHouse non-OLTP semantics
- `graph.sql` / migration sync contract
- ontology + migration co-evolution (for current scope)
- watermark/checkpoint interaction

### Answered directionally, but still under-argued or open
- KV watch vs consumer tradeoffs
- code indexing vs SDLC migration differences

### Common pattern across threads

The author generally did the right thing: not just replying in prose, but updating the design docs. The main remaining issue is that some responses convert reviewer concerns into explicit open questions rather than fully closing them. That is acceptable, but it means the MR should be read as:

- **solid architectural direction**,
- **improved and substantially clearer than before**,
- but **not fully resolved in every area that matters for implementation**.
