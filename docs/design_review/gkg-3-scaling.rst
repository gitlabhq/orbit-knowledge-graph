.. _gkg-scaling:

3. Multi-Tenancy, Scaling, and COGS
====================================

Tenancy Isolation Model
-----------------------

**Three-layer authorization stack:**

1. **Org isolation** --- requests are scoped to a single organization at the
   DAP/JWT level.
2. **Traversal ID filtering** --- the query engine injects traversal_path
   filters into every ClickHouse query.  The allowed traversal paths are
   delivered from Rails in the request JWT (no Orbit-side cache); see
   :ref:`Appendix: Authorization Cache <gkg-appendix-auth-cache>`.
3. **Rails redaction** --- after ClickHouse returns results, global IDs are
   extracted and sent to the Rails redaction service
   (``Ability.allowed?``) for final permission check.

Primary Scaling Axes
--------------------

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Axis
     - Mechanism
     - Bottleneck
   * - **Repository count (code index)**
     - Incremental file fetch from Gitaly; event-driven reconciliation
     - Gitaly throughput; ClickHouse insert rate
   * - **SDLC entity volume**
     - Siphon CDC with keyset pagination; per-entity watermarks for retry
     - PostgreSQL CDC lag; NATS JetStream throughput; ClickHouse insert rate
   * - **Concurrent search users**
     - Orbit is stateless; scale horizontally behind DAP
     - ClickHouse query concurrency; Puma thread blocking (Section 5)
   * - **Query complexity**
     - Semi-Join Pushdown (SIP), edge-centric traversal, keyset pagination
     - ClickHouse scan time for deep multi-hop traversals
   * - **Auth claim size**
     - Allowed traversal paths delivered per request in the JWT
     - JWT payload size for broad-access users; Rails-side cost of computing
       the path set

Behavior at 10x and 100x
~~~~~~~~~~~~~~~~~~~~~~~~~

**10x load** (10x repositories, 10x SDLC entities, 10x concurrent searches):

- **Orbit compute:** Horizontal --- add more stateless pods behind DAP.
  Indexing throughput scales with pod count.
- **ClickHouse Cloud:** Storage grows linearly.  Code index ~10-50 MB per
  medium project; SDLC index depends on entity volume.  Query load at 10x is
  within ClickHouse Cloud default tier capacity.
- **NATS JetStream:** 10x CDC events is well within NATS capacity.
- **Puma threads:** 10x concurrent searches hold 10x Puma threads.
  Pressure but not exhaustion.
- **Auth claims:** Per-request JWT carries the user's allowed traversal
  paths.  At 10x, payload size and Rails-side path computation are modest.

**100x load** (100x repositories, 100x SDLC entities, 100x concurrent searches):

- **Orbit compute:** Still horizontal, but 100x indexing requires managing
  concurrency and prioritization (active repos first, stale repos deferred).
  SDLC indexing at 100x creates sustained ETL load on the Rust engine.

- **ClickHouse Cloud cost --- open question.** At 100x, ClickHouse Cloud
  becomes the dominant COGS line item:

  - **Storage:** 100x repos + SDLC entities.  Code index is modest (1-5 GB
    columnar), but SDLC entities (work items, MRs, pipelines, vulnerabilities,
    notes across all projects) can be much larger.  ClickHouse Cloud charges
    for replicated storage.
  - **Query compute:** 100x concurrent search queries hitting ClickHouse.
    The query engine uses SIP and keyset pagination to limit scan cost, but
    sustained concurrency drives ClickHouse Cloud burst pricing.
  - **Insert compute:** Continuous SDLC CDC replication + code re-indexing
    generates sustained insert load.  Need benchmarking against ClickHouse
    Cloud tier limits.
  - **Egress:** Results flow from ClickHouse -> Orbit -> redaction service ->
    Rails.  At 100x this egress is non-trivial.

- **Puma thread exhaustion.** The synchronous Rails -> DAP -> Orbit ->
  ClickHouse -> redaction -> Rails flow blocks a Puma thread per request.  At
  100x concurrent searches, this exhausts the Puma pool and degrades all Rails
  request handling.  This is the **primary scaling wall** (see Section 5).

- **Auth claim size.** At 100x tenants with large permission sets, the set of
  allowed traversal paths carried in each JWT may become significant for
  broad-access users, and Rails must compute that set per request.  Worst-case
  claim size and Rails-side computation cost need benchmarking.

- **NATS JetStream.** 100x CDC event volume requires sizing the JetStream
  cluster.  Per-entity watermarks enable granular retry but increase JetStream
  state.

.. admonition:: Open question --- ClickHouse Cloud cost model

   The team needs to produce a cost projection for ClickHouse Cloud at
   GitLab.com scale (all customer repos + SDLC entities indexed):

   - Per-query compute pricing under sustained concurrency
   - Storage replication factor and retention cost (code + SDLC)
   - Insert throughput pricing for continuous CDC + code re-indexing
   - Reserved-capacity vs. on-demand pricing for the expected load profile
   - Comparison against self-hosted ClickHouse as a cost ceiling
   - SDLC entity volume growth rate (work items, MRs, notes scale with usage)

Major Cost Drivers
------------------

.. list-table::
   :widths: 25 35 40
   :header-rows: 1

   * - Driver
     - Where
     - How bounded
   * - **ClickHouse Cloud**
     - Storage, query compute, insert compute, egress
     - Dominant COGS at scale.  Pricing model TBD.
   * - **SDLC indexing (ETL)**
     - Rust ETL engine: CDC consume, transform, ClickHouse write
     - Per-entity watermarks enable selective retry; keyset pagination avoids
       full-table scans
   * - **Code indexing**
     - Gitaly fetch, AST parsing, ClickHouse write
     - Incremental fetch with rename detection; file size cap (5 MB)
   * - **Puma threads (Rails)**
     - Synchronous search flow blocks a thread per request
     - Bounded by Puma pool size.  Scaling wall at high concurrency.
   * - **Auth claims (JWT)**
     - Allowed traversal path sets computed by Rails, sent per request
     - Bounded per request; cost shifts to Rails, not Orbit
   * - **Billing pipeline**
     - Snowplow event emission -> CDot credit tracking
     - Proportional to query volume
   * - **No LLM inference cost**
     - Orbit does not call LLMs
     - LLMs call Orbit (via DAP); inference cost is on the caller

Self-Managed Resource Footprint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Full Orbit (bring-your-own-ClickHouse):

- Orbit binary + ClickHouse + NATS
- Customer bears all infrastructure provisioning, operation, and cost
- See Section 2 for Self-Managed delivery risk (non-Omnibus headwind)
