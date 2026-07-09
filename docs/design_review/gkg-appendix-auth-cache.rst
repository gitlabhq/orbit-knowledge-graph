.. _gkg-appendix-auth-cache:

Appendix: Traversal-Path Authorization
=======================================

How Traversal ID Filtering Works
---------------------------------

When a user queries the graph, Orbit needs to know which entities (projects,
groups, work items, etc.) that user is allowed to see, so it can inject filter
predicates into the ClickHouse SQL before any data leaves the database.

GitLab encodes group and project hierarchy using **traversal IDs** --- an
ordered list of ancestor IDs from the root group down to the entity.  For
example, a project nested under ``TopGroup > SubGroup > Project`` might have
a traversal path of ``[1, 45, 312]``.  Traversal IDs are the mechanism
GitLab uses throughout the platform to efficiently answer "does this user have
access to this entity?" without walking the full group tree.

Where the Allowed Paths Come From
---------------------------------

Orbit does **not** compute or cache the user's allowed traversal paths itself,
and there is **no Redis (or other) auth cache inside the service**.  Rails owns
authorization.  When Rails mints the request JWT, it embeds the user's allowed
traversal paths directly in the token as
``Claims.group_traversal_ids`` --- a list of ``TraversalPathClaim`` entries,
each tagged with the raw ``Gitlab::Access`` level the user holds at that path.

This means the permission set travels **with the request**.  Orbit reads it
from the validated JWT; there is no per-query round-trip to Rails or lookup
against an external cache to discover what the user can see.

.. note::

   Rails may represent and compact the permission set internally (for example
   as a prefix tree / trie that collapses shared group ancestry) before
   serializing it into the JWT.  Any such trie optimization lives on the Rails
   side.  From Orbit's perspective the allowed paths arrive as claims in the
   token.

How It Fits the Auth Pipeline
-----------------------------

The layered authorization model uses the JWT-supplied paths in the middle:

1. **Org isolation (JWT):** The request is scoped to a single organization at
   the DAP level.  This is a hard boundary --- no cross-org data is ever
   visible.

2. **Traversal ID filtering (from JWT claims):** The ``SecurityStage`` reads
   ``Claims.group_traversal_ids`` into a ``SecurityContext``.  The query
   engine's ``security``, ``enforce``, and ``partition`` compiler passes then
   inject per-entity ``traversal_path`` filters into the generated ClickHouse
   SQL.  Unauthorized rows are excluded at the database level --- they never
   leave ClickHouse.  Because the paths ride in the token, this step needs no
   network call.

3. **Rails redaction (Ability.allowed?):** Results that pass the ClickHouse
   filter go through a final fine-grained permission check.  The
   ``AuthorizationStage`` emits a ``RedactionRequired`` message (carrying the
   ``resource_type`` and the abilities to check, default ``"read"``) back to
   Rails over the **bidirectional gRPC stream**, and the ``RedactionStage``
   drops any resource Rails denies.  This catches edge cases the traversal-path
   filter cannot model (e.g., confidential work items, time-based access
   expiry).  It is a gRPC-stream callback, not an HTTP ``Ability.allowed?``
   request.

Scaling Considerations
-----------------------

- **JWT size:** The permission set is carried in the request token, so users
  with access to very many groups and projects produce larger JWTs.  Rails-side
  compaction (collapsing shared group ancestry) keeps this manageable for most
  users; admin-level users on very large instances are the worst case and need
  benchmarking.
- **No Orbit-side cache to invalidate:** Because Orbit holds no persisted
  permission cache, there is nothing on the Orbit side to invalidate when
  permissions change --- the next token simply carries the updated paths.
- **Redaction round-trips:** The remaining per-request cost is the gRPC-stream
  redaction callback to Rails.  At 100x concurrency this callback volume, not
  an auth cache, is the authorization-path scaling concern.  See Section 3 for
  the scaling analysis.
