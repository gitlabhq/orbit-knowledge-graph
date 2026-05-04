.. _gkg-appendix-auth-cache:

Appendix: Authorization Cache (Prefix-Tree in Redis)
=====================================================

How Traversal ID Filtering Works
---------------------------------

When a user queries the graph, Orbit needs to know which entities (projects,
groups, issues, etc.) that user is allowed to see, so it can inject filter
predicates into the ClickHouse SQL before any data leaves the database.

GitLab encodes group and project hierarchy using **traversal IDs** --- an
ordered list of ancestor IDs from the root group down to the entity.  For
example, a project nested under ``TopGroup > SubGroup > Project`` might have
a traversal path of ``[1, 45, 312]``.  Traversal IDs are the mechanism
GitLab uses throughout the platform to efficiently answer "does this user have
access to this entity?" without walking the full group tree.

Why a Prefix Tree
-----------------

Computing a user's full set of allowed traversal paths on every query would be
expensive --- it requires evaluating the GitLab permission model across every
group and project the user can access.  Instead, Orbit precomputes the allowed
paths and stores them in a **prefix tree** (also called a trie).

A prefix tree is a tree data structure where each node represents one segment
of a key, and paths from root to leaf spell out complete keys.  Keys that
share a prefix share the same tree nodes.  This is a natural fit for
traversal IDs because many projects share the same top-level group ancestry::

    Root
     |
     1 (TopGroup)
     |--- 45 (SubGroup-A)
     |     |--- 312 (Project-X)  -> allowed
     |     |--- 313 (Project-Y)  -> allowed
     |
     |--- 46 (SubGroup-B)
           |--- 500 (Project-Z)  -> allowed

Instead of storing three full paths (``[1,45,312]``, ``[1,45,313]``,
``[1,46,500]``), the prefix tree shares the common prefix ``[1]`` and the
sub-prefix ``[1,45]``, reducing memory significantly.

**Compaction** goes further: if a user has access to an entire subtree (e.g.,
all projects under SubGroup-A), the tree collapses that branch into a single
node rather than enumerating every leaf.  This is especially effective for
admin users or users with broad group-level access.

How It Fits the Auth Pipeline
-----------------------------

The three-layer authorization stack uses the prefix tree at the second layer:

1. **Org isolation (JWT):** The request is scoped to a single organization at
   the DAP level.  This is a hard boundary --- no cross-org data is ever
   visible.

2. **Traversal ID filtering (prefix-tree in Redis):** The query engine looks
   up the user's allowed traversal paths from Redis, then injects a
   ``traversal_path`` filter into the generated ClickHouse SQL.  This means
   unauthorized rows are excluded at the database level --- they never leave
   ClickHouse.  The Redis lookup is fast (single key per user), and the
   prefix-tree structure keeps the cached value compact even for users with
   access to thousands of projects.

3. **Rails redaction (Ability.allowed?):** Results that pass the ClickHouse
   filter are sent back to Rails for a final fine-grained permission check.
   This catches edge cases the prefix tree cannot model (e.g., confidential
   issues, time-based access expiry).

Scaling Considerations
-----------------------

- **Per-tenant size:** The prefix tree grows with the number of groups and
  projects a user can access.  Compaction keeps this manageable for most
  users, but admin-level users on very large instances may produce large
  trees.
- **Cache invalidation:** When permissions change (user added/removed from a
  group, project visibility changed), the cached prefix tree for affected
  users must be invalidated.
- **Redis memory:** At 100x tenants with large permission sets, aggregate
  Redis memory for all cached prefix trees needs benchmarking.  See Section 3
  for the scaling analysis.
