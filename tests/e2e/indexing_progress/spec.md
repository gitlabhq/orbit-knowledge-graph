# Indexing Progress E2E Test Specification

Tests the full indexing progress pipeline: data seeding via Rails API, Siphon CDC
replication, GKG dispatch + indexing, post-ETL count queries, NATS KV writes, and
the `GetIndexingStatus` RPC / REST API read path.

## Prerequisites

- GDK running with EE Ultimate license
- ClickHouse, PostgreSQL, Siphon, NATS all running
- GKG webserver, dispatcher, and indexer running with indexing progress support
- `knowledge_graph` feature flag enabled
- PAT at `~/.gdk_token`

## Test 1: Initial Backfill (from scratch)

**Goal:** Verify that enabling a new namespace and running the indexer produces
correct counts and state in the indexing progress KV.

**Steps:**
1. Create a top-level group via API
2. Create a subgroup under it
3. Create 2 projects under the top group
4. Create 3 merge requests across the projects
5. Create 2 issues (work items)
6. Enable the namespace for KG indexing
7. Wait for Siphon CDC replication (~15s)
8. Run dispatch + wait for indexer to complete
9. Call `GET /api/v4/orbit/indexing_status?traversal_path=<org>/<ns>/`

**Expected:**
- `state` = `idle`
- `initial_backfill_done` = `true`
- `stale` = `false`
- `domains` contains entries for each ontology domain
- `domains[core].items` includes Group (count >= 2), Project (count >= 2)
- `domains[code_review].items` includes MergeRequest (count >= 3)
- `domains[plan].items` includes WorkItem (count >= 2)
- `edge_counts` has entries for IN_PROJECT, CONTAINS, AUTHORED, etc.

## Test 2: Hierarchy Rollup

**Goal:** Verify counts roll up correctly from subgroups to parent groups.

**Steps:**
1. Using the namespace from Test 1, create a project under the subgroup
2. Wait for CDC + indexing
3. Call `GET /api/v4/orbit/indexing_status?traversal_path=<org>/<ns>/` (parent)
4. Call `GET /api/v4/orbit/indexing_status?traversal_path=<org>/<ns>/<sub>/` (child)

**Expected:**
- Parent traversal_path counts include child's entities
- Child traversal_path counts only include its own entities
- Parent project count = child project count + projects directly under parent

## Test 3: Incremental SDLC Updates

**Goal:** Verify that creating new entities after initial backfill increments counts.

**Steps:**
1. Record current counts from the indexing status endpoint
2. Create 2 new merge requests via API
3. Create 1 new issue via API
4. Wait for CDC + indexing cycle
5. Call indexing status endpoint again

**Expected:**
- MergeRequest count increased by 2
- WorkItem count increased by 1
- `updated_at` timestamp is more recent than before
- `state` cycles through indexing -> idle

## Test 4: Code Indexing Progress

**Goal:** Verify code indexing status appears after pushing code.

**Steps:**
1. Push a commit with source files (e.g., 3 Ruby files with classes/methods)
   to a project in the test namespace
2. Wait for code indexing to trigger and complete
3. Call indexing status endpoint

**Expected:**
- `code.projects_indexed` >= 1
- `code.projects_total` matches total project count
- Code-related node counts appear (File, Definition, etc.)
- Code-related edge counts appear (CONTAINS, DEFINES, etc.)

## Test 5: State Transitions

**Goal:** Verify the state machine transitions correctly.

**Steps:**
1. Check status before any indexing runs -> should be `pending`
2. Trigger dispatch, check during indexing -> should be `indexing`
3. Wait for completion -> should be `idle`
4. Verify `initial_backfill_done` transitions from false to true

**Expected:**
- State transitions: pending -> indexing -> idle
- `initial_backfill_done` is monotonic (once true, stays true)

## Test 6: Staleness Detection

**Goal:** Verify the stale flag works.

**Steps:**
1. Get indexing status immediately after indexing -> `stale` = false
2. Wait beyond the staleness threshold (120s) without indexing
3. Get indexing status again -> `stale` = true

**Expected:**
- `stale` = false when `updated_at` is recent
- `stale` = true when `updated_at` is older than 120s

## Test 7: Debounce (10s)

**Goal:** Verify rapid dispatch cycles don't flood KV.

**Steps:**
1. Trigger 3 rapid dispatch+index cycles within 10 seconds
2. Check NATS KV revision for the counts key

**Expected:**
- KV revision should not increase by 3 (debounce skips intermediate writes)
- Only 1-2 KV puts should occur within the 10s window

## Test 8: Edge Count Accuracy (uniq vs count)

**Goal:** Verify edge counts use HLL dedup and approximate FINAL counts.

**Steps:**
1. After initial backfill, get counts from indexing status (KV path)
2. Get counts with `exact_counts=true` (live FINAL queries)
3. Compare

**Expected:**
- Difference between KV counts and exact counts is < 2% for most edge types
- No edge type has > 5% error

## Test 9: Empty Namespace

**Goal:** Verify behavior for a namespace with no indexed data.

**Steps:**
1. Create a new top-level group
2. Enable it for KG indexing
3. Call indexing status before any dispatch runs

**Expected:**
- `state` = `pending`
- `initial_backfill_done` = false
- All counts are 0
- `stale` = true (no `updated_at`)

## Test 10: Authorization

**Goal:** Verify traversal path authorization is enforced.

**Steps:**
1. Create a non-admin user with access to only one subgroup
2. Generate a PAT for that user
3. Call indexing status with a traversal_path the user can access -> should succeed
4. Call indexing status with a traversal_path the user cannot access -> should fail

**Expected:**
- Authorized path returns 200 with data
- Unauthorized path returns 403/PermissionDenied
