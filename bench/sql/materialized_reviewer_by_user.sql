-- Materialized join table: REVIEWER (User -> MergeRequest), reviewer-first sort key
-- Proof of concept for denormalized join tables.
--
-- Three MVs trigger on inserts to each source table (edge, MR, user).
-- ReplacingMergeTree deduplicates by (traversal_path, mr_id, u_id),
-- keeping the row with the latest _version.

-- 1. Target table
CREATE TABLE IF NOT EXISTS v93_mat_reviewer_by_user (
  -- relationship
  traversal_path     String        DEFAULT '0/'  CODEC(ZSTD(1)),
  relationship_kind  LowCardinality(String)       CODEC(LZ4),

  -- MergeRequest columns
  mr_id              Int64                        CODEC(Delta(8), ZSTD(1)),
  mr_iid             Nullable(Int64)              CODEC(ZSTD(1)),
  mr_title           String                       CODEC(ZSTD(1)),
  mr_state           LowCardinality(String)       CODEC(LZ4),
  mr_source_branch   String                       CODEC(ZSTD(1)),
  mr_target_branch   String                       CODEC(ZSTD(1)),
  mr_draft           Bool                         CODEC(ZSTD(1)),
  mr_project_id      Int64                        CODEC(T64, ZSTD(1)),
  mr_created_at      Nullable(DateTime64(0, 'UTC')) CODEC(ZSTD(1)),
  mr_merged_at       Nullable(DateTime64(0, 'UTC')) CODEC(ZSTD(1)),

  -- User columns
  u_id               Int64                        CODEC(Delta(8), ZSTD(1)),
  u_username          String                       CODEC(ZSTD(1)),
  u_name             String                       CODEC(ZSTD(1)),
  u_state            LowCardinality(String)       CODEC(LZ4),

  -- system
  _version           DateTime64(6, 'UTC'),
  _deleted           Bool                         DEFAULT false
)
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, u_id, mr_id)
SETTINGS index_granularity = 1024;

-- 2. MV: fires on edge insert
CREATE MATERIALIZED VIEW IF NOT EXISTS v93_mat_reviewer_by_user_on_edge
TO v93_mat_reviewer_by_user AS
SELECT
  e.traversal_path AS traversal_path,
  e.relationship_kind AS relationship_kind,
  mr.id AS mr_id,
  mr.iid AS mr_iid,
  mr.title AS mr_title,
  mr.state AS mr_state,
  mr.source_branch AS mr_source_branch,
  mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft,
  mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at,
  mr.merged_at AS mr_merged_at,
  u.id AS u_id,
  u.username AS u_username,
  u.name AS u_name,
  u.state AS u_state,
  greatest(e._version, mr._version, u._version) AS _version,
  (e._deleted OR mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_edge AS e
INNER JOIN v93_gl_merge_request AS mr FINAL ON e.target_id = mr.id
INNER JOIN v93_gl_user AS u FINAL ON e.source_id = u.id
WHERE e.relationship_kind = 'REVIEWER'
  AND e.source_kind = 'User'
  AND e.target_kind = 'MergeRequest';

-- 3. MV: fires on MergeRequest insert (re-materializes when MR properties change)
CREATE MATERIALIZED VIEW IF NOT EXISTS v93_mat_reviewer_by_user_on_mr
TO v93_mat_reviewer_by_user AS
SELECT
  e.traversal_path AS traversal_path,
  e.relationship_kind AS relationship_kind,
  mr.id AS mr_id,
  mr.iid AS mr_iid,
  mr.title AS mr_title,
  mr.state AS mr_state,
  mr.source_branch AS mr_source_branch,
  mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft,
  mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at,
  mr.merged_at AS mr_merged_at,
  u.id AS u_id,
  u.username AS u_username,
  u.name AS u_name,
  u.state AS u_state,
  greatest(e._version, mr._version, u._version) AS _version,
  (e._deleted OR mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_merge_request AS mr
INNER JOIN v93_gl_edge AS e FINAL
  ON e.target_id = mr.id
  AND e.relationship_kind = 'REVIEWER'
  AND e.source_kind = 'User'
  AND e.target_kind = 'MergeRequest'
INNER JOIN v93_gl_user AS u FINAL ON e.source_id = u.id;

-- 4. MV: fires on User insert (re-materializes when user properties change)
CREATE MATERIALIZED VIEW IF NOT EXISTS v93_mat_reviewer_by_user_on_user
TO v93_mat_reviewer_by_user AS
SELECT
  e.traversal_path AS traversal_path,
  e.relationship_kind AS relationship_kind,
  mr.id AS mr_id,
  mr.iid AS mr_iid,
  mr.title AS mr_title,
  mr.state AS mr_state,
  mr.source_branch AS mr_source_branch,
  mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft,
  mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at,
  mr.merged_at AS mr_merged_at,
  u.id AS u_id,
  u.username AS u_username,
  u.name AS u_name,
  u.state AS u_state,
  greatest(e._version, mr._version, u._version) AS _version,
  (e._deleted OR mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_user AS u
INNER JOIN v93_gl_edge AS e FINAL
  ON e.source_id = u.id
  AND e.relationship_kind = 'REVIEWER'
  AND e.source_kind = 'User'
  AND e.target_kind = 'MergeRequest'
INNER JOIN v93_gl_merge_request AS mr FINAL ON e.target_id = mr.id;

-- 5. Backfill: populate from existing data (run once after creating the table)
INSERT INTO v93_mat_reviewer_by_user
SELECT
  e.traversal_path AS traversal_path,
  e.relationship_kind AS relationship_kind,
  mr.id AS mr_id,
  mr.iid AS mr_iid,
  mr.title AS mr_title,
  mr.state AS mr_state,
  mr.source_branch AS mr_source_branch,
  mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft,
  mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at,
  mr.merged_at AS mr_merged_at,
  u.id AS u_id,
  u.username AS u_username,
  u.name AS u_name,
  u.state AS u_state,
  greatest(e._version, mr._version, u._version) AS _version,
  (e._deleted OR mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_edge AS e FINAL
INNER JOIN v93_gl_merge_request AS mr FINAL ON e.target_id = mr.id
INNER JOIN v93_gl_user AS u FINAL ON e.source_id = u.id
WHERE e.relationship_kind = 'REVIEWER'
  AND e.source_kind = 'User'
  AND e.target_kind = 'MergeRequest'
  AND NOT e._deleted;
