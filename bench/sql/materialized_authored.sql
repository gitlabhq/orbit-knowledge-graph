-- Materialized join table: AUTHORED (User -> MergeRequest), FK-backed via mr.author_id
-- Tests whether materialization still wins when the baseline is a direct FK join
-- (no edge table involved). Two MVs: one per source table.

CREATE TABLE IF NOT EXISTS v93_mat_authored (
  traversal_path     String        DEFAULT '0/'  CODEC(ZSTD(1)),

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

  u_id               Int64                        CODEC(Delta(8), ZSTD(1)),
  u_username          String                       CODEC(ZSTD(1)),
  u_name             String                       CODEC(ZSTD(1)),
  u_state            LowCardinality(String)       CODEC(LZ4),

  _version           DateTime64(6, 'UTC'),
  _deleted           Bool                         DEFAULT false
)
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, mr_id)
SETTINGS index_granularity = 1024;

CREATE MATERIALIZED VIEW IF NOT EXISTS v93_mat_authored_on_mr
TO v93_mat_authored AS
SELECT
  mr.traversal_path AS traversal_path,
  mr.id AS mr_id, mr.iid AS mr_iid, mr.title AS mr_title, mr.state AS mr_state,
  mr.source_branch AS mr_source_branch, mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft, mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at, mr.merged_at AS mr_merged_at,
  u.id AS u_id, u.username AS u_username, u.name AS u_name, u.state AS u_state,
  greatest(mr._version, u._version) AS _version,
  (mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_merge_request AS mr
INNER JOIN v93_gl_user AS u FINAL ON mr.author_id = u.id;

CREATE MATERIALIZED VIEW IF NOT EXISTS v93_mat_authored_on_user
TO v93_mat_authored AS
SELECT
  mr.traversal_path AS traversal_path,
  mr.id AS mr_id, mr.iid AS mr_iid, mr.title AS mr_title, mr.state AS mr_state,
  mr.source_branch AS mr_source_branch, mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft, mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at, mr.merged_at AS mr_merged_at,
  u.id AS u_id, u.username AS u_username, u.name AS u_name, u.state AS u_state,
  greatest(mr._version, u._version) AS _version,
  (mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_user AS u
INNER JOIN v93_gl_merge_request AS mr FINAL ON mr.author_id = u.id;

INSERT INTO v93_mat_authored
SELECT
  mr.traversal_path AS traversal_path,
  mr.id AS mr_id, mr.iid AS mr_iid, mr.title AS mr_title, mr.state AS mr_state,
  mr.source_branch AS mr_source_branch, mr.target_branch AS mr_target_branch,
  mr.draft AS mr_draft, mr.project_id AS mr_project_id,
  mr.created_at AS mr_created_at, mr.merged_at AS mr_merged_at,
  u.id AS u_id, u.username AS u_username, u.name AS u_name, u.state AS u_state,
  greatest(mr._version, u._version) AS _version,
  (mr._deleted OR u._deleted) AS _deleted
FROM v93_gl_merge_request AS mr FINAL
INNER JOIN v93_gl_user AS u FINAL ON mr.author_id = u.id
WHERE NOT mr._deleted;
