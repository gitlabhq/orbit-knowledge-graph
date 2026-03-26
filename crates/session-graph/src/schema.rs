pub const SCHEMA_DDL: &str = "
CREATE TABLE IF NOT EXISTS gl_node (
    id              VARCHAR NOT NULL PRIMARY KEY,
    kind            VARCHAR NOT NULL,
    properties      VARCHAR NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_node_kind ON gl_node(kind);

CREATE TABLE IF NOT EXISTS gl_edge (
    source_id           VARCHAR NOT NULL,
    source_kind         VARCHAR NOT NULL,
    relationship_kind   VARCHAR NOT NULL,
    target_id           VARCHAR NOT NULL,
    target_kind         VARCHAR NOT NULL,
    properties          VARCHAR NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, relationship_kind, target_id)
);

CREATE INDEX IF NOT EXISTS idx_edge_source ON gl_edge(source_id);
CREATE INDEX IF NOT EXISTS idx_edge_target ON gl_edge(target_id);
CREATE INDEX IF NOT EXISTS idx_edge_rel ON gl_edge(relationship_kind);

CREATE TABLE IF NOT EXISTS gl_schema_registry (
    kind            VARCHAR NOT NULL PRIMARY KEY,
    description     VARCHAR NOT NULL DEFAULT '',
    property_keys   VARCHAR NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
";

pub const SESSION_VIEW_DDL: &str = "
CREATE OR REPLACE VIEW gl_session AS
SELECT
    id,
    properties->>'$.tool' AS tool,
    properties->>'$.project' AS project,
    properties->>'$.title' AS title,
    properties->>'$.summary' AS summary,
    properties->>'$.model' AS model,
    properties->>'$.git_branch' AS git_branch,
    CAST(COALESCE(NULLIF(properties->>'$.message_count', ''), '0') AS INTEGER) AS message_count,
    COALESCE(properties->>'$.filepath', '') AS filepath,
    created_at,
    updated_at
FROM gl_node
WHERE kind = 'Session';

CREATE OR REPLACE VIEW gl_topic AS
SELECT
    id,
    properties->>'$.name' AS name
FROM gl_node
WHERE kind = 'Topic';
";
