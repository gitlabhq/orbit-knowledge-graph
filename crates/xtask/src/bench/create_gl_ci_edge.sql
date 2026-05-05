CREATE TABLE IF NOT EXISTS gl_ci_edge (
    traversal_path String DEFAULT '0/' CODEC(ZSTD(1)),
    source_id Int64 CODEC(Delta(8), LZ4),
    source_kind LowCardinality(String) CODEC(LZ4),
    relationship_kind LowCardinality(String) CODEC(LZ4),
    target_id Int64 CODEC(Delta(8), LZ4),
    target_kind LowCardinality(String) CODEC(LZ4),
    source_tags Array(LowCardinality(String)) CODEC(LZ4),
    target_tags Array(LowCardinality(String)) CODEC(LZ4),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, source_id, relationship_kind, target_id, source_kind, target_kind)
PRIMARY KEY (traversal_path, source_id, relationship_kind)
SETTINGS index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;
