-- CLI test harness for DuckDB.
-- Source with: duckdb <db> ".read harness.sql"

-- ── Results accumulator ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS results (
    name   TEXT NOT NULL,
    ok     BOOLEAN NOT NULL,
    detail TEXT DEFAULT ''
);

-- ── Orbit JSON access macros ────────────────────────────────────

-- Unnest nodes from an orbit query JSON file.
-- Usage: SELECT n.* FROM orbit_nodes('/path/to/file.json') WHERE n.name = 'foo'
CREATE OR REPLACE MACRO orbit_nodes(f) AS TABLE
    SELECT unnest(nodes) AS n FROM read_json(f);

-- Unnest edges from an orbit traversal JSON file.
-- Usage: SELECT count(*) FROM orbit_edges('/path/to/file.json')
CREATE OR REPLACE MACRO orbit_edges(f) AS TABLE
    SELECT unnest(edges) AS e FROM read_json(f);

-- Compare node IDs across multiple JSON files (order-independent).
-- Usage: SELECT ok FROM files_same('/tmp/prefix*.json')
CREATE OR REPLACE MACRO files_same(pattern) AS TABLE
    WITH per_file AS (
        SELECT filename, list_sort(list(n.id)) AS ids
        FROM read_json(pattern, filename=true), unnest(nodes) AS t(n)
        GROUP BY filename
    )
    SELECT (count(DISTINCT ids) = 1) AS ok FROM per_file;

-- ── Results output ──────────────────────────────────────────────

-- Final JSON result. Usage: SELECT json FROM test_output;
CREATE OR REPLACE VIEW test_output AS
    SELECT json_object(
        'pass', (SELECT count(*)::INT FROM results WHERE ok),
        'fail', (SELECT count(*)::INT FROM results WHERE NOT ok),
        'tests', (SELECT json_group_array(
            json_object('name', name, 'ok', ok, 'detail', coalesce(detail, ''))
        ) FROM results)
    ) AS json;
