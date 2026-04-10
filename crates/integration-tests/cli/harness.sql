-- CLI test harness for DuckDB.
-- Source with: duckdb <db> ".read harness.sql"
--
-- Table macros for reading orbit JSON output, a results accumulator,
-- and assertion macros that INSERT pass/fail into results.
--
-- Test scripts SET VARIABLE for file paths, then call assert_* macros.

-- ── Results ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS results (
    name   TEXT NOT NULL,
    ok     BOOLEAN NOT NULL,
    detail TEXT DEFAULT ''
);

-- ── Orbit JSON access ───────────────────────────────────────────

CREATE OR REPLACE MACRO orbit_nodes(f) AS TABLE
    SELECT unnest(nodes) AS n FROM read_json(f);

CREATE OR REPLACE MACRO orbit_edges(f) AS TABLE
    SELECT unnest(edges) AS e FROM read_json(f);

-- Compare node IDs across files matching a glob (order-independent).
CREATE OR REPLACE MACRO files_same(pattern) AS TABLE
    WITH per_file AS (
        SELECT filename, list_sort(list(n.id)) AS ids
        FROM read_json(pattern, filename=true), unnest(nodes) AS t(n)
        GROUP BY filename
    )
    SELECT (count(DISTINCT ids) = 1) AS ok FROM per_file;

-- ── Assertion macros ────────────────────────────────────────────
-- Each inserts one row into results. Uses getvariable() so test
-- scripts can SET VARIABLE for file paths before calling.

-- Assert nodes matching a filter exist.
-- Usage: SET VARIABLE f = '/path.json';
--        SELECT assert_has('test_name', getvariable('f'), <count>);
-- where <count> is from a subquery the caller provides.

-- Since DuckDB scalar macros can't contain INSERT or WHERE with
-- dynamic SQL, assertions are done via INSERT..SELECT in lib.sh
-- using the orbit_nodes/orbit_edges table macros above.

-- ── Output ──────────────────────────────────────────────────────

CREATE OR REPLACE VIEW test_output AS
    SELECT json_object(
        'pass', (SELECT count(*)::INT FROM results WHERE ok),
        'fail', (SELECT count(*)::INT FROM results WHERE NOT ok),
        'tests', (SELECT json_group_array(
            json_object('name', name, 'ok', ok, 'detail', coalesce(detail, ''))
        ) FROM results)
    ) AS json;
