-- CLI test harness for DuckDB.
-- Source with: duckdb <db> ".read harness.sql"
--
-- Test scripts compute counts with FILTER, pass them to check_*
-- macros inside an unnest([...]) array, then INSERT into results.
--
-- Pattern:
--   WITH nodes AS (SELECT unnest(nodes) AS n FROM read_json('<file>')),
--   c AS (SELECT count(*) FILTER (WHERE ...) AS x, ... FROM nodes),
--   checks AS (SELECT unnest([check_has('name', x), ...]) AS r FROM c)
--   INSERT INTO results SELECT r.name, r.ok, r.detail FROM checks;

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

CREATE OR REPLACE MACRO files_same(pattern) AS TABLE
    WITH per_file AS (
        SELECT filename, list_sort(list(n.id)) AS ids
        FROM read_json(pattern, filename=true), unnest(nodes) AS t(n)
        GROUP BY filename
    )
    SELECT (count(DISTINCT ids) = 1) AS ok FROM per_file;

-- ── Check macros (return named structs) ─────────────────────────

CREATE OR REPLACE MACRO check_has(test_name, cnt) AS {
    'name': test_name,
    'ok': cnt > 0,
    'detail': CASE WHEN cnt > 0 THEN cnt::TEXT || ' matches' ELSE 'not found' END
};

CREATE OR REPLACE MACRO check_count(test_name, cnt, expected, msg) AS {
    'name': test_name,
    'ok': cnt = expected,
    'detail': CASE WHEN cnt = expected THEN msg ELSE 'expected ' || expected || ', got ' || cnt END
};

CREATE OR REPLACE MACRO check_edges(test_name, cnt) AS {
    'name': test_name,
    'ok': cnt > 0,
    'detail': CASE WHEN cnt > 0 THEN cnt::TEXT || ' edges' ELSE 'no edges' END
};

CREATE OR REPLACE MACRO check_ok(test_name, is_ok, msg) AS {
    'name': test_name,
    'ok': is_ok,
    'detail': msg
};

-- ── Record checks ───────────────────────────────────────────────
-- Unnest an array of check structs into result rows.
-- Usage: INSERT INTO results SELECT * FROM record([check_has(...), ...]);
CREATE OR REPLACE MACRO record(checks) AS TABLE
    SELECT r.name, r.ok, r.detail FROM unnest(checks) AS t(r);

-- ── Output ──────────────────────────────────────────────────────

CREATE OR REPLACE VIEW test_output AS
    SELECT json_object(
        'pass', (SELECT count(*)::INT FROM results WHERE ok),
        'fail', (SELECT count(*)::INT FROM results WHERE NOT ok),
        'tests', (SELECT json_group_array(
            json_object('name', name, 'ok', ok, 'detail', coalesce(detail, ''))
        ) FROM results)
    ) AS json;
