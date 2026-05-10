-- Pattern queries for security audit rolling window analysis.
-- Each query measures the surface area of a vulnerability class
-- in the codebase at a given point in time.
-- Run against ~/.orbit/graph.duckdb after `orbit index`.

-- ═══════════════════════════════════════════════════════════
-- 1. AUTHZ: Controller actions without authorization calls
--    Measures: methods in controllers that call service-layer
--    methods but don't call Ability.allowed?/can?/authorize!
-- ═══════════════════════════════════════════════════════════
-- authz_surface: controller methods that call services but lack auth checks
SELECT 'authz_unguarded_controllers' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_definition s
JOIN gl_edge e ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE s.file_path LIKE 'app/controllers/%'
  AND s.definition_type = 'Method'
  AND t.file_path LIKE 'app/services/%'
  AND e.relationship_kind = 'CALLS'
  AND s.id NOT IN (
    SELECT e2.source_id FROM gl_edge e2
    JOIN gl_definition auth ON e2.target_id = auth.id AND e2.target_kind = 'Definition'
    WHERE e2.relationship_kind = 'CALLS'
      AND (auth.fqn LIKE '%allowed?%' OR auth.fqn LIKE '%can?%' OR auth.fqn LIKE '%authorize%')
  );

-- authz_total: total controller methods that call services (denominator)
SELECT 'authz_total_controller_service_calls' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_definition s
JOIN gl_edge e ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE s.file_path LIKE 'app/controllers/%'
  AND s.definition_type = 'Method'
  AND t.file_path LIKE 'app/services/%'
  AND e.relationship_kind = 'CALLS';

-- ═══════════════════════════════════════════════════════════
-- 2. XSS: Output methods that don't go through sanitization
--    Measures: render/html_safe usage without sanitize calls
-- ═══════════════════════════════════════════════════════════
-- xss_html_safe_usage: callers of html_safe (potential XSS source)
SELECT 'xss_html_safe_callers' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_edge e
JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE e.relationship_kind = 'CALLS'
  AND t.name = 'html_safe'
  AND s.file_path NOT LIKE '%spec%'
  AND s.file_path NOT LIKE '%test%';

-- ═══════════════════════════════════════════════════════════
-- 3. DOS: Unbounded input parsing (JSON.parse without safe_parse)
--    Measures: callers of unsafe parse vs safe_parse
-- ═══════════════════════════════════════════════════════════
-- dos_unsafe_json_parse: callers of Gitlab::Json::parse (unsafe)
SELECT 'dos_unsafe_json_parse' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_edge e
JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE e.relationship_kind = 'CALLS'
  AND t.fqn = 'Gitlab::Json::parse'
  AND s.file_path NOT LIKE '%spec%';

-- dos_safe_json_parse: callers of Gitlab::Json::safe_parse (safe)
SELECT 'dos_safe_json_parse' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_edge e
JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE e.relationship_kind = 'CALLS'
  AND t.fqn = 'Gitlab::Json::safe_parse'
  AND s.file_path NOT LIKE '%spec%';

-- ═══════════════════════════════════════════════════════════
-- 4. INFO_DISCLOSURE: API endpoints exposing data without auth
--    Measures: API helper methods complexity (callers of present/expose)
-- ═══════════════════════════════════════════════════════════
-- info_api_surface: unique API entity files
SELECT 'info_api_entity_files' as metric, COUNT(DISTINCT file_path) as value
FROM gl_definition
WHERE file_path LIKE 'lib/api/entities/%'
  AND definition_type IN ('Class', 'Module');

-- ═══════════════════════════════════════════════════════════
-- 5. INJECTION: Popen/system call surface
--    Measures: callers of shell execution methods
-- ═══════════════════════════════════════════════════════════
-- injection_popen_callers: code calling Gitlab::Popen
SELECT 'injection_popen_callers' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_edge e
JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE e.relationship_kind = 'CALLS'
  AND t.fqn LIKE 'Gitlab::Popen%'
  AND s.file_path NOT LIKE '%spec%';

-- ═══════════════════════════════════════════════════════════
-- 6. AUTH_BYPASS: Complexity of authentication concerns
--    Measures: method count in auth-related concerns (more methods = more bypass surface)
-- ═══════════════════════════════════════════════════════════
-- auth_concern_methods: methods in authentication concerns
SELECT 'auth_concern_methods' as metric, COUNT(*) as value
FROM gl_definition
WHERE file_path LIKE 'app/controllers/concerns/authenticates%'
  AND definition_type = 'Method';

-- auth_concern_callers: external callers of auth concerns
SELECT 'auth_concern_external_callers' as metric, COUNT(DISTINCT s.fqn) as value
FROM gl_edge e
JOIN gl_definition s ON e.source_id = s.id AND e.source_kind = 'Definition'
JOIN gl_definition t ON e.target_id = t.id AND e.target_kind = 'Definition'
WHERE e.relationship_kind = 'CALLS'
  AND t.file_path LIKE 'app/controllers/concerns/authenticates%'
  AND s.file_path NOT LIKE 'app/controllers/concerns/authenticates%'
  AND s.file_path NOT LIKE '%spec%';

-- ═══════════════════════════════════════════════════════════
-- 7. GENERAL: Codebase complexity metrics (context)
-- ═══════════════════════════════════════════════════════════
-- total Ruby definitions
SELECT 'total_ruby_definitions' as metric, COUNT(*) as value
FROM gl_definition WHERE file_path LIKE '%.rb';

-- total CALLS edges
SELECT 'total_calls_edges' as metric, COUNT(*) as value
FROM gl_edge WHERE relationship_kind = 'CALLS';

-- total EXTENDS edges (mixin/inheritance complexity)
SELECT 'total_extends_edges' as metric, COUNT(*) as value
FROM gl_edge WHERE relationship_kind = 'EXTENDS';

-- controller method count
SELECT 'total_controller_methods' as metric, COUNT(*) as value
FROM gl_definition
WHERE file_path LIKE 'app/controllers/%'
  AND definition_type = 'Method';

-- service class count
SELECT 'total_service_classes' as metric, COUNT(*) as value
FROM gl_definition
WHERE file_path LIKE 'app/services/%'
  AND definition_type IN ('Class', 'Module');
