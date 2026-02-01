//! Integration tests for security entity processing in the namespace handler.

use arrow::array::Array;
use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edge_count, create_namespace_payload, default_test_watermark,
    get_boolean_column, get_namespace_handler, get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerabilities() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerabilities
                (id, title, description, project_id, author_id, state, severity, report_type,
                 resolved_on_default_branch, present_on_default_branch, uuid,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'SQL Injection in login', 'Critical SQL injection vulnerability', 1000, 1, 4, 7, 0,
                 false, true, 'uuid-001',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'XSS in comments', 'Cross-site scripting vulnerability', 1000, 2, 1, 4, 3,
                 false, true, 'uuid-002',
                 '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, title, state, severity, report_type FROM gl_vulnerability ORDER BY id")
        .await;
    assert!(!result.is_empty(), "vulnerabilities should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "SQL Injection in login");
    assert_eq!(titles.value(1), "XSS in comments");

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "confirmed");
    assert_eq!(states.value(1), "detected");

    let severities = get_string_column(batch, "severity");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    assert_edge_count(&context, "IN_PROJECT", "Vulnerability", "Project", 2).await;
    assert_edge_count(&context, "AUTHORED", "User", "Vulnerability", 2).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_scanners() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_scanners
                (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'bandit', 'Bandit', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, external_id, name, vendor FROM gl_vulnerability_scanner ORDER BY id")
        .await;
    assert!(!result.is_empty(), "scanners should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let names = get_string_column(batch, "name");
    assert_eq!(names.value(0), "Gemnasium");
    assert_eq!(names.value(1), "Bandit");

    let external_ids = get_string_column(batch, "external_id");
    assert_eq!(external_ids.value(0), "gemnasium");
    assert_eq!(external_ids.value(1), "bandit");

    assert_edge_count(&context, "SCANS", "VulnerabilityScanner", "Project", 2).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_identifiers() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_identifiers
                (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'cwe', 'CWE-89', 'SQL Injection', 'https://cwe.mitre.org/data/definitions/89.html', 'fp2', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, external_type, external_id, name, url FROM gl_vulnerability_identifier ORDER BY id")
        .await;
    assert!(!result.is_empty(), "vulnerability identifiers should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let names = get_string_column(batch, "name");
    assert_eq!(names.value(0), "Log4Shell");
    assert_eq!(names.value(1), "SQL Injection");

    let external_types = get_string_column(batch, "external_type");
    assert_eq!(external_types.value(0), "cve");
    assert_eq!(external_types.value(1), "cwe");

    let external_ids = get_string_column(batch, "external_id");
    assert_eq!(external_ids.value(0), "CVE-2021-44228");
    assert_eq!(external_ids.value(1), "CWE-89");

    assert_edge_count(
        &context,
        "IN_PROJECT",
        "VulnerabilityIdentifier",
        "Project",
        2,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_findings() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_scanners
                (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_security_findings
                (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
            VALUES
                (1, 'finding-uuid-001', 100, 1, 5, true, '{\"name\": \"SQL Injection\", \"description\": \"A SQL injection vulnerability\", \"solution\": \"Use parameterized queries\"}', 1000, '1/100/', '2024-01-20 12:00:00'),
                (2, 'finding-uuid-002', 100, 1, 3, false, '{\"name\": \"XSS\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, uuid, name, description, solution, severity, deduplicated FROM gl_finding ORDER BY id")
        .await;
    assert!(!result.is_empty(), "findings should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let uuids = get_string_column(batch, "uuid");
    assert_eq!(uuids.value(0), "finding-uuid-001");
    assert_eq!(uuids.value(1), "finding-uuid-002");

    let names = get_string_column(batch, "name");
    assert_eq!(names.value(0), "SQL Injection");
    assert_eq!(names.value(1), "XSS");

    let descriptions = get_string_column(batch, "description");
    assert_eq!(descriptions.value(0), "A SQL injection vulnerability");
    assert!(descriptions.is_null(1));

    let solutions = get_string_column(batch, "solution");
    assert_eq!(solutions.value(0), "Use parameterized queries");
    assert!(solutions.is_null(1));

    let severities = get_string_column(batch, "severity");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    let deduplicated = get_boolean_column(batch, "deduplicated");
    assert!(deduplicated.value(0));
    assert!(!deduplicated.value(1));

    assert_edge_count(&context, "IN_PROJECT", "Finding", "Project", 2).await;
    assert_edge_count(
        &context,
        "DETECTED_BY",
        "Finding",
        "VulnerabilityScanner",
        2,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_with_user_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerabilities
                (id, title, project_id, author_id, state, severity, report_type,
                 confirmed_by_id, resolved_by_id, dismissed_by_id, uuid,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'Confirmed vulnerability', 1000, 1, 4, 6, 0,
                 2, NULL, NULL, 'uuid-003',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'Resolved vulnerability', 1000, 1, 3, 4, 1,
                 NULL, 3, NULL, 'uuid-004',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (3, 'Dismissed vulnerability', 1000, 1, 2, 3, 2,
                 NULL, NULL, 4, 'uuid-005',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, title, state FROM gl_vulnerability ORDER BY id")
        .await;
    assert!(!result.is_empty(), "vulnerabilities should exist");
    assert_eq!(result[0].num_rows(), 3);

    assert_edge_count(&context, "AUTHORED", "User", "Vulnerability", 3).await;
    assert_edge_count(&context, "CONFIRMED_BY", "User", "Vulnerability", 1).await;
    assert_edge_count(&context, "RESOLVED_BY", "User", "Vulnerability", 1).await;
    assert_edge_count(&context, "DISMISSED_BY", "User", "Vulnerability", 1).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_finding_edge() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_security_findings
                (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
            VALUES (1, 'finding-uuid-001', 100, 1, 5, true, '{\"name\": \"Test Finding\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerabilities
                (id, title, project_id, author_id, state, severity, report_type, finding_id, uuid,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'Vulnerability with finding', 1000, 1, 1, 7, 0, 1, 'uuid-006',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    assert_edge_count(&context, "HAS_FINDING", "Vulnerability", "Finding", 1).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_occurrences() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_scanners
                (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_identifiers
                (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerabilities
                (id, title, project_id, author_id, state, severity, report_type, uuid,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'Log4Shell Vulnerability', 1000, 1, 1, 7, 0, 'vuln-uuid-001',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_occurrences
                (id, uuid, name, description, solution, cve, location, location_fingerprint,
                 severity, report_type, detection_method, project_id, scanner_id,
                 primary_identifier_id, vulnerability_id, metadata_version,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'occurrence-uuid-001', 'SQL Injection', 'A SQL injection vulnerability', 'Use parameterized queries',
                 'CVE-2021-44228', 'src/main.rs:42', 'fp-location-1',
                 7, 0, 0, 1000, 1, 1, 1, '1.0',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'occurrence-uuid-002', 'XSS Vulnerability', NULL, NULL,
                 NULL, 'src/web.rs:100', 'fp-location-2',
                 5, 3, 1, 1000, 1, 1, NULL, '1.0',
                 '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, uuid, name, description, solution, cve, severity, report_type, detection_method FROM gl_vulnerability_occurrence ORDER BY id")
        .await;
    assert!(!result.is_empty(), "vulnerability occurrences should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let uuids = get_string_column(batch, "uuid");
    assert_eq!(uuids.value(0), "occurrence-uuid-001");
    assert_eq!(uuids.value(1), "occurrence-uuid-002");

    let names = get_string_column(batch, "name");
    assert_eq!(names.value(0), "SQL Injection");
    assert_eq!(names.value(1), "XSS Vulnerability");

    let descriptions = get_string_column(batch, "description");
    assert_eq!(descriptions.value(0), "A SQL injection vulnerability");
    assert!(descriptions.is_null(1));

    let severities = get_string_column(batch, "severity");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    let report_types = get_string_column(batch, "report_type");
    assert_eq!(report_types.value(0), "sast");
    assert_eq!(report_types.value(1), "dast");

    let detection_methods = get_string_column(batch, "detection_method");
    assert_eq!(detection_methods.value(0), "gitlab_security_report");
    assert_eq!(detection_methods.value(1), "external_security_report");

    assert_edge_count(
        &context,
        "IN_PROJECT",
        "VulnerabilityOccurrence",
        "Project",
        2,
    )
    .await;
    assert_edge_count(
        &context,
        "DETECTED_BY",
        "VulnerabilityOccurrence",
        "VulnerabilityScanner",
        2,
    )
    .await;
    assert_edge_count(
        &context,
        "HAS_IDENTIFIER",
        "VulnerabilityOccurrence",
        "VulnerabilityIdentifier",
        2,
    )
    .await;
    assert_edge_count(
        &context,
        "OCCURRENCE_OF",
        "VulnerabilityOccurrence",
        "Vulnerability",
        1,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_merge_request_links() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerabilities
                (id, title, project_id, author_id, state, severity, report_type, uuid,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'SQL Injection', 1000, 1, 1, 7, 0, 'vuln-uuid-001',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'XSS Vulnerability', 1000, 1, 1, 6, 0, 'vuln-uuid-002',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, author_id, traversal_path, version)
            VALUES
                (10, 101, 'Fix SQL injection', 'Fixes the vulnerability', 'fix-sql', 'main', 3, 'merged',
                 false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
                (20, 102, 'Fix XSS', 'Fixes XSS issue', 'fix-xss', 'main', 3, 'merged',
                 false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_merge_request_links
                (id, vulnerability_id, merge_request_id, project_id, traversal_path,
                 created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 1, 10, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 2, 20, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    assert_edge_count(&context, "FIXES", "MergeRequest", "Vulnerability", 2).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_vulnerability_occurrence_identifiers() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_scanners
                (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_identifiers
                (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'cwe', 'CWE-89', 'SQL Injection', 'https://cwe.mitre.org/data/definitions/89.html', 'fp2', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (3, 'cve', 'CVE-2022-12345', 'Another CVE', 'https://nvd.nist.gov/vuln/detail/CVE-2022-12345', 'fp3', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_occurrences
                (id, uuid, name, severity, report_type, detection_method, project_id, scanner_id,
                 primary_identifier_id, metadata_version, location, location_fingerprint,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 'occurrence-uuid-001', 'SQL Injection', 7, 0, 0, 1000, 1, 1, '1.0', 'src/main.rs:42', 'fp-loc-1',
                 '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 'occurrence-uuid-002', 'XSS Vulnerability', 5, 0, 0, 1000, 1, 2, '1.0', 'src/web.rs:100', 'fp-loc-2',
                 '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_occurrence_identifiers
                (id, occurrence_id, identifier_id, project_id, traversal_path,
                 created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 1, 1, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 1, 2, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (3, 2, 3, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    assert_edge_count(
        &context,
        "HAS_IDENTIFIER",
        "VulnerabilityOccurrence",
        "VulnerabilityIdentifier",
        3,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_security_scans() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_pipelines
                (id, project_id, status, source, tag, traversal_path, _siphon_replicated_at)
            VALUES (500, 1000, 'success', 1, false, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_p_ci_builds
                (id, name, status, project_id, stage_id, allow_failure, traversal_path, _siphon_replicated_at)
            VALUES (600, 'sast-job', 'success', 1000, 1, false, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_security_scans
                (id, build_id, scan_type, status, latest, project_id, pipeline_id,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 600, 1, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 600, 2, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query("SELECT id, scan_type, status, latest FROM gl_security_scan ORDER BY id")
        .await;
    assert!(!result.is_empty(), "security scans should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let scan_types = get_string_column(batch, "scan_type");
    assert_eq!(scan_types.value(0), "sast");
    assert_eq!(scan_types.value(1), "dependency_scanning");

    let statuses = get_string_column(batch, "status");
    assert_eq!(statuses.value(0), "succeeded");
    assert_eq!(statuses.value(1), "succeeded");

    let latest_values = get_boolean_column(batch, "latest");
    assert!(latest_values.value(0));
    assert!(latest_values.value(1));

    assert_edge_count(&context, "IN_PROJECT", "SecurityScan", "Project", 2).await;
    assert_edge_count(&context, "IN_PIPELINE", "SecurityScan", "Pipeline", 2).await;
    assert_edge_count(&context, "RAN_BY", "SecurityScan", "Job", 2).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_security_scan_finding_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_vulnerability_scanners
                (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_security_scans
                (id, build_id, scan_type, status, latest, project_id, pipeline_id,
                 traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 600, 1, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_security_findings
                (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
            VALUES
                (1, 'finding-uuid-001', 100, 1, 5, true, '{\"name\": \"SQL Injection\"}', 1000, '1/100/', '2024-01-20 12:00:00'),
                (2, 'finding-uuid-002', 100, 1, 3, false, '{\"name\": \"XSS\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    assert_edge_count(&context, "HAS_FINDING", "SecurityScan", "Finding", 2).await;
}
