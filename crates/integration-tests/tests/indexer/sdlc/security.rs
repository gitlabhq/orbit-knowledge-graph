use arrow::array::{Array, BooleanArray, StringArray};

use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    create_project, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_vulnerabilities(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerabilities
            (id, title, description, project_id, author_id, state, severity, report_type,
             resolved_on_default_branch, present_on_default_branch, uuid,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'SQL Injection in login', 'Critical SQL injection vulnerability', 1000, 1, 4, 7, 0,
             false, true, 'uuid-001',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 'XSS in comments', 'Cross-site scripting vulnerability', 1000, 2, 1, 5, 9,
             false, true, 'uuid-002',
             '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_vulnerability", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT title, state, severity, report_type FROM {} FINAL ORDER BY id",
            t("gl_vulnerability")
        ))
        .await;
    let batch = &result[0];

    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "SQL Injection in login");
    assert_eq!(titles.value(1), "XSS in comments");

    let states =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "state").expect("state column");
    assert_eq!(states.value(0), "confirmed");
    assert_eq!(states.value(1), "detected");

    let severities =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "severity").expect("severity column");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    let report_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "report_type")
        .expect("report_type column");
    assert_eq!(report_types.value(0), "sast");
    assert_eq!(report_types.value(1), "sarif");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Vulnerability", "Project", "1/100/", 2)
        .await;
    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "Vulnerability", "1/100/", 2).await;
}

pub async fn processes_scanners(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_scanners
            (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 'bandit', 'Bandit', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_vulnerability_scanner", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT name, external_id FROM {} FINAL ORDER BY id",
            t("gl_vulnerability_scanner")
        ))
        .await;
    let batch = &result[0];
    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), "Gemnasium");
    assert_eq!(names.value(1), "Bandit");

    let external_ids = ArrowUtils::get_column_by_name::<StringArray>(batch, "external_id")
        .expect("external_id column");
    assert_eq!(external_ids.value(0), "gemnasium");
    assert_eq!(external_ids.value(1), "bandit");

    assert_edges_have_traversal_path(ctx, "SCANS", "VulnerabilityScanner", "Project", "1/100/", 2)
        .await;
}

pub async fn processes_vulnerability_identifiers(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_identifiers
            (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 'cwe', 'CWE-89', 'SQL Injection', 'https://cwe.mitre.org/data/definitions/89.html', 'fp2', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_vulnerability_identifier", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT name, external_type, external_id FROM {} FINAL ORDER BY id",
            t("gl_vulnerability_identifier")
        ))
        .await;
    let batch = &result[0];

    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), "Log4Shell");
    assert_eq!(names.value(1), "SQL Injection");

    let external_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "external_type")
        .expect("external_type column");
    assert_eq!(external_types.value(0), "cve");
    assert_eq!(external_types.value(1), "cwe");

    let external_ids = ArrowUtils::get_column_by_name::<StringArray>(batch, "external_id")
        .expect("external_id column");
    assert_eq!(external_ids.value(0), "CVE-2021-44228");
    assert_eq!(external_ids.value(1), "CWE-89");

    assert_edges_have_traversal_path(
        ctx,
        "IN_PROJECT",
        "VulnerabilityIdentifier",
        "Project",
        "1/100/",
        2,
    )
    .await;
}

pub async fn processes_findings(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_scanners
            (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_security_findings
            (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, '00000000-0000-0000-0000-000000000f01', 100, 1, 7, true, '{\"name\": \"SQL Injection\", \"description\": \"A SQL injection vulnerability\", \"solution\": \"Use parameterized queries\"}', 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, '00000000-0000-0000-0000-000000000f02', 100, 1, 5, false, '{\"name\": \"XSS\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_finding", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT uuid, name, description, solution, severity, deduplicated FROM {} FINAL ORDER BY id",
            t("gl_finding")
        ))
        .await;
    let batch = &result[0];

    let uuids = ArrowUtils::get_column_by_name::<StringArray>(batch, "uuid").expect("uuid column");
    assert_eq!(uuids.value(0), "00000000-0000-0000-0000-000000000f01");
    assert_eq!(uuids.value(1), "00000000-0000-0000-0000-000000000f02");

    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), "SQL Injection");
    assert_eq!(names.value(1), "XSS");

    let descriptions = ArrowUtils::get_column_by_name::<StringArray>(batch, "description")
        .expect("description column");
    assert_eq!(descriptions.value(0), "A SQL injection vulnerability");
    assert!(descriptions.is_null(1));

    let solutions =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "solution").expect("solution column");
    assert_eq!(solutions.value(0), "Use parameterized queries");
    assert!(solutions.is_null(1));

    let severities =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "severity").expect("severity column");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    let deduplicated = ArrowUtils::get_column_by_name::<BooleanArray>(batch, "deduplicated")
        .expect("deduplicated column");
    assert!(deduplicated.value(0));
    assert!(!deduplicated.value(1));

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "Finding", "Project", "1/100/", 2).await;
    assert_edges_have_traversal_path(
        ctx,
        "DETECTED_BY",
        "Finding",
        "VulnerabilityScanner",
        "1/100/",
        2,
    )
    .await;
}

pub async fn processes_vulnerability_with_user_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
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

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_vulnerability", 3).await;

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "Vulnerability", "1/100/", 3).await;
    assert_edges_have_traversal_path(ctx, "CONFIRMED_BY", "User", "Vulnerability", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "RESOLVED_BY", "User", "Vulnerability", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "DISMISSED_BY", "User", "Vulnerability", "1/100/", 1)
        .await;
}

pub async fn processes_vulnerability_finding_edge(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_security_findings
            (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
        VALUES (1, '00000000-0000-0000-0000-000000000f01', 100, 1, 5, true, '{\"name\": \"Test Finding\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerabilities
            (id, title, project_id, author_id, state, severity, report_type, finding_id, uuid,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'Vulnerability with finding', 1000, 1, 1, 7, 0, 1, 'uuid-006',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "HAS_FINDING", "Vulnerability", "Finding", "1/100/", 1)
        .await;
}

pub async fn processes_vulnerability_occurrences(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_scanners
            (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_identifiers
            (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerabilities
            (id, title, project_id, author_id, state, severity, report_type, uuid,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'Log4Shell Vulnerability', 1000, 1, 1, 7, 0, '00000000-0000-0000-0000-000000000b01',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_occurrences
            (id, uuid, name, description, solution, cve, location, location_fingerprint,
             severity, report_type, detection_method, project_id, scanner_id,
             primary_identifier_id, vulnerability_id, metadata_version,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, '00000000-0000-0000-0000-0000000000a1', 'SQL Injection', 'A SQL injection vulnerability', 'Use parameterized queries',
             'CVE-2021-44228', 'src/main.rs:42', 'fp-location-1',
             7, 0, 0, 1000, 1, 1, 1, '1.0',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, '00000000-0000-0000-0000-0000000000a2', 'XSS Vulnerability', NULL, NULL,
             NULL, 'src/web.rs:100', 'fp-location-2',
             5, 3, 1, 1000, 1, 1, NULL, '1.0',
             '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_vulnerability_occurrence", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT uuid, name, description, severity, report_type, detection_method FROM {} FINAL ORDER BY id",
            t("gl_vulnerability_occurrence")
        ))
        .await;
    let batch = &result[0];

    let uuids = ArrowUtils::get_column_by_name::<StringArray>(batch, "uuid").expect("uuid column");
    assert_eq!(uuids.value(0), "00000000-0000-0000-0000-0000000000a1");
    assert_eq!(uuids.value(1), "00000000-0000-0000-0000-0000000000a2");

    let names = ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
    assert_eq!(names.value(0), "SQL Injection");
    assert_eq!(names.value(1), "XSS Vulnerability");

    let descriptions = ArrowUtils::get_column_by_name::<StringArray>(batch, "description")
        .expect("description column");
    assert_eq!(descriptions.value(0), "A SQL injection vulnerability");
    assert_eq!(descriptions.value(1), "");

    let severities =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "severity").expect("severity column");
    assert_eq!(severities.value(0), "critical");
    assert_eq!(severities.value(1), "medium");

    let report_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "report_type")
        .expect("report_type column");
    assert_eq!(report_types.value(0), "sast");
    assert_eq!(report_types.value(1), "dast");

    let detection_methods =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "detection_method")
            .expect("detection_method column");
    assert_eq!(detection_methods.value(0), "gitlab_security_report");
    assert_eq!(detection_methods.value(1), "external_security_report");

    assert_edges_have_traversal_path(
        ctx,
        "IN_PROJECT",
        "VulnerabilityOccurrence",
        "Project",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(
        ctx,
        "DETECTED_BY",
        "VulnerabilityOccurrence",
        "VulnerabilityScanner",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(
        ctx,
        "HAS_IDENTIFIER",
        "VulnerabilityOccurrence",
        "VulnerabilityIdentifier",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(
        ctx,
        "OCCURRENCE_OF",
        "VulnerabilityOccurrence",
        "Vulnerability",
        "1/100/",
        1,
    )
    .await;
}

pub async fn processes_vulnerability_merge_request_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerabilities
            (id, title, project_id, author_id, state, severity, report_type, uuid,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'SQL Injection', 1000, 1, 1, 7, 0, '00000000-0000-0000-0000-000000000b01',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 'XSS Vulnerability', 1000, 1, 1, 6, 0, '00000000-0000-0000-0000-000000000b02',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Fix SQL injection', 'Fixes the vulnerability', 'fix-sql', 'main', 3, 'merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
            (20, 102, 'Fix XSS', 'Fixes XSS issue', 'fix-xss', 'main', 3, 'merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_merge_request_links
            (id, vulnerability_id, merge_request_id, project_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 1, 10, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 2, 20, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "FIXES", "MergeRequest", "Vulnerability", "1/100/", 2)
        .await;
}

pub async fn processes_vulnerability_occurrence_identifiers(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_scanners
            (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_identifiers
            (id, external_type, external_id, name, url, fingerprint, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 'cve', 'CVE-2021-44228', 'Log4Shell', 'https://nvd.nist.gov/vuln/detail/CVE-2021-44228', 'fp1', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 'cwe', 'CWE-89', 'SQL Injection', 'https://cwe.mitre.org/data/definitions/89.html', 'fp2', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (3, 'cve', 'CVE-2022-12345', 'Another CVE', 'https://nvd.nist.gov/vuln/detail/CVE-2022-12345', 'fp3', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_occurrences
            (id, uuid, name, severity, report_type, detection_method, project_id, scanner_id,
             primary_identifier_id, metadata_version, location, location_fingerprint,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, '00000000-0000-0000-0000-0000000000a1', 'SQL Injection', 7, 0, 0, 1000, 1, 1, '1.0', 'src/main.rs:42', 'fp-loc-1',
             '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, '00000000-0000-0000-0000-0000000000a2', 'XSS Vulnerability', 5, 0, 0, 1000, 1, 3, '1.0', 'src/web.rs:100', 'fp-loc-2',
             '1/100/', '2024-01-16 10:00:00', '2024-01-16 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_occurrence_identifiers
            (id, occurrence_id, identifier_id, project_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 1, 1, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 1, 2, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (3, 2, 3, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(
        ctx,
        "HAS_IDENTIFIER",
        "VulnerabilityOccurrence",
        "VulnerabilityIdentifier",
        "1/100/",
        3,
    )
    .await;
}

pub async fn processes_security_scans(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_pipelines
            (id, project_id, status, source, tag, traversal_path, _siphon_replicated_at)
        VALUES (500, 1000, 'success', 1, false, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_p_ci_builds
            (id, name, status, project_id, stage_id, allow_failure, traversal_path, _siphon_replicated_at)
        VALUES (600, 'sast-job', 'success', 1000, 1, false, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_security_scans
            (id, build_id, scan_type, status, latest, project_id, pipeline_id,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 600, 1, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 600, 2, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (3, 600, 9, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_security_scan", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT scan_type, status, latest FROM {} FINAL ORDER BY id",
            t("gl_security_scan")
        ))
        .await;
    let batch = &result[0];

    let scan_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "scan_type")
        .expect("scan_type column");
    assert_eq!(scan_types.value(0), "sast");
    assert_eq!(scan_types.value(1), "dependency_scanning");
    assert_eq!(scan_types.value(2), "sarif");

    let statuses =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "status").expect("status column");
    assert_eq!(statuses.value(0), "succeeded");
    assert_eq!(statuses.value(1), "succeeded");
    assert_eq!(statuses.value(2), "succeeded");

    let latest_values =
        ArrowUtils::get_column_by_name::<BooleanArray>(batch, "latest").expect("latest column");
    assert!(latest_values.value(0));
    assert!(latest_values.value(1));
    assert!(latest_values.value(2));

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "SecurityScan", "Project", "1/100/", 3)
        .await;
    assert_edges_have_traversal_path(ctx, "IN_PIPELINE", "SecurityScan", "Pipeline", "1/100/", 3)
        .await;
    assert_edges_have_traversal_path(ctx, "RAN_BY", "SecurityScan", "Job", "1/100/", 3).await;
}

pub async fn processes_security_scan_finding_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;

    ctx.execute(
        "INSERT INTO siphon_vulnerability_scanners
            (id, external_id, name, vendor, project_id, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (1, 'gemnasium', 'Gemnasium', 'GitLab', 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_security_scans
            (id, build_id, scan_type, status, latest, project_id, pipeline_id,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES (100, 600, 1, 1, true, 1000, 500, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_security_findings
            (id, uuid, scan_id, scanner_id, severity, deduplicated, finding_data, project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, '00000000-0000-0000-0000-000000000f01', 100, 1, 5, true, '{\"name\": \"SQL Injection\"}', 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, '00000000-0000-0000-0000-000000000f02', 100, 1, 4, false, '{\"name\": \"XSS\"}', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "HAS_FINDING", "SecurityScan", "Finding", "1/100/", 2)
        .await;
}
