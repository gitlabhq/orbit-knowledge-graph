//! Rows are generated server-side, so seeding never passes through the profiled process.

use std::collections::HashMap;

use anyhow::Context;
use clickhouse_client::ArrowClickHouseClient;
use orbit_server_config::ClickHouseConfiguration;

/// The siphon DDL the integration suite uses, so the profiled extracts see the
/// same engines, projections and index granularities as production.
const SIPHON_DDL: &str = include_str!(concat!(env!("FIXTURES_DIR"), "/siphon.sql"));

/// Digit count matters: id columns are carried per row through every stage.
pub const NAMESPACE_ID_BASE: i64 = 60_000_000;
pub const PROJECT_ID_BASE: i64 = 70_000_000;
const ID_STRIDE: i64 = 10_000_000;
pub const ORGANIZATION_ID: i64 = 1;

/// Every table gets its own id space so a foreign key never collides with the
/// primary key it points at. Order is load-bearing: namespaces and projects
/// must land on the two bases the traversal-path tables are seeded from.
const ID_SPACES: &[&str] = &[
    "siphon_namespaces",
    "siphon_projects",
    "siphon_users",
    "merge_requests",
    "work_items",
    "siphon_notes",
    "siphon_system_note_metadata",
    "siphon_p_ci_pipelines",
    "siphon_p_ci_stages",
    "siphon_p_ci_builds",
    "siphon_milestones",
    "siphon_labels",
    "siphon_merge_request_diffs",
    "siphon_merge_request_diff_files",
    "siphon_merge_request_metrics",
    "siphon_merge_request_reviewers",
    "siphon_merge_request_assignees",
    "siphon_merge_requests_closing_issues",
    "siphon_approvals",
    "siphon_environments",
    "siphon_deployments",
    "siphon_deployment_merge_requests",
    "siphon_ci_runners",
    "siphon_ci_sources_pipelines",
    "siphon_container_repositories",
    "siphon_vulnerabilities",
    "siphon_vulnerability_occurrences",
    "siphon_vulnerability_identifiers",
    "siphon_vulnerability_occurrence_identifiers",
    "siphon_vulnerability_scanners",
    "siphon_vulnerability_merge_request_links",
    "siphon_security_scans",
    "siphon_security_findings",
    "siphon_sbom_occurrences_vulnerabilities",
    "siphon_packages_packages",
    "siphon_packages_package_files",
    "siphon_packages_build_infos",
    "siphon_packages_dependencies",
    "siphon_packages_dependency_links",
    "siphon_packages_package_file_build_infos",
    "siphon_members",
    "siphon_label_links",
    "siphon_issue_assignees",
    "siphon_issue_links",
    "siphon_work_item_parent_links",
    "siphon_resource_state_events",
    "siphon_namespace_details",
    "siphon_routes",
    "namespace_traversal_paths",
    "project_namespace_traversal_paths",
    "siphon_knowledge_graph_enabled_namespaces",
];

/// System-note actions the parser dispatches on. A body whose action is outside
/// this set is logged and dropped, so seeding random actions would measure the
/// drop path instead of the resolver.
const SYSTEM_NOTE_ACTIONS: &[&str] = &["cross_reference", "relate", "moved", "duplicate", "merge"];

const NOTEABLE_TYPES: &[&str] = &["Issue", "MergeRequest"];

#[derive(Clone, Copy)]
pub struct Shape {
    pub namespaces: u64,
    pub projects_per_namespace: u64,
    /// Rows seeded into each entity table, spread evenly over the namespaces.
    pub rows_per_table: u64,
    /// 3 is a project under a root namespace; 4 and 5 model subgroup nesting.
    pub path_depth: usize,
    pub note_bytes: u64,
    pub description_bytes: u64,
    pub title_bytes: u64,
    pub text_bytes: u64,
}

impl Shape {
    pub fn projects(&self) -> u64 {
        (self.namespaces * self.projects_per_namespace).max(1)
    }

    fn rows(&self, table: &str) -> u64 {
        match table {
            "siphon_namespaces"
            | "namespace_traversal_paths"
            | "siphon_knowledge_graph_enabled_namespaces"
            | "siphon_namespace_details" => self.namespaces,
            "siphon_projects" | "project_namespace_traversal_paths" => self.projects(),
            "siphon_routes" => self.namespaces + self.projects(),
            // Half are system notes and half user comments, so the Note and
            // SystemNote pipelines each get a full-width page.
            "siphon_notes" => self.rows_per_table * 2,
            _ => self.rows_per_table,
        }
    }
}

fn id_base(table: &str) -> i64 {
    let index = ID_SPACES
        .iter()
        .position(|candidate| *candidate == table)
        .unwrap_or(ID_SPACES.len());
    NAMESPACE_ID_BASE + ID_STRIDE * index as i64
}

/// Maps a foreign-key column to the table whose id space it draws from, so
/// lookup joins in the generated extracts actually match rows.
fn foreign_key_table(column: &str) -> Option<&'static str> {
    let table = match column {
        "project_id" | "target_project_id" | "source_project_id" | "parent_project_id" => {
            "siphon_projects"
        }
        "namespace_id" | "root_namespace_id" | "group_id" | "parent_id" => "siphon_namespaces",
        "author_id"
        | "user_id"
        | "updated_by_id"
        | "closed_by_id"
        | "resolved_by_id"
        | "assignee_id"
        | "reviewer_id"
        | "approver_id"
        | "created_by_id"
        | "last_edited_by_id"
        | "merge_user_id"
        | "merged_by_id"
        | "dismissed_by_id"
        | "confirmed_by_id"
        | "resolved_by_push_user_id" => "siphon_users",
        "merge_request_id" => "merge_requests",
        "issue_id"
        | "work_item_id"
        | "target_issue_id"
        | "source_issue_id"
        | "moved_to_id"
        | "duplicated_to_id"
        | "promoted_to_epic_id" => "work_items",
        "milestone_id" | "sprint_id" => "siphon_milestones",
        "label_id" => "siphon_labels",
        "pipeline_id" | "source_pipeline_id" | "child_pipeline_id" | "ci_pipeline_id" => {
            "siphon_p_ci_pipelines"
        }
        "stage_id" => "siphon_p_ci_stages",
        "build_id" | "job_id" | "source_job_id" => "siphon_p_ci_builds",
        "vulnerability_id" => "siphon_vulnerabilities",
        "occurrence_id" => "siphon_vulnerability_occurrences",
        "identifier_id" => "siphon_vulnerability_identifiers",
        "scanner_id" => "siphon_vulnerability_scanners",
        "environment_id" => "siphon_environments",
        "deployment_id" => "siphon_deployments",
        "runner_id" => "siphon_ci_runners",
        "package_id" => "siphon_packages_packages",
        "package_file_id" => "siphon_packages_package_files",
        "note_id" => "siphon_notes",
        "container_repository_id" => "siphon_container_repositories",
        "scan_id" | "security_scan_id" => "siphon_security_scans",
        "finding_id" => "siphon_security_findings",
        "merge_request_diff_id" => "siphon_merge_request_diffs",
        _ => return None,
    };
    Some(table)
}

fn text_bytes(column: &str, shape: &Shape) -> u64 {
    match column {
        "note" | "st_diff" | "description_html" => shape.note_bytes,
        "description" | "note_html" | "body" | "message" | "solution" => shape.description_bytes,
        _ => {
            if column == "title" || column.ends_with("name") || column.ends_with("path") {
                shape.title_bytes
            } else {
                shape.text_bytes
            }
        }
    }
}

/// Varies the length per row so the page is not uniformly sized, and uses
/// incompressible bytes so the decoded page is the size the codec suggests.
fn random_text(bytes: u64) -> String {
    let max = bytes.max(4);
    let min = max / 2;
    let spread = max - min + 1;
    format!("substring(randomPrintableASCII({max}), 1, {min} + (number % {spread}))")
}

/// A siphon enum-ish column: a handful of distinct values across the whole
/// table, which is what the dictionary-encoding and LowCardinality paths expect.
fn enumerated(values: &[&str]) -> String {
    let arms: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("[{}][1 + (number % {})]", arms.join(", "), values.len())
}

pub struct Seeder {
    url: String,
    username: String,
    password: Option<String>,
    pub datalake: String,
    pub graph: String,
}

impl Seeder {
    pub fn new(
        url: &str,
        username: &str,
        password: Option<&str>,
        datalake: &str,
        graph: &str,
    ) -> Self {
        Self {
            url: url.to_string(),
            username: username.to_string(),
            password: password.map(str::to_string),
            datalake: datalake.to_string(),
            graph: graph.to_string(),
        }
    }

    pub fn configuration(&self, database: &str) -> ClickHouseConfiguration {
        ClickHouseConfiguration {
            database: database.to_string(),
            url: self.url.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            session_settings: HashMap::new(),
            quorum_writes: false,
            insert_settings: HashMap::new(),
            profiling: Default::default(),
        }
    }

    pub fn client(&self, database: &str) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            database,
            &self.username,
            self.password.as_deref(),
            &HashMap::new(),
            &HashMap::new(),
        )
    }

    pub async fn wait_ready(&self, attempts: u32) -> anyhow::Result<()> {
        let client = self.client("default");
        for attempt in 1..=attempts {
            if client.execute("SELECT 1").await.is_ok() {
                return Ok(());
            }
            if attempt == attempts {
                anyhow::bail!("ClickHouse at {} not ready", self.url);
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(())
    }

    /// Recreated on every run even when the datalake is reused, so each arm of
    /// an A/B writes into an empty database its output can be fingerprinted from.
    pub async fn reset_graph(&self) -> anyhow::Result<()> {
        let default = self.client("default");
        default
            .execute(&format!("DROP DATABASE IF EXISTS {}", self.graph))
            .await
            .context("dropping graph database")?;
        default
            .execute(&format!("CREATE DATABASE {}", self.graph))
            .await
            .context("creating graph database")?;
        let client = self.client(&self.graph);
        for statement in graph_ddl()? {
            client
                .execute(&statement)
                .await
                .with_context(|| format!("graph DDL: {statement}"))?;
        }
        Ok(())
    }

    pub async fn seed_datalake(&self, shape: Shape) -> anyhow::Result<u64> {
        let default = self.client("default");
        default
            .execute(&format!("DROP DATABASE IF EXISTS {}", self.datalake))
            .await
            .context("dropping datalake database")?;
        default
            .execute(&format!("CREATE DATABASE {}", self.datalake))
            .await
            .context("creating datalake database")?;

        let client = self.client(&self.datalake);
        let ddl = SIPHON_DDL
            .replace("$DICTIONARY_USER", &self.username)
            .replace(
                "$DICTIONARY_PASSWORD",
                self.password.as_deref().unwrap_or(""),
            )
            .replace("$DICTIONARY_SECURE", "0")
            .replace("$DICTIONARY_DATABASE", &self.datalake);
        for statement in ddl.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            client
                .execute(statement)
                .await
                .with_context(|| format!("siphon DDL: {statement}"))?;
        }

        let columns = self.table_columns(&client).await?;
        let mut seeded = 0;
        for table in ID_SPACES {
            let Some(table_columns) = columns.get(*table) else {
                continue;
            };
            let rows = shape.rows(table);
            client
                .execute(&generic_insert(table, table_columns, rows, shape))
                .await
                .with_context(|| format!("seeding {table}"))?;
            seeded += rows;
            tracing::info!(table, rows, "seeded");
        }
        Ok(seeded)
    }

    async fn table_columns(
        &self,
        client: &ArrowClickHouseClient,
    ) -> anyhow::Result<HashMap<String, Vec<Column>>> {
        let sql = format!(
            "SELECT table, name, type FROM system.columns \
             WHERE database = '{}' AND default_kind NOT IN ('MATERIALIZED', 'ALIAS') \
             ORDER BY table, position",
            self.datalake
        );
        let batches = client
            .query_arrow(&sql)
            .await
            .context("reading system.columns")?;
        let mut out: HashMap<String, Vec<Column>> = HashMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let (Some(table), Some(name), Some(ty)) = (
                    orbit_utils::arrow::ArrowUtils::get_column_string(batch, "table", row),
                    orbit_utils::arrow::ArrowUtils::get_column_string(batch, "name", row),
                    orbit_utils::arrow::ArrowUtils::get_column_string(batch, "type", row),
                ) else {
                    continue;
                };
                out.entry(table).or_default().push(Column { name, ty });
            }
        }
        Ok(out)
    }
}

struct Column {
    name: String,
    ty: String,
}

fn generic_insert(table: &str, columns: &[Column], rows: u64, shape: Shape) -> String {
    let mut names = Vec::new();
    let mut values = Vec::new();
    for column in columns {
        let Some(value) = column_expression(table, column, shape) else {
            continue;
        };
        names.push(format!("`{}`", column.name));
        values.push(value);
    }
    format!(
        "INSERT INTO {table} ({}) SELECT {} FROM numbers({rows})",
        names.join(", "),
        values.join(", ")
    )
}

/// Columns whose value has to agree with another table rather than merely be
/// well-typed: traversal paths the namespaced extracts filter on, the route
/// paths the system-note resolver reads back, and the note bodies and actions
/// the note parser dispatches on.
fn column_override(table: &str, column: &str, shape: Shape) -> Option<String> {
    let namespaces = shape.namespaces.max(1);
    let projects = shape.projects();
    match (table, column) {
        ("namespace_traversal_paths" | "siphon_namespaces", "id") => {
            Some(format!("{NAMESPACE_ID_BASE} + number"))
        }
        ("namespace_traversal_paths", "traversal_path") => Some(namespace_path_expression()),
        ("siphon_knowledge_graph_enabled_namespaces", "id") => Some("number + 1".to_string()),
        ("siphon_knowledge_graph_enabled_namespaces", "root_namespace_id") => {
            Some(format!("{NAMESPACE_ID_BASE} + number"))
        }
        ("siphon_knowledge_graph_enabled_namespaces", "traversal_path") => {
            Some(namespace_path_expression())
        }
        ("siphon_namespaces" | "siphon_namespace_details", "parent_id") => Some("NULL".to_string()),
        ("siphon_namespace_details", "namespace_id") => {
            Some(format!("{NAMESPACE_ID_BASE} + number"))
        }
        ("project_namespace_traversal_paths" | "siphon_projects", "id") => {
            Some(format!("{PROJECT_ID_BASE} + number"))
        }
        ("siphon_projects", "namespace_id") => {
            Some(format!("{NAMESPACE_ID_BASE} + (number % {namespaces})"))
        }
        (_, "path") if table == "siphon_namespaces" => {
            Some(format!("concat('group', toString(number % {namespaces}))"))
        }
        (_, "path") if table == "siphon_projects" => {
            Some(format!("concat('project', toString(number % {projects}))"))
        }
        ("siphon_routes", "source_id") => Some(format!("{PROJECT_ID_BASE} + number")),
        ("siphon_routes", "source_type") => Some("'Project'".to_string()),
        ("siphon_routes", "path") => Some(format!(
            "concat('group', toString(number % {namespaces}), '/project', toString(number))"
        )),
        ("siphon_notes", "note") => Some(format!(
            "if(number % 2 = 0, concat('mentioned in ', if(number % 4 = 0, '#', '!'), \
             toString(1 + (number % 1000)), ' ', {}), {})",
            random_text(shape.note_bytes),
            random_text(shape.note_bytes)
        )),
        ("siphon_notes", "system") => Some("number % 2 = 0".to_string()),
        ("siphon_notes", "noteable_type") => Some(enumerated(NOTEABLE_TYPES)),
        ("siphon_notes", "noteable_id") => Some(format!(
            "if({} = 'Issue', {} + (number % {}), {} + (number % {}))",
            enumerated(NOTEABLE_TYPES),
            id_base("work_items"),
            shape.rows("work_items").max(1),
            id_base("merge_requests"),
            shape.rows("merge_requests").max(1)
        )),
        ("siphon_notes", "discussion_id") => Some("toString(number)".to_string()),
        // Attaches metadata to the even-numbered notes, which are the system
        // notes; a note without metadata has no action to dispatch on.
        ("siphon_system_note_metadata", "note_id") => {
            Some(format!("{} + (number * 2)", id_base("siphon_notes")))
        }
        ("siphon_system_note_metadata", "action") => Some(enumerated(SYSTEM_NOTE_ACTIONS)),
        _ => None,
    }
}

fn column_expression(table: &str, column: &Column, shape: Shape) -> Option<String> {
    let name = column.name.as_str();
    if let Some(value) = column_override(table, name, shape) {
        return Some(value);
    }
    if name == "id" {
        return Some(format!("{} + number", id_base(table)));
    }
    if name == "traversal_path" {
        return Some(project_path_expression(shape));
    }
    if matches!(
        name,
        "_siphon_replicated_at" | "_siphon_watermark" | "version"
    ) {
        return Some("now64(6)".to_string());
    }
    if matches!(name, "_siphon_deleted" | "deleted") {
        return Some("false".to_string());
    }
    if matches!(
        name,
        "created_at" | "updated_at" | "finished_at" | "started_at"
    ) {
        return Some("now64(6)".to_string());
    }
    if name == "iid" {
        return Some("1 + (number % 100000)".to_string());
    }
    if name == "organization_id" {
        return Some(ORGANIZATION_ID.to_string());
    }
    if name == "state_id" {
        return Some("1 + (number % 2)".to_string());
    }
    if name == "work_item_type_id" {
        return Some("1 + (number % 9)".to_string());
    }
    if let Some(referenced) = foreign_key_table(name) {
        let count = shape.rows(referenced).max(1);
        return Some(format!("{} + (number % {count})", id_base(referenced)));
    }
    // Numeric columns keep their defaults; only text is widened, because text
    // is what sizes a page.
    if column.ty.contains("String") && !column.ty.contains("Array") {
        return Some(random_text(text_bytes(name, &shape)));
    }
    None
}

fn namespace_index_expression(shape: Shape) -> String {
    format!("(number % {})", shape.namespaces.max(1))
}

/// `<org>/<root namespace>/`
fn namespace_path_expression() -> String {
    format!("concat('{ORGANIZATION_ID}/', toString({NAMESPACE_ID_BASE} + number), '/')")
}

/// `<org>/<root namespace>/[subgroup/]*<project namespace>/`.
///
/// Rows in the entity tables outnumber projects, so the project component is
/// taken modulo the project count rather than from `number` directly.
fn project_path_expression(shape: Shape) -> String {
    let projects = shape.projects();
    let root = format!(
        "concat('{ORGANIZATION_ID}/', toString({NAMESPACE_ID_BASE} + {}), '/')",
        namespace_index_expression(shape)
    );
    let mut expression = root;
    for depth in 0..shape.path_depth.saturating_sub(3) {
        expression = format!(
            "concat({expression}, toString({} + intDiv(number % {projects}, {})), '/')",
            PROJECT_ID_BASE + 1_000_000 * (depth as i64 + 1),
            100 * (depth + 1)
        );
    }
    format!("concat({expression}, toString({PROJECT_ID_BASE} + (number % {projects})), '/')")
}

/// The same DDL the dispatcher's migration would have created.
fn graph_ddl() -> anyhow::Result<Vec<String>> {
    use indexer::schema::version::SCHEMA_VERSION;
    use query_engine::compiler::{
        emit_create_materialized_view, emit_create_table,
        generate_graph_materialized_views_with_prefix, generate_graph_tables_with_prefix,
    };

    let ontology = ontology::Ontology::load_embedded().context("ontology must load")?;
    let prefix = if *SCHEMA_VERSION == 0 {
        String::new()
    } else {
        format!("v{}_", *SCHEMA_VERSION)
    };
    let mut statements: Vec<String> = generate_graph_tables_with_prefix(&ontology, &prefix)
        .iter()
        .map(emit_create_table)
        .collect();
    statements.extend(
        generate_graph_materialized_views_with_prefix(&ontology, &prefix)
            .iter()
            .map(emit_create_materialized_view),
    );
    Ok(statements)
}
