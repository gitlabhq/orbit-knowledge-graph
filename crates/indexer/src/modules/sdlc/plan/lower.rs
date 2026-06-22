use std::collections::HashSet;

use ontology::{EtlScope, Ontology, QueryTemplate, constants::TRAVERSAL_PATH_COLUMN};

use super::input::{
    DenormalizedColumnProjection, DerivedEntityPlan, EdgeFilter, EdgeId, EdgeKind, EnrichmentSql,
    ExtractColumn, ExtractPlan, ExtractSource, FkEdgeTransform, NodeColumn, NodePlan, PlanInput,
    StandaloneEdgePlan,
};
use super::{Plan, Plans, SOURCE_DATA_TABLE, TransformSpec, Transformation};

const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

pub(in crate::modules::sdlc) fn lower(
    inputs: PlanInput,
    ontology: &Ontology,
    global_batch_size: u64,
    namespaced_batch_size: u64,
    batch_size_overrides: &std::collections::HashMap<String, u64>,
) -> Plans {
    let mut global = Vec::new();
    let mut namespaced = Vec::new();

    for node in inputs.node_plans {
        let scope_default = match node.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&node.name)
            .copied()
            .unwrap_or(scope_default);
        let scope = node.scope;
        let plan = lower_node_plan(node, batch_size, ontology);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    for edge in inputs.standalone_edge_plans {
        let scope_default = match edge.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&edge.relationship_kind)
            .copied()
            .unwrap_or(scope_default);
        let scope = edge.scope;
        let plan = lower_standalone_edge_plan(edge, batch_size, ontology);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    for derived in inputs.derived_entity_plans {
        let scope_default = match derived.scope {
            EtlScope::Global => global_batch_size,
            EtlScope::Namespaced => namespaced_batch_size,
        };
        let batch_size = batch_size_overrides
            .get(&derived.name)
            .copied()
            .unwrap_or(scope_default);
        let scope = derived.scope;
        let plan = lower_derived_entity_plan(derived, batch_size);
        match scope {
            EtlScope::Global => global.push(plan),
            EtlScope::Namespaced => namespaced.push(plan),
        }
    }

    Plans { global, namespaced }
}

fn lower_derived_entity_plan(input: DerivedEntityPlan, batch_size: u64) -> Plan {
    let mut plan = lower_extract_plan(input.extract, batch_size);
    plan.name = input.name;
    plan.transform = TransformSpec::Rust(input.transform);
    plan
}

fn lower_node_plan(input: NodePlan, batch_size: u64, ontology: &Ontology) -> Plan {
    let node_destination = input.extract.destination_table.clone();
    let mut plan = lower_extract_plan(input.extract, batch_size);

    let dict_columns = ontology
        .get_node(&input.name)
        .map(|node| {
            node.storage
                .columns
                .iter()
                .filter(|col| col.ch_type.starts_with("LowCardinality"))
                .map(|col| col.name.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut transforms = vec![Transformation {
        sql: lower_node_transform(&input.columns),
        destination_table: node_destination,
        dict_encode_columns: dict_columns,
    }];

    for fk_edge in &input.edges {
        transforms.push(lower_fk_edge_transform(fk_edge, ontology));
    }

    plan.name = input.name;
    plan.transform = TransformSpec::DataFusion(transforms);
    plan
}

fn edge_table_metadata(relationship_kind: &str, ontology: &Ontology) -> EdgeTableMetadata {
    let table = ontology.edge_table_for_relationship(relationship_kind);

    let sort_key = ontology
        .sort_key_for_table(table)
        .map(|keys| keys.to_vec())
        .unwrap_or_default();

    let dict_columns = ontology
        .edge_table_config(table)
        .map(|config| {
            config
                .storage
                .columns
                .iter()
                .filter(|col| col.ch_type.starts_with("LowCardinality"))
                .map(|col| col.name.clone())
                .collect()
        })
        .unwrap_or_default();

    EdgeTableMetadata {
        sort_key,
        dict_columns,
    }
}

struct EdgeTableMetadata {
    sort_key: Vec<String>,
    dict_columns: HashSet<String>,
}

fn lower_fk_edge_transform(fk_edge: &FkEdgeTransform, ontology: &Ontology) -> Transformation {
    let meta = edge_table_metadata(&fk_edge.relationship_kind, ontology);
    let sql = build_edge_transform_sql(
        &lower_edge_id(&fk_edge.source_id),
        &lower_edge_kind(&fk_edge.source_kind),
        &fk_edge.relationship_kind,
        &lower_edge_id(&fk_edge.target_id),
        &lower_edge_kind(&fk_edge.target_kind),
        fk_edge.namespaced,
        &fk_edge.denormalized_columns,
        &fk_edge.filters,
        &meta.sort_key,
    );
    Transformation {
        sql,
        destination_table: fk_edge.destination_table.clone(),
        dict_encode_columns: meta.dict_columns,
    }
}

fn lower_node_transform(columns: &[NodeColumn]) -> String {
    let mut select_list: Vec<String> = columns.iter().map(lower_node_column).collect();
    select_list.push(VERSION_ALIAS.to_string());
    select_list.push(DELETED_ALIAS.to_string());
    format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "))
}

fn lower_node_column(column: &NodeColumn) -> String {
    match column {
        NodeColumn::Identity(name) => name.clone(),
        NodeColumn::Rename { source, target } => format!("{source} AS {target}"),
        NodeColumn::IntEnum {
            source,
            target,
            values,
            nullable,
        } => {
            let cases: Vec<String> = values
                .iter()
                .map(|(key, value)| format!("WHEN {source} = {key} THEN '{value}'"))
                .collect();
            let null_case = if *nullable {
                format!("WHEN {source} IS NULL THEN NULL ")
            } else {
                format!("WHEN {source} IS NULL THEN '' ")
            };
            format!(
                "CASE {null_case}{} ELSE 'unknown' END AS {target}",
                cases.join(" ")
            )
        }
    }
}

fn lower_standalone_edge_plan(
    input: StandaloneEdgePlan,
    batch_size: u64,
    ontology: &Ontology,
) -> Plan {
    let destination_table = input.extract.destination_table.clone();
    let name = plan_name(&input.relationship_kind, &input.extract.source);
    let mut plan = lower_extract_plan(input.extract, batch_size);
    let meta = edge_table_metadata(&input.relationship_kind, ontology);
    let sql = build_edge_transform_sql(
        &lower_edge_id(&input.source_id),
        &lower_edge_kind(&input.source_kind),
        &input.relationship_kind,
        &lower_edge_id(&input.target_id),
        &lower_edge_kind(&input.target_kind),
        input.namespaced,
        &input.denormalized_columns,
        &input.filters,
        &meta.sort_key,
    );
    plan.name = name;
    plan.transform = TransformSpec::DataFusion(vec![Transformation {
        sql,
        destination_table,
        dict_encode_columns: meta.dict_columns,
    }]);
    plan
}

fn plan_name(relationship_kind: &str, source: &ExtractSource) -> String {
    match source {
        ExtractSource::Table(table) => format!("{relationship_kind}_{table}"),
        ExtractSource::Raw(_) => relationship_kind.to_string(),
    }
}

fn lower_edge_id(id: &EdgeId) -> String {
    match id {
        EdgeId::Column(column) => column.clone(),
        EdgeId::Exploded { column, delimiter } => {
            format!("CAST(NULLIF(unnest(string_to_array({column}, '{delimiter}')), '') AS BIGINT)")
        }
        EdgeId::ArrayElement { column, field } => format!("unnest({column})['{field}']"),
        EdgeId::ArrayUnnest { column } => format!("unnest({column})"),
    }
}

fn lower_edge_kind(kind: &EdgeKind) -> String {
    match kind {
        EdgeKind::Literal(value) => format!("'{value}'"),
        EdgeKind::Column { column, mapping } if mapping.is_empty() => column.clone(),
        EdgeKind::Column { column, mapping } => {
            let cases: Vec<String> = mapping
                .iter()
                .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                .collect();
            format!("CASE {} ELSE {column} END", cases.join(" "))
        }
    }
}

fn lower_filter(filter: &EdgeFilter) -> String {
    match filter {
        EdgeFilter::IsNotNull(column) => format!("({column} IS NOT NULL)"),
        EdgeFilter::NotEmpty(column) => format!("({column} != '')"),
        EdgeFilter::ArrayNotEmpty(column) => format!("(cardinality({column}) > 0)"),
        EdgeFilter::TypeIn { column, types } => {
            let types_list = types
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{column} IN ({types_list})")
        }
    }
}

fn lower_filters(filters: &[EdgeFilter]) -> Option<String> {
    if filters.is_empty() {
        return None;
    }
    Some(
        filters
            .iter()
            .map(lower_filter)
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "SQL builder takes each edge-transform input as a distinct typed parameter"
)]
fn build_edge_transform_sql(
    source_id: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_id: &str,
    target_kind: &str,
    namespaced: bool,
    denormalized: &[DenormalizedColumnProjection],
    filters: &[EdgeFilter],
    sort_key: &[String],
) -> String {
    let select_list = lower_edge_select(
        source_id,
        source_kind,
        relationship_kind,
        target_id,
        target_kind,
        namespaced,
        denormalized,
    );
    let mut sql = format!("SELECT {} FROM {SOURCE_DATA_TABLE}", select_list.join(", "));
    if let Some(where_sql) = lower_filters(filters) {
        sql.push_str(" WHERE ");
        sql.push_str(&where_sql);
    }
    if !sort_key.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&sort_key.join(", "));
    }
    sql
}

fn lower_edge_select(
    source_id: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_id: &str,
    target_kind: &str,
    namespaced: bool,
    denormalized: &[DenormalizedColumnProjection],
) -> Vec<String> {
    let traversal_path = if namespaced {
        "traversal_path".to_string()
    } else {
        "'0/' AS traversal_path".to_string()
    };

    let mut cols = vec![
        traversal_path,
        format!("{source_id} AS source_id"),
        format!("{source_kind} AS source_kind"),
        format!("'{relationship_kind}' AS relationship_kind"),
        format!("{target_id} AS target_id"),
        format!("{target_kind} AS target_kind"),
        VERSION_ALIAS.to_string(),
        DELETED_ALIAS.to_string(),
    ];

    let mut tag_groups: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for d in denormalized {
        let tag_expr = match &d.enum_mapping {
            Some(mapping) => {
                let cases: Vec<String> = mapping
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "WHEN {} = {} THEN '{}'",
                            d.source_column,
                            key,
                            value.replace('\'', "\\'")
                        )
                    })
                    .collect();
                format!(
                    "CASE WHEN {col} IS NULL THEN '{key}:null' ELSE concat('{key}:', CASE {cases} ELSE CAST({col} AS VARCHAR) END) END",
                    key = d.tag_key,
                    cases = cases.join(" "),
                    col = d.source_column
                )
            }
            None => format!(
                "CASE WHEN {col} IS NULL THEN '{key}:null' ELSE concat('{key}:', CAST({col} AS VARCHAR)) END",
                key = d.tag_key,
                col = d.source_column
            ),
        };
        tag_groups
            .entry(d.edge_column.clone())
            .or_default()
            .push(tag_expr);
    }

    // Always emit both tag columns (empty array when unused) so the Arrow
    // batch schema matches the ClickHouse edge table.
    for col_name in &["source_tags", "target_tags"] {
        let expr = match tag_groups.remove(*col_name) {
            Some(tag_exprs) => format!("make_array({})", tag_exprs.join(", ")),
            None => "make_array()".to_string(),
        };
        cols.push(format!("{expr} AS {col_name}"));
    }

    cols
}

fn lower_extract_plan(input: ExtractPlan, batch_size: u64) -> Plan {
    // A verbatim `query:` file is the complete template, used as parsed — it
    // has no synthesized columns. Any other source is a FROM expression the
    // plan selects its own columns from before the paging markers are added.
    let columns_empty = input.columns.is_empty();
    let from_sql = match input.source {
        ExtractSource::Raw(template) if columns_empty => {
            return Plan {
                name: String::new(),
                extract_template: template,
                watermark_column: input.watermark,
                sort_key: input.order_by,
                batch_size,
                transform: TransformSpec::DataFusion(vec![]),
            };
        }
        ExtractSource::Raw(template) => template.raw().to_string(),
        ExtractSource::Table(table) => table,
    };

    let select_list = build_extract_select_list(&input.columns, &input.watermark, &input.deleted);
    let traversal_predicate =
        build_traversal_predicate(input.namespaced, &input.traversal_path_filter);

    let mut where_predicates: Vec<String> = Vec::new();
    if let Some(p) = traversal_predicate {
        where_predicates.push(p);
    }
    if let Some(p) = &input.additional_where {
        where_predicates.push(format!("({p})"));
    }
    let where_clause = if where_predicates.is_empty() {
        "1=1".to_string()
    } else {
        where_predicates.join(" AND ")
    };

    let order_by_sql = input.order_by.join(", ");

    let extract_sql = match &input.enrichment {
        Some(enrichment) => render_cte_template(
            &select_list,
            &from_sql,
            &where_clause,
            &order_by_sql,
            &input.columns,
            enrichment,
        ),
        None => format!(
            "SELECT {select} FROM {from_sql} WHERE {where_clause} {{{{filters}}}} \
             ORDER BY {order_by_sql} {{{{limit}}}}",
            select = select_list.join(", "),
        ),
    };

    Plan {
        name: String::new(),
        extract_template: parse_synthesized(&extract_sql),
        watermark_column: input.watermark,
        sort_key: input.order_by,
        batch_size,
        transform: TransformSpec::DataFusion(vec![]),
    }
}

fn build_extract_select_list(
    columns: &[ExtractColumn],
    watermark: &str,
    deleted: &str,
) -> Vec<String> {
    let mut select_list: Vec<String> = columns.iter().map(lower_extract_column).collect();
    select_list.push(format!("{watermark} AS {VERSION_ALIAS}"));
    select_list.push(format!("{deleted} AS {DELETED_ALIAS}"));
    select_list
}

/// Parses a template the lowering itself assembles. A failure is a bug in the
/// synthesized SQL, not bad input, so it surfaces loudly rather than silently.
fn parse_synthesized(sql: &str) -> QueryTemplate {
    QueryTemplate::parse("synthesized extract", sql)
        .expect("lowering builds a valid marker template")
}

fn build_traversal_predicate(namespaced: bool, custom_filter: &Option<String>) -> Option<String> {
    if !namespaced {
        return None;
    }
    Some(custom_filter.clone().unwrap_or_else(|| {
        format!("startsWith({TRAVERSAL_PATH_COLUMN}, {{traversal_path:String}})")
    }))
}

fn render_cte_template(
    select_list: &[String],
    from_sql: &str,
    where_clause: &str,
    order_by_sql: &str,
    columns: &[ExtractColumn],
    enrichment: &EnrichmentSql,
) -> String {
    let outer_cols: Vec<String> = columns
        .iter()
        .map(|c| {
            let name = extract_column_alias(c);
            format!("_batch.{name} AS {name}")
        })
        .chain([
            format!("_batch.{VERSION_ALIAS} AS {VERSION_ALIAS}"),
            format!("_batch.{DELETED_ALIAS} AS {DELETED_ALIAS}"),
        ])
        .chain(enrichment.select_exprs.iter().cloned())
        .collect();

    format!(
        "WITH _batch AS (\
         SELECT {select} FROM {from_sql} \
         WHERE {where_clause} {{{{filters}}}} \
         ORDER BY {order_by_sql} {{{{limit}}}}\
         ), {cte_defs} \
         SELECT {outer_select} FROM _batch {joins}",
        select = select_list.join(", "),
        cte_defs = enrichment.cte_defs.join(", "),
        outer_select = outer_cols.join(", "),
        joins = enrichment.join_clauses.join(" "),
    )
}

fn extract_column_alias(column: &ExtractColumn) -> String {
    let raw = match column {
        ExtractColumn::Bare(name) => name.as_str(),
        ExtractColumn::ToString(name) => name.as_str(),
        ExtractColumn::DateClamp(name) => name.as_str(),
    };
    raw.rsplit_once(" AS ")
        .map(|(_, alias)| alias.trim().to_string())
        .unwrap_or_else(|| raw.to_string())
}

fn lower_extract_column(column: &ExtractColumn) -> String {
    match column {
        ExtractColumn::Bare(name) => name.clone(),
        ExtractColumn::ToString(name) => format!("toString({name}) AS {name}"),
        // Postgres `date` is wider than ClickHouse `Date32` (1900-01-01..2299-12-31).
        // Clamp at the projection layer; a single out-of-range row would
        // poison the whole Arrow batch.
        ExtractColumn::DateClamp(name) => format!(
            "if({name} >= toDate('1900-01-01') AND {name} <= toDate('2299-12-31'), {name}, NULL) AS {name}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::super::input;
    use super::super::{Cursor, CursorFilter, TraversalPathFilter, WatermarkFilter};
    use super::*;
    use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn test_ontology() -> ontology::Ontology {
        ontology::Ontology::load_embedded().expect("should load ontology")
    }

    fn build_plans(ontology: &ontology::Ontology, batch_size: u64) -> Plans {
        lower(
            input::from_ontology(ontology),
            ontology,
            batch_size,
            batch_size,
            &std::collections::HashMap::new(),
        )
    }

    fn render_namespaced_extract(plan: &Plan, path: &str) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(TraversalPathFilter { path })
            .to_sql()
    }

    fn render_global_extract(plan: &Plan) -> String {
        plan.prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .to_sql()
    }

    #[test]
    fn build_plans_partitions_by_scope() {
        let ontology = test_ontology();
        let plans = build_plans(&ontology, 1_000_000);

        let global_names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let namespaced_names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

        assert!(global_names.contains(&"User"));
        assert!(namespaced_names.contains(&"Group"));
        assert!(namespaced_names.contains(&"Project"));
    }

    #[test]
    fn batch_size_override_applies_to_named_pipeline() {
        let ontology = test_ontology();
        let overrides = std::collections::HashMap::from([("WorkItem".to_string(), 50_000u64)]);
        let plans = lower(
            input::from_ontology(&ontology),
            &ontology,
            1_000_000,
            1_000_000,
            &overrides,
        );

        let work_item = plans
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan should exist");
        assert_eq!(work_item.batch_size, 50_000);

        let group = plans
            .namespaced
            .iter()
            .find(|p| p.name == "Group")
            .expect("Group plan should exist");
        assert_eq!(group.batch_size, 1_000_000);
    }

    #[test]
    fn node_plan_includes_fk_edge_transforms() {
        let ontology = test_ontology();
        let plans = build_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        assert!(note_plan.transformations().len() >= 2);
        assert_eq!(
            note_plan.transformations()[0].destination_table,
            prefixed_table_name("gl_note", *SCHEMA_VERSION),
        );
        assert_eq!(
            note_plan.transformations()[1].destination_table,
            prefixed_table_name(ontology.edge_table(), *SCHEMA_VERSION),
        );
    }

    #[test]
    fn note_has_note_edge_transform_applies_type_mapping() {
        let ontology = test_ontology();
        let plans = build_plans(&ontology, 1_000_000);

        let note_plan = plans.namespaced.iter().find(|p| p.name == "Note").unwrap();
        let sql = note_plan
            .transformations()
            .iter()
            .map(|t| t.sql.clone())
            .find(|sql| sql.contains("'HAS_NOTE' AS relationship_kind"))
            .expect("HAS_NOTE transform on Note plan");

        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("WHEN noteable_type = 'Epic' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("'MergeRequest'"), "sql: {sql}");
        assert!(sql.contains("'Vulnerability'"), "sql: {sql}");
    }

    #[test]
    fn enriched_standalone_edge_extract_sql() {
        let ontology = test_ontology();
        let plans = build_plans(&ontology, 1_000_000);

        let plan = plans
            .namespaced
            .iter()
            .find(|p| {
                let sql = render_namespaced_extract(p, "1/2/");
                sql.contains("siphon_issue_assignees")
            })
            .expect("siphon_issue_assignees plan");

        let sql = render_namespaced_extract(plan, "1/2/");

        assert!(sql.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(sql.contains("_e0 AS ("), "sql: {sql}");
        assert!(sql.contains("FROM _batch"), "sql: {sql}");
        assert!(sql.contains("LEFT JOIN _e0"), "sql: {sql}");
        assert!(sql.contains("argMax("), "sql: {sql}");
        assert!(sql.contains("GROUP BY id"), "sql: {sql}");
        assert!(
            sql.contains("id IN (SELECT DISTINCT issue_id FROM _batch)"),
            "sql: {sql}"
        );

        let e0_body = sql
            .split("_e0 AS (")
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("");
        assert!(
            !e0_body.contains("traversal_path"),
            "enrichment CTE body should not filter by traversal_path: {e0_body}"
        );

        assert!(
            plan.transformations()[0].sql.contains("target_tags"),
            "transform should produce target_tags"
        );
    }

    #[test]
    fn node_transform_handles_rename_and_enum() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".to_string());
        values.insert(1, "blocked".to_string());

        let columns = vec![
            NodeColumn::Identity("id".to_string()),
            NodeColumn::Rename {
                source: "admin".to_string(),
                target: "is_admin".to_string(),
            },
            NodeColumn::IntEnum {
                source: "state".to_string(),
                target: "state".to_string(),
                values,
                nullable: false,
            },
        ];

        let sql = lower_node_transform(&columns);
        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
        assert!(sql.contains("WHEN state = 0 THEN 'active'"), "sql: {sql}");
        assert!(sql.contains("ELSE 'unknown' END AS state"), "sql: {sql}");
    }

    #[test]
    fn node_transform_preserves_nullable_int_enum_nulls() {
        let mut values = BTreeMap::new();
        values.insert(1, "script_failure".to_string());

        let columns = vec![NodeColumn::IntEnum {
            source: "failure_reason".to_string(),
            target: "failure_reason".to_string(),
            values,
            nullable: true,
        }];

        let sql = lower_node_transform(&columns);
        assert!(sql.contains("WHEN failure_reason IS NULL THEN NULL"));
        assert!(sql.contains("WHEN failure_reason = 1 THEN 'script_failure'"));
        assert!(sql.contains("ELSE 'unknown' END AS failure_reason"));
    }

    #[test]
    fn fk_edge_transform_outgoing_literal() {
        let fk_edge = FkEdgeTransform {
            relationship_kind: "owns".to_string(),
            source_id: EdgeId::Column("id".to_string()),
            source_kind: EdgeKind::Literal("Group".to_string()),
            target_id: EdgeId::Column("owner_id".to_string()),
            target_kind: EdgeKind::Literal("User".to_string()),
            filters: vec![EdgeFilter::IsNotNull("owner_id".to_string())],
            namespaced: true,
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };

        let sql = lower_fk_edge_transform(&fk_edge, &test_ontology()).sql;

        assert!(sql.contains("id AS source_id"));
        assert!(sql.contains("'Group' AS source_kind"));
        assert!(sql.contains("owner_id AS target_id"));
        assert!(sql.contains("'User' AS target_kind"));
        assert!(sql.contains("(owner_id IS NOT NULL)"));
    }

    #[test]
    fn fk_edge_transform_type_mapping_collapses_raw_values() {
        let mut mapping = BTreeMap::new();
        mapping.insert("Issue".to_string(), "WorkItem".to_string());
        mapping.insert("Epic".to_string(), "WorkItem".to_string());

        let fk_edge = FkEdgeTransform {
            relationship_kind: "HAS_NOTE".to_string(),
            source_id: EdgeId::Column("noteable_id".to_string()),
            source_kind: EdgeKind::Column {
                column: "noteable_type".to_string(),
                mapping,
            },
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("Note".to_string()),
            filters: vec![
                EdgeFilter::IsNotNull("noteable_id".to_string()),
                EdgeFilter::TypeIn {
                    column: "noteable_type".to_string(),
                    types: vec!["MergeRequest".to_string(), "Issue".to_string()],
                },
            ],
            namespaced: true,
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };

        let sql = lower_fk_edge_transform(&fk_edge, &test_ontology()).sql;
        assert!(
            sql.contains("WHEN noteable_type = 'Issue' THEN 'WorkItem'"),
            "sql: {sql}"
        );
        assert!(sql.contains("ELSE noteable_type END"), "sql: {sql}");
        assert!(
            sql.contains("noteable_type IN ('MergeRequest', 'Issue')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn fk_edge_transform_exploded_id_and_array_element() {
        let fk_edge_exploded = FkEdgeTransform {
            relationship_kind: "assigned".to_string(),
            source_id: EdgeId::Exploded {
                column: "assignee_ids".to_string(),
                delimiter: "/".to_string(),
            },
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("WorkItem".to_string()),
            filters: vec![
                EdgeFilter::IsNotNull("assignee_ids".to_string()),
                EdgeFilter::NotEmpty("assignee_ids".to_string()),
            ],
            namespaced: true,
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };
        let sql = lower_fk_edge_transform(&fk_edge_exploded, &test_ontology()).sql;
        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array(assignee_ids, '/')), '') AS BIGINT)"),
            "sql: {sql}"
        );
        assert!(sql.contains("(assignee_ids != '')"), "sql: {sql}");

        let fk_edge_array = FkEdgeTransform {
            relationship_kind: "assigned".to_string(),
            source_id: EdgeId::ArrayElement {
                column: "assignees".to_string(),
                field: "user_id".to_string(),
            },
            source_kind: EdgeKind::Literal("User".to_string()),
            target_id: EdgeId::Column("id".to_string()),
            target_kind: EdgeKind::Literal("MergeRequest".to_string()),
            filters: vec![EdgeFilter::ArrayNotEmpty("assignees".to_string())],
            namespaced: true,
            destination_table: "gl_edge".to_string(),
            denormalized_columns: vec![],
        };
        let sql = lower_fk_edge_transform(&fk_edge_array, &test_ontology()).sql;
        assert!(sql.contains("unnest(assignees)['user_id']"), "sql: {sql}");
        assert!(sql.contains("(cardinality(assignees) > 0)"), "sql: {sql}");
    }

    #[test]
    fn extract_plan_produces_template_with_markers() {
        let extract = ExtractPlan {
            destination_table: "gl_user".to_string(),
            columns: vec![ExtractColumn::Bare("id".to_string())],
            source: ExtractSource::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = lower_extract_plan(extract, 1000);
        assert_eq!(plan.watermark_column, "_siphon_watermark");
        assert_eq!(plan.sort_key, vec!["id"]);
        assert_eq!(plan.batch_size, 1000);
        assert!(plan.extract_template.raw().contains("{{filters}}"));
        assert!(plan.extract_template.raw().contains("{{limit}}"));
    }

    #[test]
    fn extract_plan_table_etl_emits_expected_sql() {
        let extract = ExtractPlan {
            destination_table: "gl_user".to_string(),
            columns: vec![
                ExtractColumn::Bare("id".to_string()),
                ExtractColumn::Bare("name".to_string()),
            ],
            source: ExtractSource::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = lower_extract_plan(extract, 1000);
        let sql = render_global_extract(&plan);
        assert!(sql.contains("SELECT id, name,"), "sql: {sql}");
        assert!(sql.contains("_siphon_watermark AS _version"), "sql: {sql}");
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_user"), "sql: {sql}");
        assert!(sql.contains("ORDER BY id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
    }

    #[test]
    fn extract_plan_clamps_date_columns() {
        let extract = ExtractPlan {
            destination_table: "gl_work_item".to_string(),
            columns: vec![ExtractColumn::DateClamp("due_date".to_string())],
            source: ExtractSource::Table("siphon_work_items".to_string()),
            base_table: "siphon_work_items".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = lower_extract_plan(extract, 1000);
        let sql = render_global_extract(&plan);
        assert!(
            sql.contains("if(due_date >= toDate('1900-01-01') AND due_date <= toDate('2299-12-31'), due_date, NULL) AS due_date"),
            "sql: {sql}"
        );
    }

    #[test]
    fn extract_plan_query_etl_uses_raw_from_and_filter() {
        let extract = ExtractPlan {
            destination_table: "gl_project".to_string(),
            columns: vec![
                ExtractColumn::Bare("project.id AS id".to_string()),
                ExtractColumn::Bare(
                    "traversal_paths.traversal_path AS traversal_path".to_string(),
                ),
            ],
            source: ExtractSource::Raw(
                QueryTemplate::parse(
                    "test",
                    "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id",
                )
                .unwrap(),
            ),
            base_table: "siphon_projects".to_string(),
            watermark: "project._siphon_watermark".to_string(),
            deleted: "project._siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            namespaced: true,
            traversal_path_filter: Some(
                "startsWith(traversal_path, {traversal_path:String})".to_string(),
            ),
            additional_where: None,
            enrichment: None,
        };

        let plan = lower_extract_plan(extract, 500);
        let sql = render_namespaced_extract(&plan, "1/2/");

        assert!(sql.contains("project.id AS id"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }

    #[test]
    fn cursor_filter_renders_dnf_in_extract_sql() {
        let extract = ExtractPlan {
            destination_table: "gl_user".to_string(),
            columns: vec![ExtractColumn::Bare("id".to_string())],
            source: ExtractSource::Table("siphon_user".to_string()),
            base_table: "siphon_user".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            enrichment: None,
        };

        let plan = lower_extract_plan(extract, 1000);
        let cursor = Cursor::from_checkpoint(&crate::checkpoint::Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(vec!["42".to_string()]),
            resume_floor: None,
        });

        let sql = plan
            .prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(CursorFilter {
                sort_key: &plan.sort_key,
                values: cursor.values(),
            })
            .to_sql();

        assert!(sql.contains("(id > '42')"), "sql: {sql}");
        assert!(
            sql.contains("_siphon_watermark > {last_watermark:String}"),
            "sql: {sql}"
        );
    }

    // Render every plan derived from the embedded ontology and check the SQL
    // has no leftover markers, no malformed splice, and the structural pieces
    // (watermark, _version/_deleted aliases, namespace-only traversal filter).
    #[test]
    fn every_plan_renders_valid_sql() {
        use ontology::EtlScope;

        let ontology = test_ontology();
        let plans = build_plans(&ontology, 1_000_000);

        let cases = plans
            .global
            .iter()
            .map(|p| (p, EtlScope::Global))
            .chain(plans.namespaced.iter().map(|p| (p, EtlScope::Namespaced)));

        let mut count = 0;
        for (plan, scope) in cases {
            count += 1;
            let sql = match scope {
                EtlScope::Global => render_global_extract(plan),
                EtlScope::Namespaced => render_namespaced_extract(plan, "1/2/"),
            };
            let name = &plan.name;

            assert!(
                !sql.contains("{{filters}}"),
                "{name}: unresolved {{filters}}: {sql}"
            );
            assert!(
                !sql.contains("{{limit}}"),
                "{name}: unresolved {{limit}}: {sql}"
            );
            assert!(
                !sql.contains("WHERE WHERE"),
                "{name}: malformed double-WHERE: {sql}"
            );
            assert!(
                !sql.contains("AND AND"),
                "{name}: malformed double-AND: {sql}"
            );
            assert!(
                sql.contains("_version"),
                "{name}: missing _version alias: {sql}"
            );
            assert!(
                sql.contains("_deleted"),
                "{name}: missing _deleted alias: {sql}"
            );
            assert!(
                sql.contains("> {last_watermark:String}"),
                "{name}: missing watermark lower bound: {sql}"
            );
            assert!(
                sql.contains("<= {watermark:String}"),
                "{name}: missing watermark upper bound: {sql}"
            );
            if scope == EtlScope::Namespaced {
                assert!(
                    sql.contains("startsWith"),
                    "{name}: missing traversal_path filter: {sql}"
                );
            } else {
                assert!(
                    !sql.contains("traversal_path"),
                    "{name}: global plan should not reference traversal_path: {sql}"
                );
            }
        }
        assert!(count > 0, "ontology produced no plans");
    }

    #[test]
    fn system_note_extract_bounds_metadata_join_to_page() {
        let ontology = test_ontology();
        let plans = build_plans(&ontology, 10_000);

        let plan = plans
            .namespaced
            .iter()
            .find(|p| p.name == "SystemNote")
            .expect("SystemNote plan");

        let sql = render_namespaced_extract(plan, "1/2/");

        assert!(sql.contains("WITH _batch AS ("), "sql: {sql}");
        assert!(sql.contains("LIMIT 10000"), "sql: {sql}");

        let batch_body = sql
            .split("WITH _batch AS (")
            .nth(1)
            .and_then(|s| s.split("),\n_e0 AS (").next())
            .unwrap_or("");
        assert!(
            batch_body.contains("FROM siphon_notes AS sn"),
            "batch body: {batch_body}"
        );
        // #830: the base scan must not join the metadata table above the LIMIT;
        // that would build a namespace-wide hash table per batch.
        assert!(
            !batch_body.contains("siphon_system_note_metadata"),
            "_batch must not join the metadata table: {batch_body}"
        );

        assert!(
            sql.contains("note_id IN (SELECT DISTINCT id FROM _batch)"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("LEFT JOIN _e0 ON _batch.id = _e0.id"),
            "sql: {sql}"
        );
        assert!(sql.contains("_e0.action AS action"), "sql: {sql}");
    }
}
