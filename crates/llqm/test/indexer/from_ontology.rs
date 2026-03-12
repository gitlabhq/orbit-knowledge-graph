//! Converts ontology YAML definitions into the indexer test types.
//!
//! Simulates what the real indexer's `from_ontology()` would do: read
//! ontology YAML → produce `NodePlanInput` / `StandaloneEdgePlanInput`
//! that feed into `lower_plans()`.
//!
//! Lives in `test/` because the real implementation belongs in the
//! indexer crate, not in llqm.

use std::path::Path;

use ontology::{
    DataType as OntDataType, EdgeDirection, EdgeEndpointType, EdgeMapping, EdgeTarget, EnumType,
    EtlConfig, EtlScope, Field, NodeEntity, Ontology,
};

use super::orchestrate::*;
use super::types::*;
use llqm::ir::expr::DataType;

/// Load the real ontology and convert into indexer pipeline inputs.
pub fn from_ontology(dir: &Path) -> (Vec<NodePlanInput>, Vec<StandaloneEdgePlanInput>) {
    let ontology = Ontology::load_from_dir(dir).expect("failed to load ontology");
    let edge_table = ontology.edge_table().to_string();

    let mut nodes = Vec::new();
    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };

        let scope = match etl.scope() {
            EtlScope::Global => Scope::Global,
            EtlScope::Namespaced => Scope::Namespaced,
        };

        let extract = build_extract(node, etl);
        let node_columns = build_node_columns(node);
        let edges = build_fk_edges(node, etl);

        nodes.push(NodePlanInput {
            name: node.name.clone(),
            scope,
            extract,
            node_columns,
            edges,
            edge_table: edge_table.clone(),
        });
    }

    let mut standalone_edges = Vec::new();
    for (rel_kind, config) in ontology.edge_etl_configs() {
        let scope = match config.scope {
            EtlScope::Global => Scope::Global,
            EtlScope::Namespaced => Scope::Namespaced,
        };

        let namespaced = config.scope == EtlScope::Namespaced;

        let extract = build_standalone_edge_extract(config);

        let source_id = EdgeId::Column(config.from.id_column.clone());
        let source_kind = endpoint_type_to_edge_kind(&config.from.node_type);
        let target_id = EdgeId::Column(config.to.id_column.clone());
        let target_kind = endpoint_type_to_edge_kind(&config.to.node_type);

        let mut filters = Vec::new();
        filters.push(EdgeFilter::IsNotNull(config.from.id_column.clone()));
        filters.push(EdgeFilter::IsNotNull(config.to.id_column.clone()));
        if let EdgeEndpointType::Column {
            column,
            type_mapping,
        } = &config.to.node_type
            && !type_mapping.is_empty()
        {
            filters.push(EdgeFilter::TypeIn {
                column: column.clone(),
                types: type_mapping.keys().cloned().collect(),
            });
        }
        if let EdgeEndpointType::Column {
            column,
            type_mapping,
        } = &config.from.node_type
            && !type_mapping.is_empty()
        {
            filters.push(EdgeFilter::TypeIn {
                column: column.clone(),
                types: type_mapping.keys().cloned().collect(),
            });
        }

        standalone_edges.push(StandaloneEdgePlanInput {
            name: rel_kind.to_string(),
            scope,
            extract,
            transform: FkEdgeTransformInput {
                relationship_kind: rel_kind.to_string(),
                source_id,
                source_kind,
                target_id,
                target_kind,
                filters,
                namespaced,
            },
            edge_table: edge_table.clone(),
        });
    }

    (nodes, standalone_edges)
}

fn build_extract(node: &NodeEntity, etl: &EtlConfig) -> ExtractDef {
    match etl {
        EtlConfig::Table {
            source,
            watermark,
            deleted,
            order_by,
            scope,
            ..
        } => {
            let is_namespaced = *scope == EtlScope::Namespaced;
            let source_alias = source.chars().next().unwrap_or('t').to_string();

            let mut columns: Vec<ColumnDef> = node
                .fields
                .iter()
                .map(|f| build_column_def(f, &source_alias))
                .collect();

            let join = if is_namespaced {
                // Namespaced table-based entities join against a traversal_paths table
                // Check if traversal_path is already in columns from the source table
                let has_tp_field = node
                    .fields
                    .iter()
                    .any(|f| f.name == "traversal_path" && f.source == "traversal_path");

                if has_tp_field {
                    // traversal_path comes from the source table itself (e.g. merge_requests)
                    None
                } else {
                    Some(JoinDef {
                        table: "traversal_paths".into(),
                        alias: "tp".into(),
                        left_key: "id".into(),
                        right_key: "id".into(),
                        columns: vec![ColumnDef::new("traversal_path", DataType::String)],
                    })
                }
            } else {
                None
            };

            // Remove traversal_path from entity columns if it comes via join
            if join.is_some() {
                columns.retain(|c| c.alias.as_deref().unwrap_or(&c.name) != "traversal_path");
            }

            let sort_keys: Vec<&str> = order_by.iter().map(|s| s.as_str()).collect();

            let entity = if is_namespaced {
                if let Some(j) = join {
                    EntityDef::namespaced(
                        source,
                        &source_alias,
                        &node.destination_table,
                        columns,
                        sort_keys,
                        j,
                    )
                } else {
                    // traversal_path from source table — still namespaced but no join needed
                    EntityDef {
                        source_table: source.clone(),
                        source_alias: source_alias.clone(),
                        columns,
                        sort_keys: order_by.clone(),
                        destination_table: node.destination_table.clone(),
                        join: None,
                        version_column: watermark.clone(),
                        deleted_column: deleted.clone(),
                    }
                }
            } else {
                EntityDef::global(source, &node.destination_table, columns, sort_keys)
            };

            ExtractDef::Table(ExtractInput { entity })
        }
        EtlConfig::Query {
            scope,
            select,
            from,
            where_clause,
            watermark,
            deleted,
            order_by,
            traversal_path_filter,
            ..
        } => {
            let columns = parse_select_columns(select, &node.fields);

            ExtractDef::Query(RawExtractInput {
                columns,
                from: from.clone(),
                watermark: watermark.clone(),
                deleted: deleted.clone(),
                order_by: order_by.clone(),
                namespaced: *scope == EtlScope::Namespaced,
                traversal_path_filter: traversal_path_filter.clone(),
                additional_where: where_clause.clone(),
            })
        }
    }
}

fn build_column_def(field: &Field, table_alias: &str) -> ColumnDef {
    let data_type = convert_data_type(field.data_type);

    if field.data_type == OntDataType::Uuid {
        // UUID fields need toString() wrapping — represented as aliased columns
        // The real indexer wraps them, but for table-based extract we just
        // declare the column and let the transform handle conversion.
        ColumnDef {
            name: field.source.clone(),
            data_type,
            alias: if field.name != field.source {
                Some(field.name.clone())
            } else {
                None
            },
            table_alias: Some(table_alias.into()),
        }
    } else if field.name != field.source {
        ColumnDef::aliased(&field.source, data_type, &field.name)
    } else {
        ColumnDef::new(&field.source, data_type)
    }
}

fn convert_data_type(ont_dt: OntDataType) -> DataType {
    match ont_dt {
        OntDataType::String | OntDataType::Enum | OntDataType::Uuid => DataType::String,
        OntDataType::Int => DataType::Int64,
        OntDataType::Float => DataType::Float64,
        OntDataType::Bool => DataType::Bool,
        OntDataType::Date | OntDataType::DateTime => DataType::DateTime,
    }
}

/// Parse a SQL SELECT clause into `RawExtractColumn` entries.
///
/// For fields with UUID type, wraps in `ToString`. Otherwise bare.
fn parse_select_columns(select: &str, fields: &[Field]) -> Vec<RawExtractColumn> {
    let uuid_sources: Vec<&str> = fields
        .iter()
        .filter(|f| f.data_type == OntDataType::Uuid)
        .map(|f| f.source.as_str())
        .collect();

    select
        .split(", ")
        .map(|col_expr| {
            let col_expr = col_expr.trim();
            // Check if this column references a UUID field
            let is_uuid = uuid_sources.iter().any(|src| {
                col_expr == *src
                    || col_expr.ends_with(&format!(".{src}"))
                    || col_expr.contains(&format!(".{src} AS"))
                    || col_expr.contains(&format!(".{src} as"))
            });

            if is_uuid {
                // Extract the raw column name for toString wrapping
                let name = extract_column_name(col_expr);
                RawExtractColumn::ToString(name.into())
            } else {
                RawExtractColumn::Bare(col_expr.into())
            }
        })
        .collect()
}

/// Extract the source column name from an expression like `table.col AS alias` → `col`,
/// or `col` → `col`.
fn extract_column_name(expr: &str) -> &str {
    let base = if let Some(pos) = expr.to_lowercase().find(" as ") {
        expr[..pos].trim()
    } else {
        expr.trim()
    };
    if let Some(dot_pos) = base.rfind('.') {
        &base[dot_pos + 1..]
    } else {
        base
    }
}

fn build_node_columns(node: &NodeEntity) -> Vec<NodeColumn> {
    node.fields
        .iter()
        .map(|f| {
            if f.data_type == OntDataType::Enum
                && f.enum_type == EnumType::Int
                && f.enum_values.is_some()
            {
                NodeColumn::IntEnum {
                    source: f.source.clone(),
                    target: f.name.clone(),
                    values: f.enum_values.clone().unwrap(),
                }
            } else if f.name != f.source {
                NodeColumn::Rename {
                    source: f.source.clone(),
                    target: f.name.clone(),
                }
            } else {
                NodeColumn::Identity(f.name.clone())
            }
        })
        .collect()
}

fn build_fk_edges(node: &NodeEntity, etl: &EtlConfig) -> Vec<FkEdgeTransformInput> {
    let is_namespaced = etl.scope() == EtlScope::Namespaced;

    etl.edges()
        .iter()
        .map(|(column, mapping)| build_single_fk_edge(column, mapping, &node.name, is_namespaced))
        .collect()
}

fn build_single_fk_edge(
    column: &str,
    mapping: &EdgeMapping,
    node_name: &str,
    namespaced: bool,
) -> FkEdgeTransformInput {
    let (source_id, source_kind, target_id, target_kind) = match mapping.direction {
        EdgeDirection::Outgoing => {
            // Node is source: (this_node.id) -[edge]-> (fk.target)
            let src_id = EdgeId::Column("id".into());
            let src_kind = EdgeKind::Literal(node_name.into());
            let tgt_id = build_edge_id(column, mapping);
            let tgt_kind = edge_target_to_kind(&mapping.target);
            (src_id, src_kind, tgt_id, tgt_kind)
        }
        EdgeDirection::Incoming => {
            // FK target is source: (fk.target) -[edge]-> (this_node.id)
            let src_id = build_edge_id(column, mapping);
            let src_kind = edge_target_to_kind(&mapping.target);
            let tgt_id = EdgeId::Column("id".into());
            let tgt_kind = EdgeKind::Literal(node_name.into());
            (src_id, src_kind, tgt_id, tgt_kind)
        }
    };

    let mut filters = build_edge_filters(column, mapping);

    // Add TypeIn filter for polymorphic column-based targets
    if let EdgeTarget::Column(type_col) = &mapping.target {
        // The ontology doesn't store type_mapping on EdgeTarget::Column for FK edges,
        // but we still filter on the type column being non-null
        filters.push(EdgeFilter::IsNotNull(type_col.clone()));
    }

    FkEdgeTransformInput {
        relationship_kind: mapping.relationship_kind.clone(),
        source_id,
        source_kind,
        target_id,
        target_kind,
        filters,
        namespaced,
    }
}

fn build_edge_id(column: &str, mapping: &EdgeMapping) -> EdgeId {
    if let Some(field) = &mapping.array_field {
        EdgeId::ArrayElement {
            column: column.into(),
            field: field.clone(),
        }
    } else if let Some(delimiter) = &mapping.delimiter {
        EdgeId::Exploded {
            column: column.into(),
            delimiter: delimiter.clone(),
        }
    } else {
        EdgeId::Column(column.into())
    }
}

fn build_edge_filters(column: &str, mapping: &EdgeMapping) -> Vec<EdgeFilter> {
    let mut filters = Vec::new();

    if mapping.array_field.is_some() {
        filters.push(EdgeFilter::ArrayNotEmpty(column.into()));
    } else if mapping.delimiter.is_some() {
        filters.push(EdgeFilter::IsNotNull(column.into()));
        filters.push(EdgeFilter::NotEmpty(column.into()));
    } else {
        filters.push(EdgeFilter::IsNotNull(column.into()));
    }

    filters
}

fn edge_target_to_kind(target: &EdgeTarget) -> EdgeKind {
    match target {
        EdgeTarget::Literal(lit) => EdgeKind::Literal(lit.clone()),
        EdgeTarget::Column(col) => EdgeKind::Column(col.clone()),
    }
}

fn endpoint_type_to_edge_kind(ept: &EdgeEndpointType) -> EdgeKind {
    match ept {
        EdgeEndpointType::Literal(lit) => EdgeKind::Literal(lit.clone()),
        EdgeEndpointType::Column {
            column,
            type_mapping,
        } => {
            if type_mapping.is_empty() {
                EdgeKind::Column(column.clone())
            } else {
                EdgeKind::TypeMapping {
                    column: column.clone(),
                    mapping: type_mapping.clone(),
                }
            }
        }
    }
}

fn build_standalone_edge_extract(config: &ontology::EdgeSourceEtlConfig) -> ExtractDef {
    let is_namespaced = config.scope == EtlScope::Namespaced;

    // Standalone edges are always table-based extracts
    let source = &config.source;
    let source_alias = source.chars().next().unwrap_or('t').to_string();

    let mut columns = vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new(&config.from.id_column, DataType::Int64),
        ColumnDef::new(&config.to.id_column, DataType::Int64),
    ];

    // Add type columns if present
    if let EdgeEndpointType::Column { column, .. } = &config.from.node_type {
        columns.push(ColumnDef::new(column, DataType::String));
    }
    if let EdgeEndpointType::Column { column, .. } = &config.to.node_type
        && !columns.iter().any(|c| c.name == *column)
    {
        columns.push(ColumnDef::new(column, DataType::String));
    }

    let sort_keys: Vec<&str> = config.order_by.iter().map(|s| s.as_str()).collect();

    let join = if is_namespaced {
        Some(JoinDef {
            table: "traversal_paths".into(),
            alias: "tp".into(),
            left_key: "id".into(),
            right_key: "id".into(),
            columns: vec![ColumnDef::new("traversal_path", DataType::String)],
        })
    } else {
        None
    };

    let entity = if is_namespaced {
        EntityDef::namespaced(
            source,
            &source_alias,
            &format!("{source}_extract"),
            columns,
            sort_keys,
            join.unwrap(),
        )
    } else {
        EntityDef::global(source, &format!("{source}_extract"), columns, sort_keys)
    };

    ExtractDef::Table(ExtractInput { entity })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::orchestrate::lower_plans;
    use llqm::backend::clickhouse::ClickHouseBackend;
    use llqm::pipeline::IrPhase;
    use llqm::pipeline::Pipeline;

    fn ontology_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .parent()
            .expect("workspace root")
            .join("config/ontology")
    }

    fn emit_pipeline(pipeline: Pipeline<IrPhase>) -> String {
        pipeline.emit(&ClickHouseBackend).unwrap().finish().sql
    }

    #[test]
    fn loads_ontology_successfully() {
        let (nodes, standalone_edges) = from_ontology(&ontology_dir());
        assert!(!nodes.is_empty(), "should have at least one node");
        assert!(
            !standalone_edges.is_empty(),
            "should have at least one standalone edge"
        );
    }

    #[test]
    fn user_node_is_global() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let user = nodes.iter().find(|n| n.name == "User").expect("User node");
        assert_eq!(user.scope, Scope::Global);
    }

    #[test]
    fn user_has_int_enum_columns() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let user = nodes.iter().find(|n| n.name == "User").expect("User node");

        let user_type_col = user
            .node_columns
            .iter()
            .find(|c| matches!(c, NodeColumn::IntEnum { target, .. } if target == "user_type"))
            .expect("user_type should be IntEnum");

        if let NodeColumn::IntEnum { values, .. } = user_type_col {
            assert!(values.contains_key(&0), "should map 0 → human");
            assert_eq!(values[&0], "human");
        }
    }

    #[test]
    fn user_has_string_enum_state() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let user = nodes.iter().find(|n| n.name == "User").expect("User node");

        // state has enum_type: string, so it should NOT be IntEnum
        let state_col = user
            .node_columns
            .iter()
            .find(|c| match c {
                NodeColumn::Identity(n) => n == "state",
                NodeColumn::Rename { target, .. } => target == "state",
                NodeColumn::IntEnum { target, .. } => target == "state",
            })
            .expect("state column should exist");

        assert!(
            matches!(state_col, NodeColumn::Identity(_)),
            "state is enum_type: string, should be Identity, got: {state_col:?}"
        );
    }

    #[test]
    fn user_has_rename_columns() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let user = nodes.iter().find(|n| n.name == "User").expect("User node");

        let is_admin = user
            .node_columns
            .iter()
            .find(|c| matches!(c, NodeColumn::Rename { target, .. } if target == "is_admin"))
            .expect("is_admin should be Rename");

        if let NodeColumn::Rename { source, target } = is_admin {
            assert_eq!(source, "admin");
            assert_eq!(target, "is_admin");
        }
    }

    #[test]
    fn project_is_namespaced_query() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let project = nodes
            .iter()
            .find(|n| n.name == "Project")
            .expect("Project node");
        assert_eq!(project.scope, Scope::Namespaced);
        assert!(
            matches!(&project.extract, ExtractDef::Query(_)),
            "Project should use query-based extract"
        );
    }

    #[test]
    fn project_has_fk_edges() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let project = nodes
            .iter()
            .find(|n| n.name == "Project")
            .expect("Project node");

        let creator_edge = project
            .edges
            .iter()
            .find(|e| e.relationship_kind == "CREATOR")
            .expect("Project should have CREATOR edge");

        // direction: incoming means User→Project, so source_kind is User
        assert!(
            matches!(&creator_edge.source_kind, EdgeKind::Literal(k) if k == "User"),
            "CREATOR source should be User, got: {:?}",
            creator_edge.source_kind
        );
    }

    #[test]
    fn work_item_has_delimiter_edges() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let wi = nodes
            .iter()
            .find(|n| n.name == "WorkItem")
            .expect("WorkItem node");

        let assigned = wi
            .edges
            .iter()
            .find(|e| e.relationship_kind == "ASSIGNED")
            .expect("WorkItem should have ASSIGNED edge");

        // assignee_ids with delimiter "/" → source (incoming: User) uses Exploded
        assert!(
            matches!(&assigned.source_id, EdgeId::Exploded { delimiter, .. } if delimiter == "/"),
            "ASSIGNED source_id should be Exploded with '/', got: {:?}",
            assigned.source_id
        );
    }

    #[test]
    fn merge_request_has_array_field_edges() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let mr = nodes
            .iter()
            .find(|n| n.name == "MergeRequest")
            .expect("MergeRequest node");

        let assigned = mr
            .edges
            .iter()
            .find(|e| e.relationship_kind == "ASSIGNED")
            .expect("MergeRequest should have ASSIGNED edge");

        // assignees with array_field: user_id → incoming, so source_id uses ArrayElement
        assert!(
            matches!(&assigned.source_id, EdgeId::ArrayElement { column, field }
                if column == "assignees" && field == "user_id"),
            "ASSIGNED source_id should be ArrayElement(assignees, user_id), got: {:?}",
            assigned.source_id
        );

        let has_label = mr
            .edges
            .iter()
            .find(|e| e.relationship_kind == "HAS_LABEL")
            .expect("MergeRequest should have HAS_LABEL edge");

        // label_ids with array_field: label_id → outgoing, so target_id uses ArrayElement
        assert!(
            matches!(&has_label.target_id, EdgeId::ArrayElement { column, field }
                if column == "label_ids" && field == "label_id"),
            "HAS_LABEL target_id should be ArrayElement(label_ids, label_id), got: {:?}",
            has_label.target_id
        );
    }

    #[test]
    fn merge_request_is_table_namespaced() {
        let (nodes, _) = from_ontology(&ontology_dir());
        let mr = nodes
            .iter()
            .find(|n| n.name == "MergeRequest")
            .expect("MergeRequest node");
        assert_eq!(mr.scope, Scope::Namespaced);
        assert!(
            matches!(&mr.extract, ExtractDef::Table(_)),
            "MergeRequest should use table-based extract"
        );
    }

    #[test]
    fn member_of_standalone_edge() {
        let (_, standalone) = from_ontology(&ontology_dir());
        let member_of = standalone
            .iter()
            .find(|e| e.name == "MEMBER_OF")
            .expect("MEMBER_OF standalone edge");

        assert_eq!(member_of.scope, Scope::Namespaced);

        // from: type: User (literal)
        assert!(
            matches!(&member_of.transform.source_kind, EdgeKind::Literal(k) if k == "User"),
            "source_kind should be Literal(User), got: {:?}",
            member_of.transform.source_kind
        );

        // to: type_column: source_type with type_mapping
        assert!(
            matches!(&member_of.transform.target_kind, EdgeKind::TypeMapping { column, mapping }
                if column == "source_type" && mapping.contains_key("Namespace")),
            "target_kind should be TypeMapping, got: {:?}",
            member_of.transform.target_kind
        );
    }

    // =====================================================================
    // YAML → SQL e2e tests: load real ontology → lower_plans → emit SQL
    // =====================================================================

    fn e2e_plans() -> Plans {
        let (nodes, standalone) = from_ontology(&ontology_dir());
        lower_plans(nodes, standalone).expect("lower_plans should succeed")
    }

    // --- User: table-based, global, int-enum + rename ---

    #[test]
    fn e2e_user_extract_sql() {
        let plans = e2e_plans();
        let user = plans
            .global
            .iter()
            .find(|p| p.name == "User")
            .expect("User plan");

        let sql = user.extract.to_sql(&[], 1_000_000).sql;

        assert!(sql.contains("siphon_users"), "source table: {sql}");
        assert!(sql.contains("{last_watermark:String}"), "watermark: {sql}");
        assert!(sql.contains("{watermark:String}"), "watermark: {sql}");
        assert!(sql.contains("ORDER BY"), "sort: {sql}");
        assert!(sql.contains("LIMIT 1000000"), "limit: {sql}");
        assert!(!sql.contains("JOIN"), "global = no join: {sql}");
        assert!(sql.contains("_version"), "version alias: {sql}");
        assert!(sql.contains("_deleted"), "deleted alias: {sql}");
    }

    #[test]
    fn e2e_user_node_transform_sql() {
        let plans = e2e_plans();
        let user = plans
            .global
            .iter()
            .find(|p| p.name == "User")
            .expect("User plan");

        let sql = emit_pipeline(user.transforms[0].pipeline.clone());

        assert!(sql.contains("source_data"), "reads from source_data: {sql}");
        // admin → is_admin rename
        assert!(sql.contains("admin AS is_admin"), "rename: {sql}");
        // user_type int-enum CASE WHEN
        assert!(sql.contains("CASE"), "int-enum: {sql}");
        assert!(sql.contains("END AS user_type"), "int-enum alias: {sql}");
        // state is enum_type: string → identity (no CASE)
        assert!(
            !sql.contains("END AS state"),
            "state should NOT be int-enum: {sql}"
        );
    }

    #[test]
    fn e2e_user_has_no_fk_edges() {
        let plans = e2e_plans();
        let user = plans
            .global
            .iter()
            .find(|p| p.name == "User")
            .expect("User plan");

        // User has no edges defined in etl, so only the node transform
        assert_eq!(
            user.transforms.len(),
            1,
            "User should have only node transform, got {}",
            user.transforms.len()
        );
    }

    // --- Project: query-based, namespaced, INNER JOIN, FK edges ---

    #[test]
    fn e2e_project_extract_sql() {
        let plans = e2e_plans();
        let project = plans
            .namespaced
            .iter()
            .find(|p| p.name == "Project")
            .expect("Project plan");

        let sql = project.extract.to_sql(&[], 500_000).sql;

        assert!(sql.contains("siphon_projects"), "source: {sql}");
        assert!(sql.contains("INNER JOIN"), "join: {sql}");
        assert!(
            sql.contains("project_namespace_traversal_paths"),
            "traversal join table: {sql}"
        );
        assert!(
            sql.contains("project.id IN"),
            "traversal_path_filter: {sql}"
        );
        assert!(sql.contains("{traversal_path:String}"), "ns param: {sql}");
        assert!(sql.contains("LIMIT 500000"), "limit: {sql}");
    }

    #[test]
    fn e2e_project_has_creator_edge() {
        let plans = e2e_plans();
        let project = plans
            .namespaced
            .iter()
            .find(|p| p.name == "Project")
            .expect("Project plan");

        // Node transform + 2 FK edges (CREATOR, CONTAINS)
        assert!(
            project.transforms.len() >= 3,
            "Project should have node + ≥2 edge transforms, got {}",
            project.transforms.len()
        );

        // Find the CREATOR edge transform by checking SQL output
        let edge_sqls: Vec<String> = project.transforms[1..]
            .iter()
            .map(|t| emit_pipeline(t.pipeline.clone()))
            .collect();

        let creator_sql = edge_sqls
            .iter()
            .find(|sql| sql.contains("'CREATOR'"))
            .expect("should have CREATOR edge");

        // incoming: User → Project
        assert!(
            creator_sql.contains("'User' AS source_kind"),
            "CREATOR source: {creator_sql}"
        );
        assert!(
            creator_sql.contains("'Project' AS target_kind"),
            "CREATOR target: {creator_sql}"
        );
    }

    // --- WorkItem: query-based, namespaced, delimiter-based edges ---

    #[test]
    fn e2e_work_item_extract_sql() {
        let plans = e2e_plans();
        let wi = plans
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan");

        let sql = wi.extract.to_sql(&[], 100_000).sql;

        assert!(sql.contains("hierarchy_work_items"), "source: {sql}");
        assert!(sql.contains("LEFT JOIN"), "join: {sql}");
        assert!(
            sql.contains("startsWith(wi.traversal_path, {traversal_path:String})"),
            "ns filter: {sql}"
        );
        assert!(sql.contains("wi.version"), "watermark: {sql}");
    }

    #[test]
    fn e2e_work_item_delimiter_edge_sql() {
        let plans = e2e_plans();
        let wi = plans
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan");

        let edge_sqls: Vec<String> = wi.transforms[1..]
            .iter()
            .map(|t| emit_pipeline(t.pipeline.clone()))
            .collect();

        let assigned_sql = edge_sqls
            .iter()
            .find(|sql| sql.contains("'ASSIGNED'"))
            .expect("should have ASSIGNED edge");

        // delimiter "/" → CAST(NULLIF(unnest(string_to_array(...))))
        assert!(
            assigned_sql.contains("string_to_array(assignee_ids, '/')"),
            "delimiter explode: {assigned_sql}"
        );
        assert!(
            assigned_sql.contains("CAST(NULLIF(unnest("),
            "CAST+NULLIF wrap: {assigned_sql}"
        );
    }

    #[test]
    fn e2e_work_item_has_int_enum_transforms() {
        let plans = e2e_plans();
        let wi = plans
            .namespaced
            .iter()
            .find(|p| p.name == "WorkItem")
            .expect("WorkItem plan");

        let sql = emit_pipeline(wi.transforms[0].pipeline.clone());

        // state (state_id → state) int-enum
        assert!(sql.contains("CASE"), "state int-enum: {sql}");
        assert!(sql.contains("END AS state"), "state enum alias: {sql}");
        // work_item_type (work_item_type_id → work_item_type) int-enum
        assert!(
            sql.contains("END AS work_item_type"),
            "work_item_type enum: {sql}"
        );
    }

    // --- MergeRequest: table-based, namespaced, array_field edges ---

    #[test]
    fn e2e_merge_request_extract_sql() {
        let plans = e2e_plans();
        let mr = plans
            .namespaced
            .iter()
            .find(|p| p.name == "MergeRequest")
            .expect("MergeRequest plan");

        let sql = mr.extract.to_sql(&[], 1_000_000).sql;

        assert!(sql.contains("merge_requests"), "source: {sql}");
        assert!(sql.contains("{last_watermark:String}"), "watermark: {sql}");
        // MR has traversal_path in its own source table, so no JOIN needed
        assert!(sql.contains("traversal_path"), "has tp: {sql}");
    }

    #[test]
    fn e2e_merge_request_array_field_edge_sql() {
        let plans = e2e_plans();
        let mr = plans
            .namespaced
            .iter()
            .find(|p| p.name == "MergeRequest")
            .expect("MergeRequest plan");

        let edge_sqls: Vec<String> = mr.transforms[1..]
            .iter()
            .map(|t| emit_pipeline(t.pipeline.clone()))
            .collect();

        let assigned_sql = edge_sqls
            .iter()
            .find(|sql| sql.contains("'ASSIGNED'"))
            .expect("should have ASSIGNED edge");

        // array_field: user_id → unnest(assignees).user_id
        assert!(
            assigned_sql.contains("unnest(assignees).user_id"),
            "array element: {assigned_sql}"
        );
        assert!(
            assigned_sql.contains("cardinality(assignees)"),
            "array not empty filter: {assigned_sql}"
        );

        let label_sql = edge_sqls
            .iter()
            .find(|sql| sql.contains("'HAS_LABEL'"))
            .expect("should have HAS_LABEL edge");

        assert!(
            label_sql.contains("unnest(label_ids).label_id"),
            "label array element: {label_sql}"
        );
    }

    #[test]
    fn e2e_merge_request_state_enum() {
        let plans = e2e_plans();
        let mr = plans
            .namespaced
            .iter()
            .find(|p| p.name == "MergeRequest")
            .expect("MergeRequest plan");

        let sql = emit_pipeline(mr.transforms[0].pipeline.clone());

        assert!(sql.contains("CASE"), "state int-enum: {sql}");
        assert!(sql.contains("END AS state"), "state enum alias: {sql}");
    }

    // --- member_of: standalone edge, type_mapping ---

    #[test]
    fn e2e_member_of_extract_sql() {
        let plans = e2e_plans();
        let member_of = plans
            .namespaced
            .iter()
            .find(|p| p.name == "MEMBER_OF")
            .expect("MEMBER_OF plan");

        let sql = member_of.extract.to_sql(&[], 1_000_000).sql;

        assert!(sql.contains("siphon_members"), "source: {sql}");
        assert!(sql.contains("INNER JOIN"), "namespaced join: {sql}");
        assert!(sql.contains("traversal_paths"), "traversal join: {sql}");
    }

    #[test]
    fn e2e_member_of_transform_sql() {
        let plans = e2e_plans();
        let member_of = plans
            .namespaced
            .iter()
            .find(|p| p.name == "MEMBER_OF")
            .expect("MEMBER_OF plan");

        let sql = emit_pipeline(member_of.transforms[0].pipeline.clone());

        assert!(
            sql.contains("'MEMBER_OF' AS relationship_kind"),
            "rel kind: {sql}"
        );
        assert!(
            sql.contains("'User' AS source_kind"),
            "source literal: {sql}"
        );
        // target_kind uses TypeMapping CASE WHEN on source_type column
        assert!(sql.contains("CASE"), "type mapping CASE: {sql}");
        assert!(
            sql.contains("source_type ="),
            "type mapping checks source_type: {sql}"
        );
        assert!(
            sql.contains("ELSE source_type END AS target_kind"),
            "type mapping fallback: {sql}"
        );
    }

    // --- Full pipeline: all plans lower successfully ---

    #[test]
    fn e2e_all_plans_lower_and_emit() {
        let plans = e2e_plans();

        let all: Vec<&PipelinePlan> = plans.global.iter().chain(plans.namespaced.iter()).collect();

        assert!(
            all.len() >= 5,
            "should have at least 5 plans, got {}",
            all.len()
        );

        for plan in &all {
            // Extract emits valid SQL
            let extract_sql = plan.extract.to_sql(&[], 1000).sql;
            assert!(
                extract_sql.contains("SELECT"),
                "{}: extract SQL missing SELECT: {extract_sql}",
                plan.name
            );
            assert!(
                extract_sql.contains("ORDER BY"),
                "{}: extract SQL missing ORDER BY: {extract_sql}",
                plan.name
            );

            // All transforms emit valid SQL
            for (i, t) in plan.transforms.iter().enumerate() {
                let sql = emit_pipeline(t.pipeline.clone());
                assert!(
                    sql.contains("SELECT"),
                    "{}[{i}]: transform SQL missing SELECT: {sql}",
                    plan.name
                );
                assert!(
                    sql.contains("source_data"),
                    "{}[{i}]: transform should read from source_data: {sql}",
                    plan.name
                );
            }
        }
    }

    #[test]
    fn e2e_pagination_works_on_ontology_plans() {
        let plans = e2e_plans();

        let user = plans
            .global
            .iter()
            .find(|p| p.name == "User")
            .expect("User");

        // First page: no cursor
        let sql1 = user.extract.to_sql(&[], 10_000).sql;
        assert!(sql1.contains("LIMIT 10000"), "page 1: {sql1}");
        assert!(
            !sql1.contains("> '"),
            "page 1 should have no cursor: {sql1}"
        );

        // Second page: with cursor
        let sql2 = user
            .extract
            .to_sql(&[("id".into(), "500".into())], 10_000)
            .sql;
        assert!(sql2.contains("id > '500'"), "page 2: {sql2}");

        // Base is reusable
        let sql3 = user.extract.to_sql(&[], 5_000).sql;
        assert!(sql3.contains("LIMIT 5000"), "page 3 reuse: {sql3}");
        assert_ne!(sql1, sql3, "different batch sizes → different SQL");
    }
}
