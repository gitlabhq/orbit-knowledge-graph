//! PipelinePlan orchestration.
//!
//! Ties extract + node transform + FK edge transforms into complete
//! `PipelinePlan` values, mirroring the real indexer's `lower()`.
//!
//! Every plan is constructed through `Pipeline`, never by calling
//! `Frontend::lower()` directly. The output holds `Pipeline<IrPhase>`
//! so consumers can add their own passes (security, check) and choose
//! a backend before emitting.

use super::frontend::IndexerFrontend;
use super::raw_extract::RawExtractFrontend;
use super::transform::{FkEdgeTransformFrontend, NodeTransformFrontend};
use super::types::*;
use llqm::pipeline::{IrPhase, Pipeline};

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

/// A complete entity pipeline: extract from datalake + transform in-memory.
///
/// All plans are at `Pipeline<IrPhase>` — consumers add passes and emit:
/// ```ignore
/// plan.extract.pipeline
///     .pass(&security_pass)?
///     .emit(&ClickHouseBackend)?
///     .finish()
/// ```
pub struct PipelinePlan {
    pub name: String,
    pub extract: ExtractPlanOutput,
    pub transforms: Vec<TransformOutput>,
}

pub struct ExtractPlanOutput {
    pub pipeline: Pipeline<IrPhase>,
    pub sort_keys: Vec<String>,
    pub batch_size: u64,
}

pub struct TransformOutput {
    pub pipeline: Pipeline<IrPhase>,
    pub destination_table: String,
}

/// Partitioned output: global entities (no namespace) vs namespaced.
pub struct Plans {
    pub global: Vec<PipelinePlan>,
    pub namespaced: Vec<PipelinePlan>,
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct NodePlanInput {
    pub name: String,
    pub scope: Scope,
    pub extract: ExtractDef,
    pub node_columns: Vec<NodeColumn>,
    pub edges: Vec<FkEdgeTransformInput>,
    pub edge_table: String,
}

#[derive(Debug, Clone)]
pub struct StandaloneEdgePlanInput {
    pub name: String,
    pub scope: Scope,
    pub extract: ExtractDef,
    pub transform: FkEdgeTransformInput,
    pub edge_table: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Global,
    Namespaced,
}

/// Extract definition — either table-based or query-based.
#[derive(Debug, Clone)]
pub enum ExtractDef {
    Table(ExtractInput),
    Query(RawExtractInput),
}

// ---------------------------------------------------------------------------
// Lowering — all plan construction goes through Pipeline
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum OrchestrateError {
    #[error("extract lowering failed: {0}")]
    Extract(String),
    #[error("transform lowering failed: {0}")]
    Transform(String),
}

pub fn lower_plans(
    nodes: Vec<NodePlanInput>,
    standalone_edges: Vec<StandaloneEdgePlanInput>,
) -> Result<Plans, OrchestrateError> {
    let mut global = Vec::new();
    let mut namespaced = Vec::new();

    for node in nodes {
        let scope = node.scope;
        let plan = lower_node_plan(node)?;
        match scope {
            Scope::Global => global.push(plan),
            Scope::Namespaced => namespaced.push(plan),
        }
    }

    for edge in standalone_edges {
        let scope = edge.scope;
        let plan = lower_standalone_edge_plan(edge)?;
        match scope {
            Scope::Global => global.push(plan),
            Scope::Namespaced => namespaced.push(plan),
        }
    }

    Ok(Plans { global, namespaced })
}

fn lower_node_plan(input: NodePlanInput) -> Result<PipelinePlan, OrchestrateError> {
    let (extract_pipeline, sort_keys, batch_size) = lower_extract(&input.extract)?;

    let node_pipeline = Pipeline::new()
        .input(
            NodeTransformFrontend,
            NodeTransformInput {
                columns: input.node_columns,
            },
        )
        .lower()
        .map_err(|e| OrchestrateError::Transform(e.to_string()))?;

    let node_dest = match &input.extract {
        ExtractDef::Table(e) => e.entity.destination_table.clone(),
        ExtractDef::Query(_) => input.name.clone(),
    };

    let mut transforms = vec![TransformOutput {
        pipeline: node_pipeline,
        destination_table: node_dest,
    }];

    for edge in input.edges {
        let edge_pipeline = Pipeline::new()
            .input(FkEdgeTransformFrontend, edge)
            .lower()
            .map_err(|e| OrchestrateError::Transform(e.to_string()))?;
        transforms.push(TransformOutput {
            pipeline: edge_pipeline,
            destination_table: input.edge_table.clone(),
        });
    }

    Ok(PipelinePlan {
        name: input.name,
        extract: ExtractPlanOutput {
            pipeline: extract_pipeline,
            sort_keys,
            batch_size,
        },
        transforms,
    })
}

fn lower_standalone_edge_plan(
    input: StandaloneEdgePlanInput,
) -> Result<PipelinePlan, OrchestrateError> {
    let (extract_pipeline, sort_keys, batch_size) = lower_extract(&input.extract)?;

    let edge_pipeline = Pipeline::new()
        .input(FkEdgeTransformFrontend, input.transform)
        .lower()
        .map_err(|e| OrchestrateError::Transform(e.to_string()))?;

    Ok(PipelinePlan {
        name: input.name,
        extract: ExtractPlanOutput {
            pipeline: extract_pipeline,
            sort_keys,
            batch_size,
        },
        transforms: vec![TransformOutput {
            pipeline: edge_pipeline,
            destination_table: input.edge_table,
        }],
    })
}

fn lower_extract(
    def: &ExtractDef,
) -> Result<(Pipeline<IrPhase>, Vec<String>, u64), OrchestrateError> {
    match def {
        ExtractDef::Table(input) => {
            let sort_keys = input.entity.sort_keys.clone();
            let batch_size = input.batch_size;
            let pipeline = Pipeline::new()
                .input(IndexerFrontend, input.clone())
                .lower()
                .map_err(|e| OrchestrateError::Extract(e.to_string()))?;
            Ok((pipeline, sort_keys, batch_size))
        }
        ExtractDef::Query(input) => {
            let sort_keys = input.order_by.clone();
            let batch_size = input.batch_size;
            let pipeline = Pipeline::new()
                .input(RawExtractFrontend, input.clone())
                .lower()
                .map_err(|e| OrchestrateError::Extract(e.to_string()))?;
            Ok((pipeline, sort_keys, batch_size))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — emission also goes through Pipeline
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::backend::clickhouse::ClickHouseBackend;
    use llqm::ir::expr::DataType;
    use std::collections::BTreeMap;

    fn user_extract() -> ExtractDef {
        ExtractDef::Table(ExtractInput {
            entity: EntityDef::global(
                "siphon_users",
                "gl_user",
                vec![
                    ColumnDef::new("id", DataType::Int64),
                    ColumnDef::new("name", DataType::String),
                    ColumnDef::new("username", DataType::String),
                ],
                vec!["id"],
            ),
            batch_size: 1_000_000,
            cursor_values: vec![],
        })
    }

    fn project_extract() -> ExtractDef {
        ExtractDef::Query(RawExtractInput {
            columns: vec![
                RawExtractColumn::Bare("project.id AS id".into()),
                RawExtractColumn::Bare(
                    "traversal_paths.traversal_path AS traversal_path".into(),
                ),
                RawExtractColumn::Bare("project.name AS name".into()),
            ],
            from: "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id".into(),
            watermark: "project._siphon_replicated_at".into(),
            deleted: "project._siphon_deleted".into(),
            order_by: vec!["traversal_path".into(), "id".into()],
            batch_size: 500_000,
            namespaced: true,
            traversal_path_filter: Some(
                "startsWith(traversal_path, {traversal_path:String})".into(),
            ),
            additional_where: None,
            cursor_values: vec![],
        })
    }

    /// Emit a pipeline through ClickHouse backend.
    fn emit(pipeline: Pipeline<IrPhase>) -> String {
        pipeline.emit(&ClickHouseBackend).unwrap().finish().sql
    }

    // -- Scope partitioning --

    #[test]
    fn partitions_by_scope() {
        let plans = lower_plans(
            vec![
                NodePlanInput {
                    name: "User".into(),
                    scope: Scope::Global,
                    extract: user_extract(),
                    node_columns: vec![
                        NodeColumn::Identity("id".into()),
                        NodeColumn::Identity("name".into()),
                        NodeColumn::Identity("username".into()),
                    ],
                    edges: vec![],
                    edge_table: "gl_edge".into(),
                },
                NodePlanInput {
                    name: "Project".into(),
                    scope: Scope::Namespaced,
                    extract: project_extract(),
                    node_columns: vec![
                        NodeColumn::Identity("id".into()),
                        NodeColumn::Identity("name".into()),
                    ],
                    edges: vec![],
                    edge_table: "gl_edge".into(),
                },
            ],
            vec![],
        )
        .unwrap();

        let global_names: Vec<&str> = plans.global.iter().map(|p| p.name.as_str()).collect();
        let ns_names: Vec<&str> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();

        assert!(global_names.contains(&"User"));
        assert!(ns_names.contains(&"Project"));
    }

    // -- Node plan produces extract + node transform + FK edge transforms --

    #[test]
    fn node_plan_includes_fk_edge_transforms() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "Note".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Identity("body".into()),
                ],
                edges: vec![
                    FkEdgeTransformInput {
                        relationship_kind: "authored".into(),
                        source_id: EdgeId::Column("author_id".into()),
                        source_kind: EdgeKind::Literal("User".into()),
                        target_id: EdgeId::Column("id".into()),
                        target_kind: EdgeKind::Literal("Note".into()),
                        filters: vec![EdgeFilter::IsNotNull("author_id".into())],
                        namespaced: true,
                    },
                    FkEdgeTransformInput {
                        relationship_kind: "belongs_to".into(),
                        source_id: EdgeId::Column("id".into()),
                        source_kind: EdgeKind::Literal("Note".into()),
                        target_id: EdgeId::Column("project_id".into()),
                        target_kind: EdgeKind::Literal("Project".into()),
                        filters: vec![EdgeFilter::IsNotNull("project_id".into())],
                        namespaced: true,
                    },
                ],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let note = &plans.namespaced[0];
        assert_eq!(note.name, "Note");
        assert!(
            note.transforms.len() >= 3,
            "should have node transform + 2 FK edge transforms, got {}",
            note.transforms.len()
        );
        assert_eq!(note.transforms[1].destination_table, "gl_edge");
        assert_eq!(note.transforms[2].destination_table, "gl_edge");
    }

    // -- Standalone edge produces separate plan --

    #[test]
    fn standalone_edge_produces_separate_plan() {
        let plans = lower_plans(
            vec![],
            vec![StandaloneEdgePlanInput {
                name: "label_links".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                transform: FkEdgeTransformInput {
                    relationship_kind: "labeled".into(),
                    source_id: EdgeId::Column("target_id".into()),
                    source_kind: EdgeKind::Column("target_type".into()),
                    target_id: EdgeId::Column("label_id".into()),
                    target_kind: EdgeKind::Literal("Label".into()),
                    filters: vec![
                        EdgeFilter::IsNotNull("target_id".into()),
                        EdgeFilter::IsNotNull("label_id".into()),
                    ],
                    namespaced: true,
                },
                edge_table: "gl_edge".into(),
            }],
        )
        .unwrap();

        assert_eq!(plans.namespaced.len(), 1);
        let edge_plan = &plans.namespaced[0];
        assert_eq!(edge_plan.name, "label_links");
        assert_eq!(edge_plan.transforms.len(), 1);
        assert_eq!(edge_plan.transforms[0].destination_table, "gl_edge");
    }

    // -- Extract SQL: emit through pipeline --

    #[test]
    fn extract_sql_emits_valid_clickhouse() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "User".into(),
                scope: Scope::Global,
                extract: user_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Identity("name".into()),
                    NodeColumn::Identity("username".into()),
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        // Emit through the pipeline — the whole point
        let pq = plans.global[0]
            .extract
            .pipeline
            .clone()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        let sql = &pq.sql;
        assert!(sql.contains("siphon_users"), "sql: {sql}");
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000000"), "sql: {sql}");
        assert!(sql.contains("{last_watermark:String}"), "sql: {sql}");
    }

    // -- Transform SQL: emit through pipeline --

    #[test]
    fn transform_sql_emits_valid_node_transform() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "User".into(),
                scope: Scope::Global,
                extract: user_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Rename {
                        source: "admin".into(),
                        target: "is_admin".into(),
                    },
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let sql = emit(plans.global[0].transforms[0].pipeline.clone());
        assert!(sql.contains("source_data"), "sql: {sql}");
        assert!(sql.contains("admin AS is_admin"), "sql: {sql}");
    }

    // -- FK edge transform SQL --

    #[test]
    fn fk_edge_transform_sql_correct() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "Group".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                node_columns: vec![NodeColumn::Identity("id".into())],
                edges: vec![FkEdgeTransformInput {
                    relationship_kind: "owns".into(),
                    source_id: EdgeId::Column("id".into()),
                    source_kind: EdgeKind::Literal("Group".into()),
                    target_id: EdgeId::Column("owner_id".into()),
                    target_kind: EdgeKind::Literal("User".into()),
                    filters: vec![EdgeFilter::IsNotNull("owner_id".into())],
                    namespaced: true,
                }],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let sql = emit(plans.namespaced[0].transforms[1].pipeline.clone());
        assert!(sql.contains("'Group' AS source_kind"), "sql: {sql}");
        assert!(sql.contains("'User' AS target_kind"), "sql: {sql}");
        assert!(sql.contains("'owns' AS relationship_kind"), "sql: {sql}");
    }

    // -- Query-based extract with JOIN --

    #[test]
    fn query_extract_with_join() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "Project".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::Identity("name".into()),
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let sql = emit(plans.namespaced[0].extract.pipeline.clone());
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("project.id AS id"), "sql: {sql}");
    }

    // -- Multi-value FK edge --

    #[test]
    fn multi_value_fk_edge() {
        let plans = lower_plans(
            vec![NodePlanInput {
                name: "WorkItem".into(),
                scope: Scope::Namespaced,
                extract: project_extract(),
                node_columns: vec![NodeColumn::Identity("id".into())],
                edges: vec![FkEdgeTransformInput {
                    relationship_kind: "assigned".into(),
                    source_id: EdgeId::Exploded {
                        column: "assignee_ids".into(),
                        delimiter: "/".into(),
                    },
                    source_kind: EdgeKind::Literal("User".into()),
                    target_id: EdgeId::Column("id".into()),
                    target_kind: EdgeKind::Literal("WorkItem".into()),
                    filters: vec![
                        EdgeFilter::IsNotNull("assignee_ids".into()),
                        EdgeFilter::NotEmpty("assignee_ids".into()),
                    ],
                    namespaced: true,
                }],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let sql = emit(plans.namespaced[0].transforms[1].pipeline.clone());
        assert!(
            sql.contains("CAST(NULLIF(unnest(string_to_array("),
            "sql: {sql}"
        );
    }

    // -- Int enum in node transform --

    #[test]
    fn int_enum_in_node_transform() {
        let mut values = BTreeMap::new();
        values.insert(0, "active".into());
        values.insert(1, "blocked".into());

        let plans = lower_plans(
            vec![NodePlanInput {
                name: "User".into(),
                scope: Scope::Global,
                extract: user_extract(),
                node_columns: vec![
                    NodeColumn::Identity("id".into()),
                    NodeColumn::IntEnum {
                        source: "state".into(),
                        target: "state".into(),
                        values,
                    },
                ],
                edges: vec![],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        let sql = emit(plans.global[0].transforms[0].pipeline.clone());
        assert!(sql.contains("CASE"), "sql: {sql}");
        assert!(sql.contains("END AS state"), "sql: {sql}");
    }

    // -- Pipeline<IrPhase> supports adding passes before emit --

    #[test]
    fn pipeline_supports_ir_pass_before_emit() {
        use llqm::ir::plan::Plan;
        use llqm::pipeline::IrPass;

        struct NoopPass;
        impl IrPass for NoopPass {
            type Error = std::convert::Infallible;
            fn transform(&self, plan: Plan) -> Result<Plan, Self::Error> {
                Ok(plan)
            }
        }

        let plans = lower_plans(
            vec![NodePlanInput {
                name: "User".into(),
                scope: Scope::Global,
                extract: user_extract(),
                node_columns: vec![NodeColumn::Identity("id".into())],
                edges: vec![],
                edge_table: "gl_edge".into(),
            }],
            vec![],
        )
        .unwrap();

        // The point: consumer can add passes BETWEEN lowering and emission
        let pq = plans.global[0]
            .extract
            .pipeline
            .clone()
            .pass(&NoopPass)
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        assert!(pq.sql.contains("siphon_users"), "sql: {}", pq.sql);
    }
}
