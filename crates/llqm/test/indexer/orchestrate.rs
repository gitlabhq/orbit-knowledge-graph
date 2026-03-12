//! PipelinePlan orchestration.
//!
//! Ties extract + node transform + FK edge transforms into complete
//! `PipelinePlan` values, mirroring the real indexer's `lower()`.
//!
//! Every plan is constructed through `Pipeline`, never by calling
//! `Frontend::lower()` directly. The output holds `Pipeline<IrPhase>`
//! so consumers can add their own passes (security, check) and choose
//! a backend before emitting.
//!
//! ## Pagination model
//!
//! Extract plans separate the **base query** (watermark filter + projection)
//! from **per-page parameters** (cursor, ORDER BY, LIMIT). This mirrors
//! the real indexer's `ExtractQuery` which clones its base AST, injects
//! cursor/sort/limit, then emits SQL on each page.
//!
//! ```text
//! ExtractPlanOutput::to_sql(&[], 1_000_000)     → first page (no cursor)
//! ExtractPlanOutput::to_sql(&cursor, 1_000_000) → subsequent pages
//! ```

use super::frontend::{FkEdgeTransformFrontend, NodeTransformFrontend};
use super::lower;
use super::types::*;
use llqm::backend::clickhouse::{ClickHouseBackend, ParameterizedQuery};
use llqm::ir::expr::{Expr, SortDir};
use llqm::ir::plan::Plan;
use llqm::pipeline::{IrPhase, Pipeline};

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

/// A complete entity pipeline: extract from datalake + transform in-memory.
///
/// All plans are at `Pipeline<IrPhase>` — consumers add passes and emit:
/// ```ignore
/// plan.extract.to_sql(&[], 1_000_000)  // first page
/// ```
pub struct PipelinePlan {
    pub name: String,
    pub extract: ExtractPlanOutput,
    pub transforms: Vec<TransformOutput>,
}

/// Paginated extract query, modeled after the real indexer's `ExtractQuery`.
///
/// Holds the base plan (without cursor/sort/limit) and resolved sort
/// expressions. Per-page SQL is produced by `to_sql()` which clones
/// the base, injects cursor filter + ORDER BY + LIMIT, then emits.
pub struct ExtractPlanOutput {
    pub base: Pipeline<IrPhase>,
    pub sort_exprs: Vec<(Expr, SortDir)>,
}

impl ExtractPlanOutput {
    /// Produce a parameterized SQL query for one page.
    ///
    /// Mirrors `ExtractQuery::to_sql()`: clones base plan, injects cursor
    /// filter (if any), appends ORDER BY + LIMIT, emits via ClickHouse backend.
    pub fn to_sql(
        &self,
        cursor_values: &[(String, String)],
        batch_size: u64,
    ) -> ParameterizedQuery {
        let plan = self.base.clone().into_plan();
        let mut root = plan.root;

        if !cursor_values.is_empty() {
            root = lower::cursor_filter(root, cursor_values);
        }

        root = root.sort(&self.sort_exprs).fetch(batch_size, None);

        let plan = Plan {
            output_names: plan.output_names,
            root,
            ctes: plan.ctes,
        };
        Pipeline::from_plan(plan)
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish()
    }
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
    let extract = lower_extract(&input.extract)?;

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
        extract,
        transforms,
    })
}

fn lower_standalone_edge_plan(
    input: StandaloneEdgePlanInput,
) -> Result<PipelinePlan, OrchestrateError> {
    let extract = lower_extract(&input.extract)?;

    let edge_pipeline = Pipeline::new()
        .input(FkEdgeTransformFrontend, input.transform)
        .lower()
        .map_err(|e| OrchestrateError::Transform(e.to_string()))?;

    Ok(PipelinePlan {
        name: input.name,
        extract,
        transforms: vec![TransformOutput {
            pipeline: edge_pipeline,
            destination_table: input.edge_table,
        }],
    })
}

fn lower_extract(def: &ExtractDef) -> Result<ExtractPlanOutput, OrchestrateError> {
    match def {
        ExtractDef::Table(input) => {
            let base_result =
                lower::extract_base(input).map_err(|e| OrchestrateError::Extract(e.to_string()))?;
            let pipeline = Pipeline::from_plan(base_result.plan);
            Ok(ExtractPlanOutput {
                base: pipeline,
                sort_exprs: base_result.sort_exprs,
            })
        }
        ExtractDef::Query(input) => {
            let base_result = lower::raw_extract_base(input)
                .map_err(|e| OrchestrateError::Extract(e.to_string()))?;
            let pipeline = Pipeline::from_plan(base_result.plan);
            Ok(ExtractPlanOutput {
                base: pipeline,
                sort_exprs: base_result.sort_exprs,
            })
        }
    }
}
