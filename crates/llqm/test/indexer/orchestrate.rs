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
