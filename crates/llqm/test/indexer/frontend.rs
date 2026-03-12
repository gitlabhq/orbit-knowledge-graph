//! Frontend implementations for the indexer pipeline.
//!
//! Thin wrappers that implement `Frontend` and delegate to `lower::*`.
//! Extract frontends return the base plan (no cursor/sort/limit);
//! pagination is handled by `ExtractPlanOutput::to_sql()`.

use super::lower::{self, LowerError};
use super::types::*;
use llqm::ir::plan::Plan;
use llqm::pipeline::Frontend;

pub struct IndexerFrontend;

impl Frontend for IndexerFrontend {
    type Input = ExtractInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::extract_base(&input).map(|b| b.plan)
    }
}

pub struct RawExtractFrontend;

impl Frontend for RawExtractFrontend {
    type Input = RawExtractInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::raw_extract_base(&input).map(|b| b.plan)
    }
}

pub struct NodeTransformFrontend;

impl Frontend for NodeTransformFrontend {
    type Input = NodeTransformInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::node_transform(input)
    }
}

pub struct FkEdgeTransformFrontend;

impl Frontend for FkEdgeTransformFrontend {
    type Input = FkEdgeTransformInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::fk_edge_transform(input)
    }
}
