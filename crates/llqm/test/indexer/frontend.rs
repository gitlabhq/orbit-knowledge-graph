//! Frontend implementations for the indexer pipeline.
//!
//! Thin wrappers that implement `Frontend` and delegate to `lower::*`.

use super::lower::{self, LowerError};
use super::types::*;
use llqm::ir::plan::Plan;
use llqm::pipeline::Frontend;

pub struct IndexerFrontend;

impl Frontend for IndexerFrontend {
    type Input = ExtractInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::extract(input)
    }
}

pub struct RawExtractFrontend;

impl Frontend for RawExtractFrontend {
    type Input = RawExtractInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        lower::raw_extract(input)
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
