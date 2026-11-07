//! Planning turns a validated query into a relational plan. Path finding, neighbors, and hydration
//! read logical plans and build through the catalog in one step.
//!
//! Traversal and aggregation plan a `pattern` (nodes, relations, conditions) whose steps build the
//! joined rows through `rows`, sharing what later steps read again; `filters` and `predicates` are
//! the shared condition vocabulary.

mod current;
pub(crate) mod filters;
mod hydration;
mod neighbors;
mod node;
mod path_finding;
pub(crate) mod pattern;
pub mod predicates;
pub mod query_types;
pub mod rows;

use std::collections::HashMap;

use query_data_model::OrbitQueryModel;
pub use query_data_model::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::input::*;
use crate::logical::Query;
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::passes::lower::{self, LoweredMetadata, NodeBinding};
use crate::tp_bounds::TpBounds;

pub use filters::{BoundFilter, Test};
pub use pattern::{EdgeRelation, Matched, NodePlan};

pub(crate) fn property_value(
    nodes: &HashMap<String, NodePlan>,
    node: &str,
    column: predicates::Column,
) -> predicates::OutputValue {
    match nodes.get(node) {
        Some(plan) => predicates::OutputValue::property(&plan.null_as_epoch, column),
        None => predicates::OutputValue::Column(column),
    }
}

pub(crate) fn sort_key<'m>(
    model: &'m (impl OrbitQueryModel + ?Sized),
    table: &str,
) -> Result<&'m [String]> {
    model
        .table_sort_key(table)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "table '{table}' has no sort key for latest-row resolution"
            ))
        })
}

pub struct Planned<P> {
    pub plan: P,
    pub metadata: LoweredMetadata,
}

pub enum Request<'a> {
    Query(Query<'a>),
    Hydration(&'a HydrationRequest),
}

pub fn plan<M: RelationalModel>(
    request: Request<'_>,
    tp_bounds: Option<&TpBounds>,
    model: &M,
) -> Result<Planned<PlanOf<M>>> {
    let query = match request {
        Request::Query(query) => query,
        Request::Hydration(hydration) => {
            let arms = hydration::arms(hydration, model)?;
            let plan = model
                .catalog()
                .build(|q| hydration::emit(q, &arms))
                .map_err(rejected)?;
            return Ok(Planned {
                plan,
                metadata: LoweredMetadata::default(),
            });
        }
    };
    match query.kind() {
        QueryType::PathFinding => {
            let path = path_finding::PathFinding::new(query, model)?;
            let plan = model
                .catalog()
                .build(|q| path_finding::emit(q, &path))
                .map_err(rejected)?;
            let metadata = LoweredMetadata {
                stable_order: path_finding::stable_order(),
                ..LoweredMetadata::default()
            };
            Ok(Planned { plan, metadata })
        }
        QueryType::Neighbors => {
            let neighbors = neighbors::Neighbors::new(query, model)?;
            let plan = model
                .catalog()
                .build(|q| neighbors::emit(q, &neighbors))
                .map_err(rejected)?;
            let metadata = LoweredMetadata {
                nodes: HashMap::from([(neighbors.center().to_string(), NodeBinding::Projected)]),
                stable_order: neighbors::stable_order(&neighbors),
                ..LoweredMetadata::default()
            };
            Ok(Planned { plan, metadata })
        }
        QueryType::Traversal | QueryType::Aggregation => lower::emit(query, tp_bounds, model),
        QueryType::Hydration => Err(QueryError::PipelineInvariant(
            "hydration plans from a hydration request".into(),
        )),
    }
}
