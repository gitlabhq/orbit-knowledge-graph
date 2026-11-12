use query_data_model::implementations::live::ExtractionRequest;
use query_data_model::implementations::orbit_code::StructuralSearchRequest;
use query_data_model::plan::engine::EnginePlan;
use query_data_model::plan::tracking::{Language, Plan};
use serde::Serialize;
use serde_json::Value;

use crate::passes::codegen::{CompiledQueryContext, ParameterizedQuery};
use crate::passes::scope_lookup::ScopeLookup;

/// What one compiled stage asks its executor for.
#[derive(Debug)]
pub enum Request {
    Query(Box<CompiledQueryContext>),
    Hydration(Box<ParameterizedQuery>),
    Lookup(ScopeLookup),
    Code(StructuralSearchRequest),
    Live(ExtractionRequest),
    /// The engine plan, and the compiled context of the response it produces.
    Engine {
        plan: EnginePlan,
        compiled: Box<CompiledQueryContext>,
    },
}

/// A stage request as plain data, for observers that stream it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Rendered {
    Sql {
        sql: String,
        params: Vec<(String, Value)>,
    },
    Code(StructuralSearchRequest),
    Live(ExtractionRequest),
    Engine {
        plan: Vec<String>,
    },
}

fn sql(query: &ParameterizedQuery) -> Rendered {
    let mut params: Vec<(String, Value)> = query
        .params
        .iter()
        .map(|(name, param)| (name.clone(), param.value.clone()))
        .collect();
    params.sort_by(|a, b| a.0.cmp(&b.0));
    Rendered::Sql {
        sql: query.sql.clone(),
        params,
    }
}

impl Request {
    pub fn rendered(&self) -> Vec<Rendered> {
        match self {
            Self::Query(compiled) => vec![sql(&compiled.base)],
            Self::Hydration(query) => vec![sql(query)],
            Self::Lookup(lookup) => {
                let mut params: Vec<(String, Value)> = lookup
                    .query
                    .parameters
                    .iter()
                    .map(|(name, param)| (name.clone(), param.value.clone()))
                    .collect();
                params.sort_by(|a, b| a.0.cmp(&b.0));
                vec![Rendered::Sql {
                    sql: lookup.query.sql.clone(),
                    params,
                }]
            }
            Self::Code(request) => vec![Rendered::Code(request.clone())],
            Self::Live(request) => vec![Rendered::Live(request.clone())],
            Self::Engine { plan, .. } => vec![Rendered::Engine {
                plan: describe(plan),
            }],
        }
    }
}

/// One line per node of a plan: its operation and the names it declares.
pub fn describe<L: Language>(plan: &Plan<L>) -> Vec<String> {
    plan.nodes()
        .map(|node| {
            let names: Vec<&str> = plan
                .outputs(node)
                .filter_map(|output| plan.name(output))
                .collect();
            format!("{:?} -> [{}]", plan.operation(node), names.join(", "))
        })
        .collect()
}
