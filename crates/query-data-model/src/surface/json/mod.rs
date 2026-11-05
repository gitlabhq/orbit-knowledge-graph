mod filters;
mod pattern;
mod request;
mod results;

use std::sync::OnceLock;

use serde_json::Value;

use super::{Statement, SurfaceError};
use crate::plan::logical::{Columns, LogicalBuilder, LogicalCatalog, Projection, Rows};
use request::{QueryType, Request};

pub const SCHEMA: &str = include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));

type Built<'m> = Result<Rows<'m>, SurfaceError>;

fn validator() -> &'static jsonschema::Validator {
    static VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();
    VALIDATOR.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA).expect("the query schema is JSON");
        jsonschema::validator_for(&schema).expect("the query schema is a JSON Schema")
    })
}

pub fn parse(json: &str, catalog: LogicalCatalog<'_>) -> Result<Statement, SurfaceError> {
    let value: Value =
        serde_json::from_str(json).map_err(|error| SurfaceError::Syntax(error.to_string()))?;
    let errors: Vec<String> = validator()
        .iter_errors(&value)
        .map(|error| format!("{error} at {}", error.instance_path()))
        .collect();
    if !errors.is_empty() {
        return Err(SurfaceError::Schema(errors.join("; ")));
    }
    let request: Request =
        serde_json::from_value(value).map_err(|error| SurfaceError::Syntax(error.to_string()))?;
    let plan = catalog.build(|q| build(q, &request))?;
    Ok(Statement {
        plan,
        debug: request.options.include_debug_sql,
    })
}

fn build<'m>(q: &mut LogicalBuilder<'m>, request: &Request) -> Built<'m> {
    let rows = match request.query_type {
        QueryType::Aggregation => {
            let rows = pattern::pattern(q, request)?;
            results::aggregate(q, rows, request)?
        }
        QueryType::Traversal => {
            let rows = pattern::pattern(q, request)?;
            let rows = order(q, rows, request)?;
            project(q, rows, request, None)?
        }
        QueryType::Neighbors => {
            let rows = neighbors(q, request)?;
            project(q, rows, request, Some("neighbor"))?
        }
        QueryType::PathFinding => {
            let rows = path(q, request)?;
            project(q, rows, request, Some("path"))?
        }
    };
    Ok(q.limit(rows, request.limit)?)
}

fn neighbors<'m>(q: &mut LogicalBuilder<'m>, request: &Request) -> Built<'m> {
    let Some(config) = &request.neighbors else {
        return Err(SurfaceError::schema(
            "neighbors query requires a 'neighbors' configuration",
        ));
    };
    let center = &request.nodes[0];
    let rows = pattern::node(q, center)?;
    let from = rows.binding(&center.id)?;
    let via = pattern::relationships(q, &config.rel_types)?;
    Ok(q.neighbors(rows, from, &via, config.direction.into(), "neighbor")?)
}

fn path<'m>(q: &mut LogicalBuilder<'m>, request: &Request) -> Built<'m> {
    let Some(config) = &request.path else {
        return Err(SurfaceError::schema(
            "path_finding query requires a 'path' configuration",
        ));
    };
    let via = pattern::relationships(q, &config.rel_types)?;
    if via.is_empty() {
        return Err(SurfaceError::schema(
            "shortest paths require named relationship types, such as [\"CONTAINS\"]; a wildcard expands over every edge type",
        ));
    }
    let endpoint = |alias: &str| {
        request
            .nodes
            .iter()
            .find(|node| node.id == alias)
            .ok_or_else(|| {
                SurfaceError::from(crate::plan::logical::PlanError::UnknownBinding {
                    name: alias.to_string(),
                    available: request.nodes.iter().map(|node| node.id.clone()).collect(),
                })
            })
    };
    let (from, to) = (endpoint(&config.from)?, endpoint(&config.to)?);
    let start = pattern::node(q, from)?;
    let end = pattern::node(q, to)?;
    Ok(q.shortest(start, end, &via, config.max_depth, "path")?)
}

fn order<'m>(q: &mut LogicalBuilder<'m>, rows: Rows<'m>, request: &Request) -> Built<'m> {
    let Some(spec) = &request.order_by else {
        return Ok(rows);
    };
    let (spec, order) = results::descending(spec);
    let (node, property) = results::reference(spec);
    let key = rows.binding(node)?.property(property.unwrap_or_default())?;
    Ok(q.order(rows, [(key, order)])?)
}

fn project<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    request: &Request,
    dynamic: Option<&str>,
) -> Built<'m> {
    let mut items = request
        .nodes
        .iter()
        .filter(|node| rows.binding(&node.id).is_ok())
        .map(|node| results::node_projection(&rows, node))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(name) = dynamic {
        let columns = match request.options.dynamic_columns.as_deref() {
            Some("*") => Columns::All,
            _ => Columns::Default,
        };
        let binding = rows.binding(name)?.output();
        items.push(Projection::Graph { binding, columns }.named(name));
    }
    Ok(q.select(rows, items)?)
}
