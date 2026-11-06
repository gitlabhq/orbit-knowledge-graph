use super::pattern;
use super::request::{QueryType, Request};
use super::results;
use crate::plan::logical::{Expr, LogicalBuilder, PlanError, Rows};
use crate::surface::SurfaceError;

pub(super) fn request(q: &LogicalBuilder<'_>, request: &Request) -> Result<(), SurfaceError> {
    for (index, node) in request.nodes.iter().enumerate() {
        if request.nodes[..index].iter().any(|seen| seen.id == node.id) {
            return Err(PlanError::Duplicate(node.id.clone()).into());
        }
    }
    let declared = |alias: &str| -> Result<(), SurfaceError> {
        match request.nodes.iter().any(|node| node.id == alias) {
            true => Ok(()),
            false => Err(PlanError::UnknownBinding {
                name: alias.to_string(),
                available: request.nodes.iter().map(|node| node.id.clone()).collect(),
            }
            .into()),
        }
    };
    for edge in &request.relationships {
        if edge.from == edge.to {
            return Err(SurfaceError::schema(
                "relationship endpoints must use different node aliases",
            ));
        }
        declared(&edge.from)?;
        declared(&edge.to)?;
        if let Some(names) = edge.types.as_array() {
            let names: Vec<String> = names
                .iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect();
            pattern::relationships(q, &names)?;
        } else if let Some(name) = edge.types.as_str() {
            pattern::relationships(q, &[name.to_string()])?;
        }
    }
    let shape = match request.query_type {
        QueryType::Aggregation => request
            .path
            .as_ref()
            .map(|_| "aggregation over shortest paths is not supported"),
        _ if !request.group_by.is_empty() => {
            Some("group_by is only supported for aggregation queries")
        }
        QueryType::Neighbors if request.nodes.len() != 1 => {
            Some("neighbors requires exactly one center node")
        }
        _ => None,
    };
    if let Some(message) = shape {
        return Err(SurfaceError::schema(message));
    }
    if let (QueryType::PathFinding, Some(path)) = (request.query_type, &request.path)
        && let Some(node) = request
            .nodes
            .iter()
            .find(|node| node.id != path.from && node.id != path.to)
    {
        return Err(SurfaceError::schema(format!(
            "node \"{}\" is not referenced by any relationship or path",
            node.id
        )));
    }
    Ok(())
}

pub(super) fn unused(rows: &Rows<'_>, request: &Request) -> Result<(), SurfaceError> {
    for node in &request.nodes {
        if let Ok(binding) = rows.binding(&node.id) {
            results::columns(binding, node.columns.as_ref())?;
        }
    }
    let Some(spec) = &request.order_by else {
        return Ok(());
    };
    let (spec, _) = results::descending(spec);
    let (node, property) = results::reference(spec);
    let binding = rows.binding(node)?;
    let Expr::Property { property: id, .. } = binding.property(property.unwrap_or_default())?
    else {
        return Ok(());
    };
    let model = binding.catalog().model();
    match model.lookup(id) {
        Some(_) => Err(SurfaceError::schema(format!(
            "order_by property {node}.{} is virtual and cannot be read in SQL",
            model.graph().property(id).name
        ))),
        None => Ok(()),
    }
}
