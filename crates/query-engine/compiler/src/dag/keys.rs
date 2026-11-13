use std::collections::HashMap;

use orbit_authorization::Key;
use orbit_server_config::QueryLimits;
use query_data_model::Model;
use query_data_model::plan::logical::{Literal, LogicalOperation, PlanError, Predicate};
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use serde::Serialize;

use super::KeyInput;
use crate::error::Result;

/// The keys a producer stage returned for one key input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct KeySet {
    pub input: KeyInput,
    pub keys: Vec<Key>,
}

/// Binds each key set into the stage's logical plan as an `IN` filter on its binding, right
/// where the plan binds it, so the stage's planner reads the keys before it plans; a composite
/// key set binds as a tuple IN.
pub fn bind(
    statement: Statement,
    sets: &[KeySet],
    model: &Model,
    limits: &QueryLimits,
    connections: &[query_data_model::ConnectionId],
) -> Result<Statement> {
    let limits = QueryLimits {
        in_values: limits.in_values.max(limits.stage_keys),
        pinned_ids: limits.pinned_ids.max(limits.stage_keys),
        ..*limits
    };
    let names: HashMap<Ref, String> = statement
        .plan
        .nodes()
        .flat_map(|node| statement.plan.outputs(node))
        .filter_map(|output| Some((output, statement.plan.name(output)?.to_string())))
        .collect();
    let catalog = model.logical(&limits).as_admin().across(connections);
    let plan = catalog.rebuild(statement.plan, |q, step| {
        let binds = matches!(
            step.operation,
            LogicalOperation::Entities(_) | LogicalOperation::Follow { .. }
        );
        let bound = step
            .outputs()
            .last()
            .and_then(|output| names.get(output))
            .cloned();
        let mut rows = step.keep(q)?;
        let here = |set: &&KeySet| binds && bound.as_ref() == Some(&set.input.binding);
        for set in sets.iter().filter(here) {
            let binding = rows.binding(&set.input.binding)?;
            let predicate = match &set.input.properties[..] {
                [property] => binding.property(*property)?.one_of(single(&set.keys)?),
                properties => Predicate::InTuple {
                    left: properties
                        .iter()
                        .map(|property| binding.property(*property))
                        .collect::<std::result::Result<_, _>>()?,
                    values: set
                        .keys
                        .iter()
                        .map(|key| tuple(key, properties.len()))
                        .collect::<std::result::Result<_, _>>()?,
                },
            };
            rows = q.filter(rows, predicate)?;
        }
        Ok(rows)
    })?;
    Ok(Statement { plan, ..statement })
}

fn single(keys: &[Key]) -> std::result::Result<Vec<Literal>, PlanError> {
    keys.iter()
        .map(crate::logical::anchors::literal)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| PlanError::shape("a single key column reads single keys"))
}

/// A composite key as the literals of a tuple IN, one per key column.
fn tuple(key: &Key, columns: usize) -> std::result::Result<Vec<Literal>, PlanError> {
    let Key::Composite(parts) = key else {
        return Err(PlanError::shape(
            "composite key columns read composite keys",
        ));
    };
    if parts.len() != columns {
        return Err(PlanError::shape(
            "a composite key has one part per key column",
        ));
    }
    single(parts)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orbit_authorization::Key;
    use query_data_model::{Catalog, ClickHouse, DuckDb};

    use super::KeySet;
    use crate::dag::{
        Connections, Env, Frontend, Inputs, KeyInput, Peer, Request, Source, StageQuery,
    };

    /// A composite key set on two columns of `f`, bound into the one stage of a query.
    fn rendered(frontend: Frontend, peer: Peer) -> String {
        let connections = Connections::new(vec![peer]);
        let grants = crate::testkit::admin_ctx();
        let env = Env {
            frontend: &frontend,
            connections: &connections,
            grants: &grants,
            limits: crate::testkit::limits(),
            trace: false,
        };
        let query = "MATCH (f:File) WHERE f.project_id = 42 RETURN f.path LIMIT 5";
        let mut stage = crate::dag::split_query(Source::Text(query), &env)
            .unwrap()
            .into_single()
            .unwrap();
        let keys = KeySet {
            input: KeyInput {
                from: 0,
                columns: vec!["project_id".into(), "path".into()],
                binding: "f".into(),
                properties: {
                    let model = frontend.model();
                    let file = model.names().entity("File").unwrap();
                    ["project_id", "path"]
                        .map(|name| model.names().property(file, name).unwrap())
                        .to_vec()
                },
                types: vec![
                    query_data_model::DataType::Int,
                    query_data_model::DataType::String,
                ],
            },
            keys: vec![
                Key::Composite(vec![Key::Int(42), Key::Text("src/a.rs".into())]),
                Key::Composite(vec![Key::Int(42), Key::Text("src/b.rs".into())]),
            ],
        };
        let StageQuery::Gql(statement) = stage.query else {
            panic!("a GQL stage");
        };
        let model = frontend.model();
        let bound = super::bind(statement, &[keys], model, env.limits, &[]).unwrap();
        stage.query = StageQuery::Gql(bound);
        let compiled = crate::dag::compile(stage, Inputs::default(), &env).unwrap();
        let Request::Query(compiled) = compiled.request else {
            panic!("a relational query");
        };
        compiled.base.sql
    }

    #[test]
    fn a_composite_key_set_renders_as_a_tuple_in_on_clickhouse_and_duckdb() {
        let clickhouse = Arc::new(Catalog::<ClickHouse>::embedded("server").unwrap());
        let clickhouse = rendered(
            Frontend::Server(Arc::clone(&clickhouse)),
            Peer::ClickHouse(clickhouse),
        );
        assert!(
            clickhouse.contains("(f.project_id, f.path) IN (({p"),
            "{clickhouse}"
        );
        let duckdb = Arc::new(Catalog::<DuckDb>::embedded("local").unwrap());
        let duckdb = rendered(Frontend::Local(Arc::clone(&duckdb)), Peer::DuckDb(duckdb));
        assert!(duckdb.contains("(f.project_id, f.path) IN (($"), "{duckdb}");
    }
}
