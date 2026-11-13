//! The lookups a remote read's `via` chain needs before its request goes out: the scope key of
//! each repository or workspace the plan reads, read on the graph connection and filtered by the
//! remote read's own scope set.

use orbit_authorization::Grants;
use orbit_authorization::Key;
use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Hop;
use query_data_model::catalog::{Guard, Rule};
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, JoinKind, ReadMode, Relation, RelationalError,
};
use query_data_model::implementations::relational::render::ParameterizedSql;
use query_data_model::plan::logical::LogicalPlan;
use query_data_model::plan::tracking::Declare;
use query_data_model::{Catalog, Endpoint, EntityId};
use serde_json::Value;

use super::guards::scope_set;
use super::remote_security::{Namespaces, read_entities};
use super::security::ScopeFilter;
use crate::dag::{Connections, Peer};
use crate::error::{QueryError, Result};
use crate::passes::codegen::SqlBackend;
use crate::passes::plan::remote::{Target, named_repositories};

pub const KEY: &str = "id";

/// What a lookup resolves: the repositories a code read names, or every workspace.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolves {
    Repositories,
    Workspaces,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeLookup {
    /// The entity whose `via` chain the lookup resolves.
    pub entity: EntityId,
    /// The relational connection that stores the chain, which runs the lookup.
    pub connection: query_data_model::ConnectionId,
    pub resolves: Resolves,
    pub query: ParameterizedSql,
    /// The column that holds the scope key in the lookup rows.
    pub scope: String,
}

/// The lookups a remote stage's reads need, from the stage's logical plan with its key sets
/// bound: one per entity read through a `via` chain, of the repositories the plan names or of
/// every workspace, on the relational connection that stores the chain.
pub fn lookup(
    target: &Target,
    logical: &LogicalPlan,
    placed: &[(EntityId, query_data_model::StoreId)],
    connections: &Connections,
    grants: &Grants,
) -> Result<Vec<ScopeLookup>> {
    let mut lookups = Vec::new();
    for entity in read_entities(logical)? {
        let Some(Guard {
            rule: Rule::Via(hops),
            ..
        }) = target.guard(entity)
        else {
            continue;
        };
        let guard = target
            .guard(entity)
            .ok_or_else(|| QueryError::Security("the read has no guard".into()))?;
        let (resolves, keys) = match target {
            Target::Code(code) => {
                let mut projects: Vec<i64> = named_repositories(logical, code, placed)?
                    .into_iter()
                    .filter_map(|project| match project {
                        query_data_model::plan::logical::Literal::Int(project) => Some(project),
                        _ => None,
                    })
                    .collect();
                projects.sort_unstable();
                projects.dedup();
                (
                    Resolves::Repositories,
                    Some(projects.into_iter().map(Value::from).collect()),
                )
            }
            Target::Live(_) => (Resolves::Workspaces, None),
        };
        let first = hops
            .first()
            .ok_or_else(|| QueryError::Security("the scope lookup has an empty via chain".into()))?
            .to;
        let built = connections
            .placed(first)
            .ok()
            .and_then(|placement| connections.get(placement.connection))
            .and_then(|peer| {
                let built = match peer {
                    Peer::ClickHouse(catalog) => build(catalog, guard, hops, grants, keys.clone()),
                    Peer::DuckDb(catalog) => build(catalog, guard, hops, grants, keys.clone()),
                    Peer::Remote(_) => return None,
                };
                Some(built.map(|built| (peer.connection(), built)))
            });
        let (connection, (query, scope)) = built.ok_or_else(|| {
            QueryError::Security(format!(
                "no served relational connection stores {}'s via chain",
                target.model().graph().entity(entity).name
            ))
        })??;
        lookups.push(ScopeLookup {
            entity,
            connection,
            resolves,
            query,
            scope,
        });
    }
    Ok(lookups)
}

/// The lookup's rows, as the relational connection returned them, keyed by `key`.
pub fn namespaces(
    lookup: &ScopeLookup,
    rows: &[query_data_model::catalog::Row],
    namespaces: &mut Namespaces,
) -> Result<()> {
    let resolved = namespaces.0.entry(lookup.entity).or_default();
    for row in rows {
        let invalid = || {
            QueryError::Security(format!(
                "a scope lookup row lacks {KEY} or {}",
                lookup.scope
            ))
        };
        let scope = row
            .get(&lookup.scope)
            .and_then(Value::as_str)
            .map(TraversalPath::from)
            .ok_or_else(invalid)?;
        let key = match (&lookup.resolves, row.get(KEY)) {
            (Resolves::Repositories, Some(Value::Number(id))) => id.as_i64().map(Key::Int),
            (Resolves::Workspaces, Some(Value::String(id))) => Some(Key::Text(id.clone())),
            _ => None,
        };
        resolved.insert(key.ok_or_else(invalid)?, scope);
    }
    Ok(())
}

/// One scan of the chain: its table and alias, and the columns that join it to the previous one.
struct Step {
    table: String,
    alias: String,
    on: Option<(String, String)>,
}

/// The chain's first target, joined along the rest of the chain to the entity that holds the
/// scope key, filtered to the caller's scope set for the remote read.
fn build<B: SqlBackend>(
    graph: &Catalog<B>,
    guard: &Guard,
    hops: &[Hop],
    grants: &Grants,
    keys: Option<Vec<Value>>,
) -> Result<(ParameterizedSql, String)> {
    let filter = ScopeFilter::new(scope_set(
        grants,
        graph.model(),
        guard.scope,
        guard.permission,
    )?)?;
    let shape = |what: &str| QueryError::Security(format!("the scope lookup {what}"));
    let table = |entity| {
        graph.table_of(entity).map(str::to_string).ok_or_else(|| {
            shape(&format!(
                "reaches an entity connection {} does not store",
                graph.model().connection(graph.connection()).name()
            ))
        })
    };
    let column = |property| {
        graph
            .column(property)
            .map(str::to_string)
            .ok_or_else(|| shape("joins on an unstored key"))
    };
    let first = hops
        .first()
        .ok_or_else(|| shape("has an empty via chain"))?
        .to;
    let mut steps = vec![Step {
        table: table(first)?,
        alias: "k0".into(),
        on: None,
    }];
    for (index, hop) in hops.iter().enumerate().skip(1) {
        let key = graph
            .foreign_key(hop.variant)
            .filter(|key| key.holder == Endpoint::Source)
            .ok_or_else(|| shape("follows a step that no foreign key on its source carries"))?;
        steps.push(Step {
            table: table(hop.to)?,
            alias: format!("k{index}"),
            on: Some((column(key.property)?, column(key.referenced_key)?)),
        });
    }
    let end = hops.last().map_or(first, |hop| hop.to);
    let Some(Rule::Column(scope_column)) = graph.guard(end).map(|guard| &guard.rule) else {
        return Err(shape("ends at an entity without a scope key column"));
    };
    let plan = graph
        .build(|q| {
            let mut rows: Option<Relation> = None;
            for (index, step) in steps.iter().enumerate() {
                let scan = q.scan(graph.table(&step.table)?, ReadMode::Current, &step.alias)?;
                rows = Some(match (rows, &step.on) {
                    (None, _) => scan,
                    (Some(left), Some((from, to))) => {
                        let previous = &steps[index - 1].alias;
                        let on = Expr::Binary {
                            op: BinaryOp::Eq,
                            left: Box::new(left.qualified(previous, from)?.expr()),
                            right: Box::new(scan.qualified(&step.alias, to)?.expr()),
                        };
                        q.join(left, scan, JoinKind::Inner(on))?
                    }
                    (Some(left), None) => q.join(left, scan, JoinKind::Cross)?,
                });
            }
            let rows = rows.ok_or(RelationalError::Incomplete("a scope lookup"))?;
            let key = rows.qualified("k0", "id")?;
            let scope = rows.qualified(&steps[steps.len() - 1].alias, scope_column)?;
            let mut conditions: Vec<Expr> = filter.on(scope.expr()).into_iter().collect();
            if let Some(keys) = &keys {
                conditions.push(Expr::Binary {
                    op: BinaryOp::In,
                    left: Box::new(key.expr()),
                    right: Box::new(Expr::Parameter {
                        data_type: SqlType::Int64.to_array(),
                        value: Value::Array(keys.clone()),
                    }),
                });
            }
            let condition = conditions.into_iter().reduce(|left, right| Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            });
            let rows = match condition {
                Some(condition) => q.filter(rows, condition)?,
                None => rows,
            };
            let (rows, columns) = q.compute(
                rows,
                vec![
                    Declare::named(KEY, key.expr()),
                    Declare::named(scope_column.as_str(), scope.expr()),
                ],
            )?;
            q.select(rows, columns)
        })
        .map_err(|error| shape(&format!("cannot be built: {error}")))?;
    Ok((B::render(graph, &plan), scope_column.clone()))
}
