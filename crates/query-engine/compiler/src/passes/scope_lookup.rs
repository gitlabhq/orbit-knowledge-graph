//! The lookups a remote read's `via` chain needs before its request goes out: the scope key of
//! each repository or workspace the plan reads, read on the graph connection and filtered by the
//! remote read's own scope set.

use orbit_authorization::Grants;
use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::{Guard, Rule};
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, JoinKind, ReadMode, Relation, RelationalError,
};
use query_data_model::implementations::relational::render::ParameterizedSql;
use query_data_model::plan::code::CodeOperation;
use query_data_model::plan::live::LiveOperation;
use query_data_model::{Catalog, Endpoint};
use serde_json::Value;

use super::guards::scope_set;
use super::remote_security::Namespaces;
use super::security::scope_filter;
use crate::error::{QueryError, Result};
use crate::passes::plan::remote::{RemotePlan, Target};

pub const KEY: &str = "id";

/// What a lookup resolves: the repositories a code read names, or every workspace.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolves {
    Repositories,
    Workspaces,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeLookup {
    pub resolves: Resolves,
    pub query: ParameterizedSql,
    /// The column that holds the scope key in the lookup rows.
    pub scope: String,
}

pub fn lookups(
    target: &Target,
    plan: &RemotePlan,
    graph: &Catalog<ClickHouse>,
    grants: &Grants,
) -> Result<Vec<ScopeLookup>> {
    let mut lookups = Vec::new();
    match (target, plan) {
        (Target::Code(code), RemotePlan::Code(plan)) => {
            for node in plan.nodes() {
                if let CodeOperation::Repositories { index, projects } = plan.operation(node) {
                    let keys: Vec<Value> = projects.iter().map(|id| Value::from(*id)).collect();
                    let query = lookup(graph, code.index_guard(*index), grants, Some(keys))?;
                    lookups.extend(query.map(|(query, scope)| ScopeLookup {
                        resolves: Resolves::Repositories,
                        query,
                        scope,
                    }));
                }
            }
        }
        (Target::Live(live), RemotePlan::Live(plan)) => {
            for node in plan.nodes() {
                if let LiveOperation::Call(operation) = plan.operation(node) {
                    let query = lookup(graph, live.operation_guard(*operation), grants, None)?;
                    lookups.extend(query.map(|(query, scope)| ScopeLookup {
                        resolves: Resolves::Workspaces,
                        query,
                        scope,
                    }));
                }
            }
        }
        _ => {
            return Err(QueryError::PipelineInvariant(
                "the plan does not match its target".into(),
            ));
        }
    }
    Ok(lookups)
}

/// The rows of each lookup, as the graph connection returned them, keyed by `key`.
pub fn namespaces(
    results: &[(&ScopeLookup, Vec<query_data_model::catalog::Row>)],
) -> Result<Namespaces> {
    let mut namespaces = Namespaces::default();
    for (lookup, rows) in results {
        for row in rows {
            let scope = row
                .get(&lookup.scope)
                .and_then(Value::as_str)
                .map(TraversalPath::from);
            let invalid = || {
                QueryError::Security(format!(
                    "a scope lookup row lacks {KEY} or {}",
                    lookup.scope
                ))
            };
            let scope = scope.ok_or_else(invalid)?;
            match lookup.resolves {
                Resolves::Repositories => {
                    let key = row.get(KEY).and_then(Value::as_i64).ok_or_else(invalid)?;
                    namespaces.repositories.insert(key, scope);
                }
                Resolves::Workspaces => {
                    let key = row.get(KEY).and_then(Value::as_str).ok_or_else(invalid)?;
                    namespaces.workspaces.insert(key.to_string(), scope);
                }
            }
        }
    }
    Ok(namespaces)
}

/// One scan of the chain: its table and alias, and the columns that join it to the previous one.
struct Step {
    table: String,
    alias: String,
    on: Option<(String, String)>,
}

/// The chain's first target, joined along the rest of the chain to the entity that holds the
/// scope key, filtered to the caller's scope set for the remote read; none for a checked read.
fn lookup(
    graph: &Catalog<ClickHouse>,
    guard: Option<&Guard>,
    grants: &Grants,
    keys: Option<Vec<Value>>,
) -> Result<Option<(ParameterizedSql, String)>> {
    let guard = guard.ok_or_else(|| QueryError::Security("the read has no guard".into()))?;
    let hops = match &guard.rule {
        Rule::Via(hops) => hops,
        Rule::Checked => return Ok(None),
        _ => {
            return Err(QueryError::Security(
                "a remote read is scoped through a via chain or its checks".into(),
            ));
        }
    };
    let set = scope_set(grants, graph.model(), guard.scope, guard.permission)?;
    let shape = |what: &str| QueryError::Security(format!("the scope lookup {what}"));
    let table = |entity| {
        graph
            .table_of(entity)
            .map(str::to_string)
            .ok_or_else(|| shape("reaches an entity the graph connection does not store"))
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
            let mut conditions: Vec<Expr> = scope_filter(scope.expr(), set).into_iter().collect();
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
            q.select(rows, vec![key, scope])
        })
        .map_err(|error| shape(&format!("cannot be built: {error}")))?;
    Ok(Some((graph.render(&plan), scope_column.clone())))
}
