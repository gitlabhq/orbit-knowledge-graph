//! The security phase on relational plans: each scan's guard becomes a filter on its scope key
//! column, rendered from the caller's scope set.
//!
//! - `Subtrees`: `startsWith(column, root)`, or-ed over the minimal roots;
//! - `Keys`: `column IN keys`;
//! - `All`: no filter;
//! - an empty set: `false`, so the read returns no rows.
//!
//! Each guard names its own permission, so a read of an entity that needs a stronger
//! permission than its neighbours is filtered down to the scopes where the caller holds it.

use std::collections::HashMap;

use orbit_authorization::{Grants, ScopeKey, ScopeSet};
use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::{TraversalPath, TraversalPathTrie};
use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Operation, Relational,
};
use query_data_model::plan::tracking::{NodeId, Plan, Ref};
use serde_json::Value;

use super::guards::{scan, scope_set};
use crate::error::{QueryError, Result};
use crate::passes::codegen::ClickHousePlan;

pub fn apply_security_context(
    plan: ClickHousePlan,
    catalog: &Catalog<ClickHouse>,
    grants: &Grants,
) -> Result<ClickHousePlan> {
    // No answers at all means the caller was never resolved; an empty result would hide that.
    if grants.is_empty() {
        return Err(QueryError::Security(
            "the caller holds no scope answers; refusing to compile because every read \
             would fall back to false and hide the misconfiguration"
                .into(),
        ));
    }
    let mut guards = guards(&plan.plan, catalog, grants)?;
    catalog
        .rebuild(plan, |q, step| {
            let Some(conditions) = guards.remove(&step.node()) else {
                return step.keep(q);
            };
            let mut conditions = conditions
                .into_iter()
                .map(|condition| step.expr(condition))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            match &mut step.operation {
                Operation::Filter(predicate) => {
                    conditions.push(predicate.clone());
                    if let Some(guarded) = balanced(BinaryOp::And, conditions) {
                        *predicate = guarded;
                    }
                }
                _ => {
                    if let Some(guard) = balanced(BinaryOp::And, conditions) {
                        let source = step.inputs.remove(0);
                        step.inputs.insert(0, q.filter(source, guard)?);
                    }
                }
            }
            step.keep(q)
        })
        .map_err(|error| QueryError::Security(error.to_string()))
}

fn guards(
    graph: &Plan<Relational<ClickHouse>>,
    catalog: &Catalog<ClickHouse>,
    grants: &Grants,
) -> Result<HashMap<NodeId, Vec<Expr>>> {
    let mut guards = HashMap::new();
    for node in graph.nodes() {
        let operation = graph.operation(node);
        let Some(source) = graph.inputs(node).first() else {
            continue;
        };
        if source_of_rows(operation) || !source_of_rows(graph.operation(*source)) {
            continue;
        }
        let mut scans = Vec::new();
        collect_scans(graph, *source, &mut scans);
        let mut conditions = Vec::new();
        for scanned in scans {
            let Operation::Scan { table, .. } = graph.operation(scanned) else {
                continue;
            };
            for guard in scan(catalog, *table).into_iter().flatten() {
                let Some(column) = graph
                    .outputs(scanned)
                    .find(|output| graph.name(*output) == Some(guard.column.as_str()))
                else {
                    continue;
                };
                let set = scope_set(grants, catalog.model(), guard.scope, guard.permission)?;
                conditions.extend(scope_filter(column, set));
            }
        }
        if !conditions.is_empty() {
            guards.insert(node, conditions);
        }
    }
    Ok(guards)
}

fn source_of_rows(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::Scan { .. } | Operation::Read { .. } | Operation::Join(_)
    )
}

fn collect_scans(graph: &Plan<Relational<ClickHouse>>, node: NodeId, scans: &mut Vec<NodeId>) {
    match graph.operation(node) {
        Operation::Join(_) => {
            for input in graph.inputs(node) {
                collect_scans(graph, *input, scans);
            }
        }
        Operation::Scan { .. } => scans.push(node),
        _ => {}
    }
}

/// The filter a scope set puts on a scope key column; `All` puts none.
fn scope_filter(column: Ref, set: &ScopeSet) -> Option<Expr> {
    match set {
        ScopeSet::All => None,
        ScopeSet::Subtrees(roots) => {
            let roots: Vec<TraversalPath> = roots.iter().map(|root| root.as_str().into()).collect();
            Some(path_filter(column, &roots.iter().collect::<Vec<_>>()))
        }
        ScopeSet::Keys(keys) => Some(key_filter(column, keys)),
    }
}

fn key_filter(column: Ref, keys: &[ScopeKey]) -> Expr {
    let (data_type, values): (SqlType, Vec<Value>) = match keys.first() {
        None => return falsehood(),
        Some(ScopeKey::Int(_)) => (SqlType::Int64, keys.iter().map(key_value).collect()),
        Some(ScopeKey::Text(_)) => (SqlType::String, keys.iter().map(key_value).collect()),
    };
    Expr::Binary {
        op: BinaryOp::In,
        left: Box::new(Expr::Column(column)),
        right: Box::new(Expr::Parameter {
            data_type: data_type.to_array(),
            value: Value::Array(values),
        }),
    }
}

pub(crate) fn key_value(key: &ScopeKey) -> Value {
    match key {
        ScopeKey::Int(key) => Value::from(*key),
        ScopeKey::Text(key) => Value::from(key.as_str()),
    }
}

fn falsehood() -> Expr {
    Expr::Parameter {
        data_type: SqlType::Bool,
        value: Value::Bool(false),
    }
}

fn path_filter(column: Ref, paths: &[&TraversalPath]) -> Expr {
    let starts_with = |path: &str| Expr::Call {
        function: Function::StartsWith,
        args: vec![
            Expr::Column(column),
            Expr::Parameter {
                data_type: SqlType::String,
                value: Value::String(path.to_string()),
            },
        ],
    };
    match paths {
        [] => falsehood(),
        [path] => starts_with(path.as_str()),
        _ => {
            let collapsed = TraversalPathTrie::from_paths(paths).to_minimal_prefixes();
            let arms = collapsed.iter().map(|path| starts_with(path.as_str()));
            balanced(BinaryOp::Or, arms.collect()).expect("paths is non-empty")
        }
    }
}

fn balanced(op: BinaryOp, mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 | 1 => exprs.pop(),
        length => {
            let right = exprs.split_off(length / 2);
            Some(Expr::Binary {
                op,
                left: Box::new(balanced(op, exprs)?),
                right: Box::new(balanced(op, right)?),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use query_data_model::Catalog;
    use query_data_model::implementations::relational::plan::{JoinKind, ReadMode};

    use super::*;

    fn model() -> &'static Catalog<ClickHouse> {
        static MODEL: LazyLock<Catalog<ClickHouse>> =
            LazyLock::new(|| Catalog::<ClickHouse>::embedded().unwrap());
        &MODEL
    }

    fn scans(tables: &[(&str, &str)], kind: Option<&str>) -> ClickHousePlan {
        let catalog = model();
        catalog
            .build(|q| {
                let mut rows = None;
                for (table, alias) in tables {
                    let scan = q.scan(catalog.table(table)?, ReadMode::Raw, alias)?;
                    rows = Some(match rows {
                        None => scan,
                        Some(left) => q.join(left, scan, JoinKind::Cross)?,
                    });
                }
                let rows = rows.expect("at least one table");
                let id = rows.qualified(tables[tables.len() - 1].1, "traversal_path")?;
                let rows = match kind {
                    Some(kind) => {
                        let column = rows.qualified(tables[0].1, "relationship_kind")?;
                        let predicate = Expr::Binary {
                            op: BinaryOp::Eq,
                            left: Box::new(column.expr()),
                            right: Box::new(Expr::Parameter {
                                data_type: SqlType::String,
                                value: Value::String(kind.into()),
                            }),
                        };
                        q.filter(rows, predicate)?
                    }
                    None => rows,
                };
                q.select(rows, vec![id])
            })
            .unwrap()
    }

    fn secured(plan: ClickHousePlan, paths: &[&str]) -> String {
        let grants = crate::test_grants::reporter(model().model(), paths);
        let plan = apply_security_context(plan, model(), &grants).unwrap();
        model().render(&plan).sql
    }

    #[test]
    fn grants_with_no_answers_fail_closed() {
        let grants = Grants::new(orbit_authorization::Caller::new("test", false, ()));
        let plan = scans(&[("gl_project", "p")], None);
        let error = apply_security_context(plan, model(), &grants);
        assert!(matches!(error, Err(QueryError::Security(_))));
    }

    #[test]
    fn key_sets_filter_with_an_in_list_and_all_filters_nothing() {
        let grants = |set: ScopeSet| {
            crate::test_grants::reporter(model().model(), &["1/"]).with("namespace", "read", set)
        };
        let render = |set| {
            let plan = scans(&[("gl_project", "p")], None);
            let plan = apply_security_context(plan, model(), &grants(set)).unwrap();
            model().render(&plan).sql
        };
        assert_eq!(
            render(ScopeSet::Keys(vec![ScopeKey::Text("1/2/".into())])),
            "SELECT p.traversal_path FROM gl_project AS p WHERE p.traversal_path IN {p0:Array(String)}"
        );
        assert_eq!(
            render(ScopeSet::All),
            "SELECT p.traversal_path FROM gl_project AS p"
        );
    }

    #[test]
    fn every_namespaced_scan_of_a_join_tree_guards_the_block() {
        let plan = scans(&[("gl_project", "p"), ("gl_edge", "e")], None);
        assert_eq!(
            secured(plan, &["1/"]),
            "SELECT e.traversal_path FROM gl_project AS p INNER JOIN gl_edge AS e ON 1 \
             WHERE (startsWith(p.traversal_path, {p0:String}) \
             AND startsWith(e.traversal_path, {p0:String}))"
        );
        let plan = scans(&[("gl_user", "u"), ("gl_merge_request", "mr")], None);
        assert_eq!(
            secured(plan, &["1/"]),
            "SELECT mr.traversal_path FROM gl_user AS u INNER JOIN gl_merge_request AS mr ON 1 \
             WHERE startsWith(mr.traversal_path, {p0:String})"
        );
    }

    #[test]
    fn guards_join_the_existing_predicate_and_collapse_paths() {
        let plan = scans(&[("gl_edge", "e")], Some("IN_PROJECT"));
        assert_eq!(
            secured(plan, &["1/9970/", "1/6543/", "1/9970/12/"]),
            "SELECT e.traversal_path FROM gl_edge AS e \
             WHERE ((startsWith(e.traversal_path, {p0:String}) \
             OR startsWith(e.traversal_path, {p1:String})) \
             AND (e.relationship_kind = {p2:String}))"
        );
    }
}
