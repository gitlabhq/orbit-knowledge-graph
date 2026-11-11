//! Security filter injection for multi-tenant isolation.
//!
//! Injects path filters on every scan whose guard names a path column.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(p1) OR startsWith(p2) OR ...`
//!
//! # Per-entity role scoping
//!
//! Each guard carries the entity's role floor. Before injecting the
//! `startsWith` predicate for a scan we drop any traversal path where the
//! user's access level is below that floor.
//!
//! This closes the aggregation-query oracle where a Reporter-only user
//! could count or binary-search properties on a higher-privilege entity
//! (e.g. Vulnerability) by pairing a Project `group_by` with a Vulnerability
//! target. Now the target entity's scan is filtered down to zero paths
//! (producing a Bool(false) predicate) and the aggregation counts nothing.

use std::collections::HashMap;

use orbit_utils::query_types::SqlType;
use orbit_utils::traversal_path::{TraversalPath, TraversalPathTrie};
use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Operation, Relational,
};
use query_data_model::plan::tracking::{NodeId, Plan, Ref};
use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::passes::codegen::ClickHousePlan;
pub use crate::types::SecurityContext;

/// Per-scan path columns and role floors come from the loader's guards.
/// Tables without authorization metadata keep the Reporter floor.
pub fn apply_security_context(
    plan: ClickHousePlan,
    catalog: &Catalog<ClickHouse>,
    ctx: &SecurityContext,
) -> Result<ClickHousePlan> {
    // An entirely empty security context is treated as a fail-closed bug:
    // the caller forgot to populate traversal paths. Emitting `Bool(false)`
    // here would silently return empty results, which is indistinguishable
    // from "user has no namespaces" and obscures the root cause. Note that
    // this differs from a role-mismatch empty-path set for a specific
    // alias — in that case the user has paths, just none at the required
    // role, and returning zero rows for the protected entity is the
    // intended behavior.
    if ctx.traversal_paths.is_empty() {
        return Err(QueryError::Security(
            "security context has no traversal_path entries; refusing to compile \
             because every gl_* alias would fall back to Bool(false) and hide \
             the underlying auth misconfiguration"
                .into(),
        ));
    }
    let mut guards = guards(&plan.plan, catalog, ctx);
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
    ctx: &SecurityContext,
) -> HashMap<NodeId, Vec<Expr>> {
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
        let conditions: Vec<Expr> = scans
            .into_iter()
            .filter_map(|scan| {
                let Operation::Scan { table, .. } = graph.operation(scan) else {
                    return None;
                };
                Some(
                    super::guards::scan(catalog, *table)?
                        .into_iter()
                        .filter_map(move |guard| {
                            let column = graph.outputs(scan).find(|output| {
                                graph.name(*output) == Some(guard.column.as_str())
                            })?;
                            Some(path_filter(column, &ctx.paths_at_least(guard.min_role)))
                        }),
                )
            })
            .flatten()
            .collect();
        if !conditions.is_empty() {
            guards.insert(node, conditions);
        }
    }
    guards
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
        [] => Expr::Parameter {
            data_type: SqlType::Bool,
            value: Value::Bool(false),
        },
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
        let paths = paths.iter().map(|path| path.to_string()).collect();
        let ctx = SecurityContext::new(1, paths).unwrap();
        let plan = apply_security_context(plan, model(), &ctx).unwrap();
        model().render(&plan).sql
    }

    #[test]
    fn an_empty_security_context_fails_closed() {
        let mut ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        ctx.traversal_paths.clear();
        let plan = scans(&[("gl_project", "p")], None);
        let error = apply_security_context(plan, model(), &ctx);
        assert!(matches!(error, Err(QueryError::Security(_))));
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
