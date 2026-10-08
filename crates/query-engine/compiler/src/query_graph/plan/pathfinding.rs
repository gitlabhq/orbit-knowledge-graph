use super::super::api::*;
use super::predicates;
use crate::constants::{edge_kinds_column, path_column};
use crate::input::{Input, InputNode, InputPath};
use orbit_utils::query_types::SqlType;
use query_data_model::QueryDataModel;

struct Frontier<'input> {
    node: &'input InputNode,
    anchor: Option<Cte>,
    depth: u32,
    backward: bool,
    scoped: bool,
    scope: Option<Cte>,
}

pub(super) fn build<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    input: &Input,
) -> Result<Rows<'a>> {
    let path = input.path.as_ref().ok_or(Error::Outputs)?;
    let endpoint = |alias: &str| {
        input
            .nodes
            .iter()
            .find(|node| node.id == alias)
            .ok_or(Error::Outputs)
    };
    let start = endpoint(&path.from)?;
    let end = endpoint(&path.to)?;
    let scoped = [start, end].iter().all(|node| {
        node.entity
            .as_deref()
            .is_some_and(|entity| q.catalog().entity_has_traversal_path(entity))
    });
    let start_anchor = anchor(q, start, scoped)?;
    let end_anchor = anchor(q, end, scoped)?;
    let scope = if let (true, Some(start), Some(end)) = (scoped, start_anchor, end_anchor) {
        Some(q.cte("path_scopes", |q| {
            let mut arms = Vec::new();
            for anchor in [start, end] {
                arms.push(q.subquery(|q| {
                    let rows = q.read(anchor)?;
                    let path = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
                    q.aggregate(rows, [path.named(ontology::TRAVERSAL_PATH_COLUMN)], [])
                })?);
            }
            q.union_all(arms)
        })?)
    } else {
        None
    };
    let forward = q.cte("forward", |q| {
        frontier(
            q,
            path,
            Frontier {
                node: start,
                anchor: start_anchor,
                depth: path.max_depth.div_ceil(2),
                backward: false,
                scoped,
                scope,
            },
        )
    })?;
    let backward = if path.max_depth > 1 {
        Some(q.cte("backward", |q| {
            frontier(
                q,
                path,
                Frontier {
                    node: end,
                    anchor: end_anchor,
                    depth: path.max_depth / 2,
                    backward: true,
                    scoped,
                    scope,
                },
            )
        })?)
    } else {
        None
    };
    let direct = q.subquery(|q| {
        let mut rows = q.read(forward)?;
        let depth = rows.column("depth")?;
        let identity = rows.column("end_id")?;
        let kind = rows.column("end_kind")?;
        let nodes = array_concat([
            endpoint_value(rows.column("anchor_id")?, start)?,
            rows.column("path_nodes")?.expr(),
        ]);
        let edges = rows.column("edge_kinds")?;
        rows = q.filter(
            rows,
            depth
                .eq(1)
                .and(kind.eq(end.entity.as_deref().ok_or(Error::Outputs)?)),
        )?;
        if !end.node_ids.is_empty() {
            rows = q.filter(rows, identity.in_values(end.node_ids.iter().copied()))?;
        } else if let Some(anchor) = end_anchor {
            rows = membership(q, rows, identity, anchor, "id")?;
        }
        q.select(
            rows,
            [
                depth.named("depth"),
                nodes.named(path_column()),
                edges.named(edge_kinds_column()),
            ],
        )
    })?;
    let rows = if let Some(backward) = backward {
        let intersection = q.subquery(|q| {
            let left = q.read(forward)?;
            let right = q.read(backward)?;
            let mut condition = left.column("end_id")?.eq(right.column("end_id")?);
            if scoped {
                condition = condition.and(
                    left.column(ontology::TRAVERSAL_PATH_COLUMN)?
                        .eq(right.column(ontology::TRAVERSAL_PATH_COLUMN)?),
                );
            }
            let depth = left.column("depth")?.add(right.column("depth")?);
            let nodes = array_concat([
                endpoint_value(left.column("anchor_id")?, start)?,
                left.column("path_nodes")?.expr(),
                Expr::call(Function::ArrayReverse, [right.column("path_nodes")?.expr()]),
                endpoint_value(right.column("anchor_id")?, end)?,
            ]);
            let edges = array_concat([
                left.column("edge_kinds")?.expr(),
                Expr::call(Function::ArrayReverse, [right.column("edge_kinds")?.expr()]),
            ]);
            let rows = q.join(left, right, condition)?;
            let rows = q.filter(rows, depth.clone().le(i64::from(path.max_depth)))?;
            q.select(
                rows,
                [
                    depth.named("depth"),
                    nodes.named(path_column()),
                    edges.named(edge_kinds_column()),
                ],
            )
        })?;
        q.union_all([direct, intersection])?
    } else {
        q.from(direct)?
    };
    let depth = rows.column("depth")?;
    let nodes = rows.column(path_column())?;
    let edges = rows.column(edge_kinds_column())?;
    let rows = q.sort(rows, [depth.asc()])?;
    let rows = q.limit(rows, input.limit)?;
    q.select(
        rows,
        [
            nodes.named(path_column()),
            edges.named(edge_kinds_column()),
            depth.named("depth"),
        ],
    )
}

fn anchor<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    node: &InputNode,
    scoped: bool,
) -> Result<Option<Cte>> {
    if (!scoped && !node.node_ids.is_empty())
        || (node.node_ids.is_empty() && node.filters.is_empty() && node.id_range.is_none())
    {
        return Ok(None);
    }
    q.cte(&format!("anchor_{}", node.id), |q| {
        let entity = node.entity.as_deref().ok_or(Error::Outputs)?;
        let table = q.catalog().entity_table(entity).ok_or(Error::Outputs)?;
        let mut rows = q.scan(table, Read::Current)?.labeled(&node.id)?;
        let mut outputs = vec![rows.column("id")?.named("id")];
        if q.catalog().entity_has_traversal_path(entity) {
            outputs.push(
                rows.column(ontology::TRAVERSAL_PATH_COLUMN)?
                    .named(ontology::TRAVERSAL_PATH_COLUMN),
            );
        }
        for predicate in predicates::node(q.catalog(), &rows, node)? {
            rows = q.filter(rows, predicate)?;
        }
        let rows = q.limit(rows, crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32)?;
        q.select(rows, outputs)
    })
    .map(Some)
}

fn frontier<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    path: &InputPath,
    request: Frontier<'_>,
) -> Result<Rows<'a>> {
    let entity = request.node.entity.as_deref().ok_or(Error::Outputs)?;
    let first_kinds = if path.rel_types.is_any() {
        q.catalog().graph().relationship_names(
            (!request.backward).then_some(entity),
            request.backward.then_some(entity),
        )
    } else {
        path.rel_types.as_slice().to_vec()
    };
    let tables = q.catalog().relationship_tables(path.rel_types.as_slice());
    let mut arms = Vec::new();
    for depth in 1..=request.depth {
        arms.push(q.subquery(|q| {
            let mut rows = edge(q, &tables)?;
            let (start, end, start_kind, end_kind) = if request.backward {
                ("target_id", "source_id", "target_kind", "source_kind")
            } else {
                ("source_id", "target_id", "source_kind", "target_kind")
            };
            let anchor = rows.column(start)?;
            let scope = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
            let mut last = rows.column(end)?;
            let mut last_kind = rows.column(end_kind)?;
            let mut nodes = vec![tuple([last.expr(), last_kind.expr()])];
            let mut relationships = vec![rows.column("relationship_kind")?.expr()];
            let condition = kinds(&rows.column("relationship_kind")?, &first_kinds)
                .and(rows.column(start_kind)?.eq(entity))
                .and(rows.column(ontology::DELETED_COLUMN)?.eq(false));
            if let Some(definition) = request.scope {
                rows = membership(
                    q,
                    rows,
                    scope.clone(),
                    definition,
                    ontology::TRAVERSAL_PATH_COLUMN,
                )?;
            }
            if let Some(anchor) = request.anchor {
                let identity = rows.column(start)?;
                rows = membership(q, rows, identity, anchor, "id")?;
            } else if !request.node.node_ids.is_empty() {
                rows = q.filter(
                    rows,
                    anchor.in_values(request.node.node_ids.iter().copied()),
                )?;
            }
            rows = q.filter(rows, condition)?;
            for _ in 2..=depth {
                let mut right = edge(q, &tables)?;
                let next_scope = right.column(ontology::TRAVERSAL_PATH_COLUMN)?;
                let mut condition = last
                    .eq(right.column(start)?)
                    .and(right.column(ontology::DELETED_COLUMN)?.eq(false));
                if request.scoped {
                    condition = condition.and(scope.eq(&next_scope));
                }
                if !path.rel_types.is_any() {
                    condition = condition.and(kinds(
                        &right.column("relationship_kind")?,
                        path.rel_types.as_slice(),
                    ));
                }
                last = right.column(end)?;
                last_kind = right.column(end_kind)?;
                nodes.push(tuple([last.expr(), last_kind.expr()]));
                relationships.push(right.column("relationship_kind")?.expr());
                if let Some(definition) = request.scope {
                    right = membership(
                        q,
                        right,
                        next_scope,
                        definition,
                        ontology::TRAVERSAL_PATH_COLUMN,
                    )?;
                }
                rows = q.join(rows, right, condition)?;
            }
            if request.backward {
                nodes.pop();
            }
            let nodes = if nodes.is_empty() {
                Expr::call(
                    Function::EmptyArray(ValueType::Tuple(vec![
                        ValueType::Scalar(SqlType::Int64),
                        ValueType::Scalar(SqlType::String),
                    ])),
                    [],
                )
            } else {
                array(nodes)
            };
            let mut outputs = vec![
                anchor.named("anchor_id"),
                last.named("end_id"),
                last_kind.named("end_kind"),
                nodes.named("path_nodes"),
                array(relationships).named("edge_kinds"),
                lit(i64::from(depth)).named("depth"),
            ];
            if request.scoped {
                outputs.push(scope.named(ontology::TRAVERSAL_PATH_COLUMN));
            }
            q.select(rows, outputs)
        })?);
    }
    if arms.len() == 1 {
        q.from(arms[0])
    } else {
        q.union_all(arms)
    }
}

fn edge<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    tables: &[String],
) -> Result<Rows<'a>> {
    if let [table] = tables {
        q.scan(table, Read::Raw)
    } else {
        let mut arms = Vec::new();
        for table in tables {
            arms.push(q.subquery(|q| {
                let rows = q.scan(table, Read::Raw)?;
                let outputs = [
                    "source_id",
                    "target_id",
                    "source_kind",
                    "target_kind",
                    "relationship_kind",
                    ontology::TRAVERSAL_PATH_COLUMN,
                    ontology::DELETED_COLUMN,
                ]
                .into_iter()
                .map(|name| rows.column(name).map(|column| column.named(name)))
                .collect::<Result<Vec<_>>>()?;
                q.select(rows, outputs)
            })?);
        }
        q.union_all(arms)
    }
}

fn membership<'a, M: QueryDataModel + ?Sized>(
    q: &mut QueryScope<'_, 'a, M>,
    rows: Rows<'a>,
    value: Column,
    definition: Cte,
    name: &str,
) -> Result<Rows<'a>> {
    let keys = q.read(definition)?;
    let key = keys.column(name)?;
    q.filter_in(rows, value, keys, key)
}

fn endpoint_value(identity: Column, node: &InputNode) -> Result<Expr> {
    Ok(array([tuple([
        identity.expr(),
        lit(node.entity.as_deref().ok_or(Error::Outputs)?),
    ])]))
}

fn kinds(column: &Column, kinds: &[String]) -> Expr {
    if let [kind] = kinds {
        column.eq(kind.as_str())
    } else {
        column.in_values(kinds.iter().cloned())
    }
}
