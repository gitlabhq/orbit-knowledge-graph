use super::*;
use crate::constants::{edge_kinds_column, path_column};
use crate::input::{Input, InputNode, InputPath};

#[derive(Clone, Copy)]
struct PathAnchor {
    definition: DefinitionId,
    identity: OutputId,
    scope: Option<OutputId>,
}

struct PathFrontier {
    definition: DefinitionId,
    outputs: FrontierValues<OutputId>,
}

#[derive(Clone, Copy)]
struct FrontierValues<T> {
    anchor: T,
    end: T,
    kind: T,
    nodes: T,
    edges: T,
    depth: T,
    scope: Option<T>,
}

impl<T> FrontierValues<T> {
    fn map<U>(self, mut convert: impl FnMut(T) -> Result<U>) -> Result<FrontierValues<U>> {
        Ok(FrontierValues {
            anchor: convert(self.anchor)?,
            end: convert(self.end)?,
            kind: convert(self.kind)?,
            nodes: convert(self.nodes)?,
            edges: convert(self.edges)?,
            depth: convert(self.depth)?,
            scope: self.scope.map(convert).transpose()?,
        })
    }
}

#[derive(Clone, Copy)]
struct PathEdge<'a> {
    anchor: ColumnRef<'a>,
    next: ColumnRef<'a>,
    anchor_kind: ColumnRef<'a>,
    next_kind: ColumnRef<'a>,
    relationship: ColumnRef<'a>,
    scope: ColumnRef<'a>,
    deleted: ColumnRef<'a>,
}

struct FrontierRequest<'a> {
    node: &'a InputNode,
    anchor: Option<PathAnchor>,
    depth: u32,
    backward: bool,
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn pathfinding(&mut self, input: &Input) -> Result<BlockId> {
        let path = input.path.as_ref().ok_or(GraphError::MissingOutput)?;
        let endpoint = |alias: &str| {
            input
                .nodes
                .iter()
                .find(|node| node.id == alias)
                .ok_or(GraphError::MissingOutput)
        };
        let start = endpoint(&path.from)?;
        let end = endpoint(&path.to)?;
        let scoped = [start, end].iter().all(|node| {
            node.entity
                .as_deref()
                .is_some_and(|entity| self.catalog.entity_has_traversal_path(entity))
        });
        let root = self.query();
        let start_anchor = self.path_anchor(root, start, scoped)?;
        let end_anchor = self.path_anchor(root, end, scoped)?;
        let scope = self.path_scope(root, start_anchor, end_anchor, scoped)?;
        let forward = self.path_frontier(
            root,
            path,
            FrontierRequest {
                node: start,
                anchor: start_anchor,
                depth: path.max_depth.div_ceil(2),
                backward: false,
            },
            scope,
            scoped,
        )?;
        let backward = if path.max_depth > 1 {
            Some(self.path_frontier(
                root,
                path,
                FrontierRequest {
                    node: end,
                    anchor: end_anchor,
                    depth: path.max_depth / 2,
                    backward: true,
                },
                scope,
                scoped,
            )?)
        } else {
            None
        };
        let direct = self.direct_paths(root, &forward, start, end, end_anchor)?;
        let body = if let Some(backward) = backward {
            let intersection =
                self.intersect_paths(root, &forward, &backward, start, end, path.max_depth)?;
            self.union_all(
                vec![direct, intersection],
                vec![
                    "depth".into(),
                    path_column().into(),
                    edge_kinds_column().into(),
                ],
            )?
        } else {
            direct
        };
        let [depth, nodes, edges] = self
            .outputs(body)?
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| GraphError::UnionShape)?;
        let relation = self.derive(root, body, "paths")?;
        let depth = self.output_column(relation, depth)?;
        let operation = self.sort_relation(
            self.read_relation(relation, ReadMode::Raw)?,
            vec![(depth, false)],
        )?;
        let projection = self.project_values(
            self.limit_relation(operation, input.limit)?,
            [
                (
                    path_column().into(),
                    Expression::Column(self.output_column(relation, nodes)?),
                ),
                (
                    edge_kinds_column().into(),
                    Expression::Column(self.output_column(relation, edges)?),
                ),
                ("depth".into(), Expression::Column(depth)),
            ],
        )?;
        self.finish_query(projection)
    }

    fn path_anchor(
        &mut self,
        root: BlockId,
        node: &InputNode,
        scoped: bool,
    ) -> Result<Option<PathAnchor>> {
        if (!scoped && !node.node_ids.is_empty())
            || (node.node_ids.is_empty() && node.filters.is_empty() && node.id_range.is_none())
        {
            return Ok(None);
        }
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let table = self
            .catalog
            .entity_table(entity)
            .ok_or(GraphError::MissingOutput)?;
        let inner = self.query_in(root)?;
        let scan = self.scan(inner, table, &node.id)?;
        let deleted = self.stored_column(scan, ontology::DELETED_COLUMN)?;
        let deletion = live_edge(deleted);
        let mut source = self.read_relation(scan, ReadMode::Current)?;
        for predicate in self.node_predicates(scan, node)? {
            if predicate != deletion {
                source = self.filter_relation(source, predicate)?;
            }
        }
        let mut projection = self.project_values(
            source,
            [
                (
                    "id".into(),
                    Expression::Column(self.stored_column(scan, "id")?),
                ),
                (ontology::DELETED_COLUMN.into(), Expression::Column(deleted)),
            ],
        )?;
        let scope = if self.catalog.entity_has_traversal_path(entity) {
            Some(self.append_projection(
                &mut projection,
                ontology::TRAVERSAL_PATH_COLUMN,
                Expression::Column(self.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?),
            )?)
        } else {
            None
        };
        let outputs = projection.outputs().map(|(id, _)| id).collect::<Vec<_>>();
        self.finish_query(projection)?;
        let outer = self.query_in(root)?;
        let relation = self.derive(outer, inner, &node.id)?;
        let operation = self.filter_relation(
            self.read_relation(relation, ReadMode::Raw)?,
            live_edge(self.output_column(relation, outputs[1])?),
        )?;
        let operation = self.limit_relation(
            operation,
            crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32,
        )?;
        let mut projection = self.project_values(
            operation,
            [(
                "id".into(),
                Expression::Column(self.output_column(relation, outputs[0])?),
            )],
        )?;
        let identity = projection
            .outputs()
            .next()
            .ok_or(GraphError::EmptyProjection)?
            .0;
        let scope = scope
            .map(|output| {
                self.append_projection(
                    &mut projection,
                    ontology::TRAVERSAL_PATH_COLUMN,
                    Expression::Column(self.output_column(relation, output)?),
                )
            })
            .transpose()?;
        self.finish_query(projection)?;
        let definition = self.define(root, outer, format!("_nf_{}", node.id))?;
        Ok(Some(PathAnchor {
            definition,
            identity,
            scope,
        }))
    }

    fn path_scope(
        &mut self,
        root: BlockId,
        start: Option<PathAnchor>,
        end: Option<PathAnchor>,
        scoped: bool,
    ) -> Result<Option<(DefinitionId, OutputId)>> {
        let (true, Some(start), Some(end)) = (scoped, start, end) else {
            return Ok(None);
        };
        let mut arms = Vec::new();
        for (anchor, hint) in [(start, "_path_scope_start"), (end, "_path_scope_end")] {
            let block = self.query_in(root)?;
            let relation = self.reference(block, anchor.definition, hint)?;
            let column =
                self.output_column(relation, anchor.scope.ok_or(GraphError::MissingOutput)?)?;
            let operation = self.aggregate_relation(
                self.read_relation(relation, ReadMode::Raw)?,
                vec![Expression::Column(column)],
            )?;
            let projection = self.project_values(
                operation,
                [(
                    ontology::TRAVERSAL_PATH_COLUMN.into(),
                    Expression::Column(column),
                )],
            )?;
            arms.push(self.finish_query(projection)?);
        }
        let body = self.union_all(arms, vec![ontology::TRAVERSAL_PATH_COLUMN.into()])?;
        let output = self
            .outputs(body)?
            .next()
            .ok_or(GraphError::EmptyProjection)?;
        Ok(Some((
            self.define(root, body, "_path_scope_traversal_paths")?,
            output,
        )))
    }

    fn path_frontier(
        &mut self,
        root: BlockId,
        path: &InputPath,
        request: FrontierRequest<'_>,
        scope: Option<(DefinitionId, OutputId)>,
        scoped: bool,
    ) -> Result<PathFrontier> {
        let entity = request
            .node
            .entity
            .as_deref()
            .ok_or(GraphError::MissingOutput)?;
        let first_kinds = if path.rel_types.is_any() {
            self.catalog.graph().relationship_names(
                (!request.backward).then_some(entity),
                request.backward.then_some(entity),
            )
        } else {
            path.rel_types.as_slice().to_vec()
        };
        let tables = self.catalog.relationship_tables(path.rel_types.as_slice());
        let mut arms = Vec::new();
        for depth in 1..=request.depth {
            let block = self.query_in(root)?;
            let (mut operation, first) =
                self.path_edge(block, &tables, "e1".into(), request.backward)?;
            if let Some(scope) = scope {
                operation = self.narrow(block, operation, first.scope, scope)?;
            }
            if let Some(anchor) = request.anchor {
                operation = self.narrow(
                    block,
                    operation,
                    first.anchor,
                    (anchor.definition, anchor.identity),
                )?;
            } else if !request.node.node_ids.is_empty() {
                operation = self.filter_relation(
                    operation,
                    Expression::membership(first.anchor, &request.node.node_ids),
                )?;
            }
            operation =
                self.filter_relation(operation, path_kind(first.relationship, &first_kinds))?;
            operation = self.filter_relation(
                operation,
                Expression::equal(
                    Expression::Column(first.anchor_kind),
                    Expression::Text(entity.into()),
                ),
            )?;
            operation = self.filter_relation(operation, live_edge(first.deleted))?;
            let mut edges = vec![first];
            let mut last = first;
            for step in 2..=depth {
                let (right, edge) =
                    self.path_edge(block, &tables, format!("e{step}"), request.backward)?;
                let mut condition = Expression::equal(
                    Expression::Column(last.next),
                    Expression::Column(edge.anchor),
                );
                if scoped {
                    condition = and(
                        condition,
                        Expression::equal(
                            Expression::Column(last.scope),
                            Expression::Column(edge.scope),
                        ),
                    );
                }
                if !path.rel_types.is_any() {
                    condition = and(
                        condition,
                        path_kind(edge.relationship, path.rel_types.as_slice()),
                    );
                }
                if let Some((definition, output)) = scope {
                    let reference = self.reference(
                        block,
                        definition,
                        self.definition_hint(definition)?.to_owned(),
                    )?;
                    condition = and(
                        condition,
                        Expression::InQuery {
                            value: Box::new(Expression::Column(edge.scope)),
                            key: self.output_column(reference, output)?,
                        },
                    );
                }
                operation = self.join_relations(
                    operation,
                    right,
                    JoinKind::Inner,
                    and(condition, live_edge(edge.deleted)),
                )?;
                edges.push(edge);
                last = edge;
            }
            let nodes = edges[..edges.len() - usize::from(request.backward)]
                .iter()
                .map(|edge| {
                    Expression::Tuple(vec![
                        Expression::Column(edge.next),
                        Expression::Column(edge.next_kind),
                    ])
                })
                .collect::<Vec<_>>();
            let nodes = if nodes.is_empty() {
                Expression::EmptyArray(ValueType::Tuple(vec![
                    ValueType::Scalar(SqlType::Int64),
                    ValueType::Scalar(SqlType::String),
                ]))
            } else {
                Expression::Array(nodes)
            };
            let mut outputs = vec![
                ("anchor_id".into(), Expression::Column(first.anchor)),
                ("end_id".into(), Expression::Column(last.next)),
                ("end_kind".into(), Expression::Column(last.next_kind)),
                ("path_nodes".into(), nodes),
                (
                    "edge_kinds".into(),
                    Expression::Array(
                        edges
                            .iter()
                            .map(|edge| Expression::Column(edge.relationship))
                            .collect(),
                    ),
                ),
                ("depth".into(), Expression::Integer(i64::from(depth))),
            ];
            if scoped {
                outputs.push((
                    ontology::TRAVERSAL_PATH_COLUMN.into(),
                    Expression::Column(first.scope),
                ));
            }
            let projection = self.project_values(operation, outputs)?;
            arms.push(self.finish_query(projection)?);
        }
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        let body = if arms.len() == 1 {
            first
        } else {
            let labels = self
                .outputs(first)?
                .map(|output| self.output_label(output).map(str::to_owned))
                .collect::<Result<_>>()?;
            self.union_all(arms, labels)?
        };
        let outputs = self.outputs(body)?.collect::<Vec<_>>();
        let outputs = FrontierValues {
            anchor: outputs[0],
            end: outputs[1],
            kind: outputs[2],
            nodes: outputs[3],
            edges: outputs[4],
            depth: outputs[5],
            scope: outputs.get(6).copied(),
        };
        let definition = self.define(
            root,
            body,
            if request.backward {
                "backward"
            } else {
                "forward"
            },
        )?;
        Ok(PathFrontier {
            definition,
            outputs,
        })
    }

    fn read_frontier(
        &mut self,
        block: BlockId,
        frontier: &PathFrontier,
        hint: &str,
    ) -> Result<(PhysicalOperation<'a>, FrontierValues<ColumnRef<'a>>)> {
        let relation = self.reference(block, frontier.definition, hint)?;
        let outputs = frontier
            .outputs
            .map(|output| self.output_column(relation, output))?;
        Ok((self.read_relation(relation, ReadMode::Raw)?, outputs))
    }

    fn direct_paths(
        &mut self,
        root: BlockId,
        forward: &PathFrontier,
        start: &InputNode,
        end: &InputNode,
        anchor: Option<PathAnchor>,
    ) -> Result<BlockId> {
        let block = self.query_in(root)?;
        let (operation, values) = self.read_frontier(block, forward, "f")?;
        let operation = self.filter_relation(
            operation,
            Expression::equal(Expression::Column(values.depth), Expression::Integer(1)),
        )?;
        let mut operation = self.filter_relation(
            operation,
            Expression::equal(
                Expression::Column(values.kind),
                Expression::Text(end.entity.clone().ok_or(GraphError::MissingOutput)?),
            ),
        )?;
        if !end.node_ids.is_empty() {
            operation =
                self.filter_relation(operation, Expression::membership(values.end, &end.node_ids))?;
        } else if let Some(anchor) = anchor {
            operation = self.narrow(
                block,
                operation,
                values.end,
                (anchor.definition, anchor.identity),
            )?;
        }
        let projection = self.project_values(
            operation,
            [
                ("depth".into(), Expression::Column(values.depth)),
                (
                    path_column().into(),
                    Expression::Concat(vec![
                        path_endpoint(values.anchor, start)?,
                        Expression::Column(values.nodes),
                    ]),
                ),
                (edge_kinds_column().into(), Expression::Column(values.edges)),
            ],
        )?;
        self.finish_query(projection)
    }

    fn intersect_paths(
        &mut self,
        root: BlockId,
        forward: &PathFrontier,
        backward: &PathFrontier,
        start: &InputNode,
        end: &InputNode,
        max_depth: u32,
    ) -> Result<BlockId> {
        let block = self.query_in(root)?;
        let (left, forward) = self.read_frontier(block, forward, "f")?;
        let (right, backward) = self.read_frontier(block, backward, "b")?;
        let mut condition = Expression::equal(
            Expression::Column(forward.end),
            Expression::Column(backward.end),
        );
        if let (Some(left), Some(right)) = (forward.scope, backward.scope) {
            condition = and(
                condition,
                Expression::equal(Expression::Column(left), Expression::Column(right)),
            );
        }
        let depth = Expression::Add(
            Box::new(Expression::Column(forward.depth)),
            Box::new(Expression::Column(backward.depth)),
        );
        let operation = self.join_relations(left, right, JoinKind::Inner, condition)?;
        let operation = self.filter_relation(
            operation,
            Expression::LessEqual(
                Box::new(depth.clone()),
                Box::new(Expression::Integer(i64::from(max_depth))),
            ),
        )?;
        let nodes = Expression::Concat(vec![
            path_endpoint(forward.anchor, start)?,
            Expression::Column(forward.nodes),
            Expression::Reverse(Box::new(Expression::Column(backward.nodes))),
            path_endpoint(backward.anchor, end)?,
        ]);
        let edges = Expression::Concat(vec![
            Expression::Column(forward.edges),
            Expression::Reverse(Box::new(Expression::Column(backward.edges))),
        ]);
        let projection = self.project_values(
            operation,
            [
                ("depth".into(), depth),
                (path_column().into(), nodes),
                (edge_kinds_column().into(), edges),
            ],
        )?;
        self.finish_query(projection)
    }

    fn path_edge(
        &mut self,
        block: BlockId,
        tables: &[String],
        hint: String,
        backward: bool,
    ) -> Result<(PhysicalOperation<'a>, PathEdge<'a>)> {
        let names = [
            "source_id",
            "target_id",
            "source_kind",
            "target_kind",
            "relationship_kind",
            ontology::TRAVERSAL_PATH_COLUMN,
            ontology::DELETED_COLUMN,
        ];
        let mut arms = Vec::new();
        let mut stored = None;
        for name in tables {
            let table = self
                .catalog
                .stored_table(name)
                .ok_or_else(|| GraphError::UnknownStored(name.clone()))?;
            if tables.len() == 1 {
                stored = Some(self.scan_stored(block, table, &hint)?);
                break;
            }
            let arm = self.query_in(block)?;
            let scan = self.scan_stored(arm, table, "_e")?;
            let values = names
                .iter()
                .map(|name| {
                    Ok((
                        (*name).into(),
                        Expression::Column(self.stored_column(scan, name)?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let projection =
                self.project_values(self.read_relation(scan, ReadMode::Raw)?, values)?;
            arms.push(self.finish_query(projection)?);
        }
        let (relation, columns) = if let Some(relation) = stored {
            let columns = names
                .iter()
                .map(|name| self.stored_column(relation, name))
                .collect::<Result<Vec<_>>>()?;
            (relation, columns)
        } else {
            let body = self.union_all(arms, names.iter().map(|name| (*name).into()).collect())?;
            let relation = self.derive(block, body, hint)?;
            let columns = self
                .outputs(body)?
                .map(|output| self.output_column(relation, output))
                .collect::<Result<Vec<_>>>()?;
            (relation, columns)
        };
        let [
            source,
            target,
            source_kind,
            target_kind,
            relationship,
            scope,
            deleted,
        ] = columns.try_into().map_err(|_| GraphError::UnionShape)?;
        let (anchor, next, anchor_kind, next_kind) = if backward {
            (target, source, target_kind, source_kind)
        } else {
            (source, target, source_kind, target_kind)
        };
        Ok((
            self.read_relation(relation, ReadMode::Raw)?,
            PathEdge {
                anchor,
                next,
                anchor_kind,
                next_kind,
                relationship,
                scope,
                deleted,
            },
        ))
    }
}

fn path_endpoint<'a>(identity: ColumnRef<'a>, node: &InputNode) -> Result<Expression<'a>> {
    Ok(Expression::Array(vec![Expression::Tuple(vec![
        Expression::Column(identity),
        Expression::Text(node.entity.clone().ok_or(GraphError::MissingOutput)?),
    ])]))
}

fn path_kind<'a>(column: ColumnRef<'a>, kinds: &[String]) -> Expression<'a> {
    if let [kind] = kinds {
        Expression::equal(Expression::Column(column), Expression::Text(kind.clone()))
    } else {
        Expression::In(
            Box::new(Expression::Column(column)),
            Box::new(Expression::Strings(kinds.to_vec())),
        )
    }
}

fn live_edge(column: ColumnRef<'_>) -> Expression<'_> {
    Expression::equal(Expression::Column(column), Expression::Boolean(false))
}
fn and<'a>(left: Expression<'a>, right: Expression<'a>) -> Expression<'a> {
    Expression::And(Box::new(left), Box::new(right))
}
