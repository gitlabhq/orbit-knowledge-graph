use super::*;
use crate::constants::{edge_kinds_column, path_column};
use crate::input::{Input, InputNode};

struct Frontier<'a> {
    anchor: Option<(DefinitionId, OutputId)>,
    node: &'a InputNode,
    depth: u32,
    backward: bool,
}

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn pathfinding(&mut self, input: &Input) -> Result<BlockId> {
        let path = input.path.as_ref().ok_or(GraphError::MissingOutput)?;
        let start = input
            .nodes
            .iter()
            .find(|node| node.id == path.from)
            .ok_or(GraphError::MissingOutput)?;
        let end = input
            .nodes
            .iter()
            .find(|node| node.id == path.to)
            .ok_or(GraphError::MissingOutput)?;
        let scoped = [start, end].iter().all(|node| {
            node.entity
                .as_deref()
                .is_some_and(|entity| self.catalog.entity_has_traversal_path(entity))
        });
        let root = self.select(PhysicalOperation::One);
        let start_anchor = self.path_anchor(root, start, scoped)?;
        let end_anchor = self.path_anchor(root, end, scoped)?;
        let scope = if scoped
            && let (Some((start_definition, _)), Some((end_definition, _))) =
                (start_anchor, end_anchor)
        {
            let mut arms = Vec::new();
            for (definition, alias) in [
                (start_definition, "_path_scope_start"),
                (end_definition, "_path_scope_end"),
            ] {
                let block = self.select(PhysicalOperation::One);
                let relation = self.reference(block, definition, alias)?;
                let column = self.column(relation, ontology::TRAVERSAL_PATH_COLUMN)?;
                self.project(
                    block,
                    ontology::TRAVERSAL_PATH_COLUMN,
                    Expression::Column(column),
                )?;
                *self.operation_mut(block)? =
                    PhysicalOperation::source(relation).aggregate(vec![column]);
                arms.push(block);
            }
            let body = self.union_all(arms, vec![ontology::TRAVERSAL_PATH_COLUMN.into()])?;
            let output = self
                .outputs(body)?
                .next()
                .ok_or(GraphError::MissingOutput)?;
            Some((
                self.define(root, body, "_path_scope_traversal_paths", false)?,
                output,
            ))
        } else {
            None
        };
        let forward = self.path_frontier(
            root,
            input,
            Frontier {
                anchor: start_anchor,
                node: start,
                depth: path.max_depth.div_ceil(2),
                backward: false,
            },
            scope,
            scoped,
        )?;
        let backward = if path.max_depth > 1 {
            Some(self.path_frontier(
                root,
                input,
                Frontier {
                    anchor: end_anchor,
                    node: end,
                    depth: path.max_depth / 2,
                    backward: true,
                },
                scope,
                scoped,
            )?)
        } else {
            None
        };
        let start_entity = start.entity.as_ref().ok_or(GraphError::MissingOutput)?;
        let end_entity = end.entity.as_ref().ok_or(GraphError::MissingOutput)?;
        let direct = self.select(PhysicalOperation::One);
        let f = self.reference(direct, forward, "f")?;
        let mut operation = PhysicalOperation::source(f)
            .filter(Expression::equal(
                Expression::Column(self.column(f, "depth")?),
                Expression::Integer(1),
            ))
            .filter(Expression::equal(
                Expression::Column(self.column(f, "end_kind")?),
                Expression::Text(end_entity.clone()),
            ));
        if !end.node_ids.is_empty() {
            operation = operation.filter(Expression::membership(
                self.column(f, "end_id")?,
                &end.node_ids,
            ));
        } else if let Some(anchor) = end_anchor {
            operation = self.narrow(direct, operation, self.column(f, "end_id")?, anchor)?;
        }
        let start_tuple = Expression::Array(vec![Expression::Tuple(vec![
            Expression::Column(self.column(f, "anchor_id")?),
            Expression::Text(start_entity.clone()),
        ])]);
        self.project(
            direct,
            "depth",
            Expression::Column(self.column(f, "depth")?),
        )?;
        self.project(
            direct,
            path_column(),
            Expression::Concat(vec![
                start_tuple,
                Expression::Column(self.column(f, "path_nodes")?),
            ]),
        )?;
        self.project(
            direct,
            edge_kinds_column(),
            Expression::Column(self.column(f, "edge_kinds")?),
        )?;
        *self.operation_mut(direct)? = operation;
        let body = if let Some(backward) = backward {
            let intersection = self.select(PhysicalOperation::One);
            let f = self.reference(intersection, forward, "f")?;
            let b = self.reference(intersection, backward, "b")?;
            let mut condition = Expression::equal(
                Expression::Column(self.column(f, "end_id")?),
                Expression::Column(self.column(b, "end_id")?),
            );
            if scoped {
                condition = Expression::And(
                    Box::new(condition),
                    Box::new(Expression::equal(
                        Expression::Column(self.column(f, "traversal_path")?),
                        Expression::Column(self.column(b, "traversal_path")?),
                    )),
                );
            }
            let depth = Expression::Add(
                Box::new(Expression::Column(self.column(f, "depth")?)),
                Box::new(Expression::Column(self.column(b, "depth")?)),
            );
            *self.operation_mut(intersection)? = PhysicalOperation::source(f)
                .join(PhysicalOperation::source(b), condition)
                .filter(Expression::LessEqual(
                    Box::new(depth.clone()),
                    Box::new(Expression::Integer(i64::from(path.max_depth))),
                ));
            let endpoint = |relation, entity: &String| -> Result<_> {
                Ok(Expression::Array(vec![Expression::Tuple(vec![
                    Expression::Column(self.column(relation, "anchor_id")?),
                    Expression::Text(entity.clone()),
                ])]))
            };
            let nodes = Expression::Concat(vec![
                endpoint(f, start_entity)?,
                Expression::Column(self.column(f, "path_nodes")?),
                Expression::Reverse(Box::new(Expression::Column(self.column(b, "path_nodes")?))),
                endpoint(b, end_entity)?,
            ]);
            let kinds = Expression::Concat(vec![
                Expression::Column(self.column(f, "edge_kinds")?),
                Expression::Reverse(Box::new(Expression::Column(self.column(b, "edge_kinds")?))),
            ]);
            self.project(intersection, "depth", depth)?;
            self.project(intersection, path_column(), nodes)?;
            self.project(intersection, edge_kinds_column(), kinds)?;
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
        let paths = self.derive(root, body, "paths")?;
        for name in [path_column(), edge_kinds_column(), "depth"] {
            self.project(root, name, Expression::Column(self.column(paths, name)?))?;
        }
        *self.operation_mut(root)? = PhysicalOperation::source(paths)
            .sort(vec![(self.column(paths, "depth")?, false)])
            .limit(input.limit);
        Ok(root)
    }

    fn path_anchor(
        &mut self,
        root: BlockId,
        node: &InputNode,
        scoped: bool,
    ) -> Result<Option<(DefinitionId, OutputId)>> {
        if !scoped && !node.node_ids.is_empty() {
            return Ok(None);
        }
        if node.node_ids.is_empty() && node.filters.is_empty() && node.id_range.is_none() {
            return Ok(None);
        }
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let inner = self.select(PhysicalOperation::One);
        let scan = self.scan(
            inner,
            self.catalog
                .entity_table(entity)
                .ok_or(GraphError::MissingOutput)?,
            &node.id,
        )?;
        let operation = self.node_source(scan, node)?;
        let PhysicalOperation::Filter { input: source, .. } = operation else {
            return Err(GraphError::JoinShape);
        };
        *self.operation_mut(inner)? = *source;
        let mut names = vec!["id", ontology::DELETED_COLUMN];
        if self.catalog.entity_has_traversal_path(entity) {
            names.push(ontology::TRAVERSAL_PATH_COLUMN);
        }
        for name in &names {
            self.project(
                inner,
                *name,
                Expression::Column(self.stored_column(scan, name)?),
            )?;
        }
        let outer = self.select(PhysicalOperation::One);
        let relation = self.derive(outer, inner, &node.id)?;
        *self.operation_mut(outer)? = PhysicalOperation::source(relation)
            .filter(Expression::equal(
                Expression::Column(self.column(relation, ontology::DELETED_COLUMN)?),
                Expression::Boolean(false),
            ))
            .limit(crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32);
        let id = self.project(
            outer,
            "id",
            Expression::Column(self.column(relation, "id")?),
        )?;
        if names.contains(&ontology::TRAVERSAL_PATH_COLUMN) {
            self.project(
                outer,
                ontology::TRAVERSAL_PATH_COLUMN,
                Expression::Column(self.column(relation, ontology::TRAVERSAL_PATH_COLUMN)?),
            )?;
        }
        Ok(Some((
            self.define(root, outer, format!("_nf_{}", node.id), false)?,
            id,
        )))
    }

    fn path_frontier(
        &mut self,
        root: BlockId,
        input: &Input,
        frontier: Frontier<'_>,
        scope: Option<(DefinitionId, OutputId)>,
        scoped: bool,
    ) -> Result<DefinitionId> {
        let path = input.path.as_ref().ok_or(GraphError::MissingOutput)?;
        let entity = frontier
            .node
            .entity
            .as_deref()
            .ok_or(GraphError::MissingOutput)?;
        let wildcard = crate::passes::normalize::is_wildcard(&path.rel_types);
        let first_kinds = if wildcard {
            self.catalog.graph().relationship_names(
                (!frontier.backward).then_some(entity),
                frontier.backward.then_some(entity),
            )
        } else {
            path.rel_types.clone()
        };
        let tables = self.catalog.relationship_tables(&path.rel_types);
        let (anchor, next, anchor_kind, next_kind) = if frontier.backward {
            ("target_id", "source_id", "target_kind", "source_kind")
        } else {
            ("source_id", "target_id", "source_kind", "target_kind")
        };
        let mut arms = Vec::new();
        for depth in 1..=frontier.depth {
            let block = self.select(PhysicalOperation::One);
            let mut scans = Vec::new();
            let mut operation = PhysicalOperation::One;
            for step in 1..=depth {
                let scan = self.path_edge(block, &tables, format!("e{step}"))?;
                let mut source = PhysicalOperation::source(scan);
                if step == 1
                    && let Some(scope) = scope
                {
                    source =
                        self.narrow(block, source, self.column(scan, "traversal_path")?, scope)?;
                }
                let kinds = if step == 1 {
                    &first_kinds[..]
                } else if wildcard {
                    &[]
                } else {
                    &path.rel_types[..]
                };
                let kind_filter = if kinds.is_empty() {
                    None
                } else {
                    let column = Expression::Column(self.column(scan, "relationship_kind")?);
                    Some(if let [kind] = kinds {
                        Expression::equal(column, Expression::Text(kind.clone()))
                    } else {
                        Expression::In(
                            Box::new(column),
                            Box::new(Expression::Strings(kinds.to_vec())),
                        )
                    })
                };
                let live = Expression::equal(
                    Expression::Column(self.column(scan, "_deleted")?),
                    Expression::Boolean(false),
                );
                if let Some(previous) = scans.last() {
                    let mut condition = Expression::equal(
                        Expression::Column(self.column(*previous, next)?),
                        Expression::Column(self.column(scan, anchor)?),
                    );
                    if scoped {
                        condition = Expression::And(
                            Box::new(condition),
                            Box::new(Expression::equal(
                                Expression::Column(self.column(*previous, "traversal_path")?),
                                Expression::Column(self.column(scan, "traversal_path")?),
                            )),
                        );
                    }
                    if let Some(kind_filter) = kind_filter {
                        condition = Expression::And(Box::new(condition), Box::new(kind_filter));
                    }
                    if let Some((definition, output)) = scope {
                        let reference = self.reference(
                            block,
                            definition,
                            self.definition_hint(definition)?.to_owned(),
                        )?;
                        condition = Expression::And(
                            Box::new(condition),
                            Box::new(Expression::InQuery {
                                value: Box::new(Expression::Column(
                                    self.column(scan, "traversal_path")?,
                                )),
                                key: self.output_column(reference, output)?,
                            }),
                        );
                    }
                    condition = Expression::And(Box::new(condition), Box::new(live));
                    operation = operation.join(source, condition);
                } else {
                    if let Some(anchor_key) = frontier.anchor {
                        source =
                            self.narrow(block, source, self.column(scan, anchor)?, anchor_key)?;
                    } else if !frontier.node.node_ids.is_empty() {
                        source = source.filter(Expression::membership(
                            self.column(scan, anchor)?,
                            &frontier.node.node_ids,
                        ));
                    }
                    if let Some(kind_filter) = kind_filter {
                        source = source.filter(kind_filter);
                    }
                    operation = source
                        .filter(Expression::equal(
                            Expression::Column(self.column(scan, anchor_kind)?),
                            Expression::Text(entity.into()),
                        ))
                        .filter(live);
                }
                scans.push(scan);
            }
            let first = scans[0];
            let last = *scans.last().ok_or(GraphError::MissingOutput)?;
            for (name, scan, column) in [
                ("anchor_id", first, anchor),
                ("end_id", last, next),
                ("end_kind", last, next_kind),
            ] {
                self.project(block, name, Expression::Column(self.column(scan, column)?))?;
            }
            let count = scans.len() - usize::from(frontier.backward);
            let nodes = scans[..count]
                .iter()
                .map(|scan| {
                    Ok(Expression::Tuple(vec![
                        Expression::Column(self.column(*scan, next)?),
                        Expression::Column(self.column(*scan, next_kind)?),
                    ]))
                })
                .collect::<Result<Vec<_>>>()?;
            self.project(
                block,
                "path_nodes",
                if nodes.is_empty() {
                    Expression::EmptyArray(ValueType::Tuple(vec![
                        ValueType::Scalar(SqlType::Int64),
                        ValueType::Scalar(SqlType::String),
                    ]))
                } else {
                    Expression::Array(nodes)
                },
            )?;
            let kinds = scans
                .iter()
                .map(|scan| {
                    self.column(*scan, "relationship_kind")
                        .map(Expression::Column)
                })
                .collect::<Result<_>>()?;
            self.project(block, "edge_kinds", Expression::Array(kinds))?;
            self.project(block, "depth", Expression::Integer(i64::from(depth)))?;
            if scoped {
                self.project(
                    block,
                    "traversal_path",
                    Expression::Column(self.column(first, "traversal_path")?),
                )?;
            }
            *self.operation_mut(block)? = operation;
            arms.push(block);
        }
        let first = *arms.first().ok_or(GraphError::EmptyProjection)?;
        let labels = self
            .outputs(first)?
            .map(|output| self.output_label(output).map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        let body = if arms.len() == 1 {
            first
        } else {
            self.union_all(arms, labels)?
        };
        self.define(
            root,
            body,
            if frontier.backward {
                "backward"
            } else {
                "forward"
            },
            false,
        )
    }

    fn path_edge(
        &mut self,
        block: BlockId,
        tables: &[String],
        alias: String,
    ) -> Result<RelationId> {
        let mut arms = Vec::new();
        let columns = [
            "source_id",
            "target_id",
            "source_kind",
            "target_kind",
            "relationship_kind",
            "traversal_path",
            "_deleted",
        ];
        for table in tables {
            let table = self
                .catalog
                .graph()
                .relationships()
                .filter_map(|relationship| self.catalog.relationship_table(&relationship.name))
                .find(|name| *name == table)
                .ok_or(GraphError::MissingOutput)?;
            if tables.len() == 1 {
                return self.scan(block, table, alias);
            }
            let arm = self.select(PhysicalOperation::One);
            let scan = self.scan(arm, table, "_e")?;
            for column in columns {
                self.project(
                    arm,
                    column,
                    Expression::Column(self.stored_column(scan, column)?),
                )?;
            }
            *self.operation_mut(arm)? = PhysicalOperation::source(scan);
            arms.push(arm);
        }
        let union = self.union_all(arms, columns.into_iter().map(str::to_owned).collect())?;
        self.derive(block, union, alias)
    }
}
