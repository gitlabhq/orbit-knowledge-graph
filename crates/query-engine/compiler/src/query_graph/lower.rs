use super::*;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub fn lower_operations(
        self,
    ) -> Result<QueryGraph<'catalog, M, Expression<'catalog>, LoweredOperation<'catalog>>> {
        let operations = self
            .blocks
            .iter()
            .enumerate()
            .filter_map(|(slot, block)| {
                let Body::Select { operation, .. } = &block.body else {
                    return None;
                };
                Some(self.lower_operation(
                    BlockId {
                        owner: self.owner,
                        slot,
                    },
                    operation,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut operations = operations.into_iter();
        self.lower(Ok, |_| {
            Ok(operations.next().expect("one lowered operation per SELECT"))
        })
    }

    fn lower_operation(
        &self,
        block: BlockId,
        operation: &PhysicalOperation<'catalog>,
    ) -> Result<LoweredOperation<'catalog>> {
        use Relational::*;
        Ok(match operation {
            One => One,
            Source { relation, read } => Source {
                relation: *relation,
                read: *read,
            },
            Filter { input, predicate } => self
                .lower_operation(block, input)?
                .filter(predicate.clone()),
            Join {
                left,
                right,
                kind,
                condition,
            } => Join {
                left: Box::new(self.lower_operation(block, left)?),
                right: Box::new(self.lower_operation(block, right)?),
                kind: *kind,
                condition: condition.clone(),
            },
            Aggregate { input, groups } => {
                self.lower_operation(block, input)?.group_by(groups.clone())
            }
            Expand { input, column } => self.lower_operation(block, input)?.expand(*column),
            Materialize { input, relation } => {
                self.lower_operation(block, input)?.materialize(*relation)
            }
            Sort { input, keys } => self.lower_operation(block, input)?.sort(keys.clone()),
            FirstBy { input, keys } => FirstBy {
                input: Box::new(self.lower_operation(block, input)?),
                keys: keys.clone(),
            },
            Limit { input, count } => self.lower_operation(block, input)?.limit(*count),
            Latest {
                input,
                requirement: LatestRows { version, deletion },
            } => {
                let mut scan = input.as_ref();
                loop {
                    match scan {
                        Filter { input, .. } => scan = input,
                        Join {
                            left,
                            kind: JoinKind::Semi | JoinKind::Membership,
                            ..
                        } => scan = left,
                        _ => break,
                    }
                }
                if !matches!(scan, Source { relation, read: ReadMode::Raw } if *relation == version.relation)
                {
                    return Err(GraphError::LatestShape);
                }
                self.check_column(block, *version)?;
                let crate::query_graph::Source::Stored(table) =
                    self.relation(version.relation)?.source
                else {
                    return Err(GraphError::LatestShape);
                };
                let keys = self
                    .catalog
                    .table_sort_key(table.name())
                    .filter(|keys| !keys.is_empty())
                    .ok_or(GraphError::LatestShape)?
                    .iter()
                    .map(|name| self.stored_column(version.relation, name))
                    .collect::<Result<Vec<_>>>()?;
                let order = keys
                    .iter()
                    .map(|column| (*column, false))
                    .chain([(*version, true)])
                    .collect();
                let latest = FirstBy {
                    input: Box::new(self.lower_operation(block, input)?.sort(order)),
                    keys,
                };
                if let Some(deleted) = deletion {
                    if deleted.relation != version.relation {
                        return Err(GraphError::LatestShape);
                    }
                    latest.filter(Expression::equal(
                        Expression::Column(*deleted),
                        Expression::Boolean(false),
                    ))
                } else {
                    latest
                }
            }
        })
    }
}
