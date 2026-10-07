use super::*;

impl<'catalog, M: QueryDataModel + ?Sized, E, O> QueryGraph<'catalog, M, E, O> {
    pub fn relation_columns(&self, relation: RelationId) -> Result<Vec<ColumnRef<'catalog>>> {
        let body = match self.relation(relation)?.source {
            Source::Stored(table) => {
                return table
                    .columns()
                    .map(|column| self.stored_port(relation, column))
                    .collect();
            }
            Source::Derived(body) => body,
            Source::Definition(definition) => self.definition(definition)?.body,
        };
        self.outputs(body)?
            .map(|output| self.output_column(relation, output))
            .collect()
    }
}

impl<'catalog, M: QueryDataModel + ?Sized, E, L>
    QueryGraph<'catalog, M, E, Relational<'catalog, L>>
{
    pub fn operation_outputs(
        &self,
        operation: &Relational<'catalog, L>,
    ) -> Result<Vec<ColumnRef<'catalog>>> {
        self.visit_operation_outputs(operation, &mut |_, _, _| Ok(()))
    }

    pub(super) fn visit_operation_outputs(
        &self,
        operation: &Relational<'catalog, L>,
        visit: &mut impl FnMut(
            &Relational<'catalog, L>,
            &[ColumnRef<'catalog>],
            &[ColumnRef<'catalog>],
        ) -> Result<()>,
    ) -> Result<Vec<ColumnRef<'catalog>>> {
        let mut inputs = operation.inputs();
        let mut left = inputs
            .next()
            .map(|input| self.visit_operation_outputs(input, visit))
            .transpose()?
            .unwrap_or_default();
        let right = inputs
            .next()
            .map(|input| self.visit_operation_outputs(input, visit))
            .transpose()?
            .unwrap_or_default();
        visit(operation, &left, &right)?;
        match operation {
            Relational::One => Ok(vec![]),
            Relational::Source { relation, .. } => self.relation_columns(*relation),
            Relational::Join {
                kind: JoinKind::Inner | JoinKind::Cross,
                ..
            } => {
                left.extend(right);
                Ok(left)
            }
            Relational::Aggregate { groups, .. } => Ok(groups
                .iter()
                .filter_map(|group| match group {
                    Expression::Column(column) => Some(*column),
                    _ => None,
                })
                .collect()),
            _ => Ok(left),
        }
    }
}
