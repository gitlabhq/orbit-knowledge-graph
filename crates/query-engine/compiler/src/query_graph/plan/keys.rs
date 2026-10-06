use super::*;

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn candidate(
        &mut self,
        parent: BlockId,
        original: RelationId,
        key: &str,
        predicates: &[Expression<'catalog>],
        memberships: &[(&str, (DefinitionId, OutputId))],
        hint: &str,
    ) -> Result<(DefinitionId, OutputId)> {
        let Source::Stored(table) = self.relation(original)?.source else {
            return Err(GraphError::LatestShape);
        };
        let alias = self.relation(original)?.hint.clone();
        let body = self.select(PhysicalOperation::One);
        let relation = self.scan_stored(body, table, alias)?;
        if let Some(input) = self.relation(original)?.input.clone() {
            self.bind_scan(relation, input)?;
        }
        let mut operation = PhysicalOperation::source(relation);
        for predicate in predicates {
            operation = operation.filter(predicate.rebind(&|column| {
                if column.relation != original {
                    return Err(GraphError::OutsideBlock);
                }
                let Port::Stored(name) = column.port else {
                    return Err(GraphError::MissingOutput);
                };
                self.stored_port(relation, name)
            })?);
        }
        for (column, candidate) in memberships {
            operation = self.narrow(
                body,
                operation,
                self.stored_column(relation, column)?,
                *candidate,
            )?;
        }
        let output = self.project(
            body,
            "id",
            Expression::Column(self.stored_column(relation, key)?),
        )?;
        *self.operation_mut(body)? = operation;
        Ok((self.define(parent, body, hint, false)?, output))
    }

    pub(super) fn cascade_keys(
        &mut self,
        parent: BlockId,
        input: &crate::input::Input,
        scans: &[KeyScan<'catalog>],
        output_column: &str,
    ) -> Result<Option<ColumnRef<'catalog>>> {
        if scans.is_empty() {
            return Ok(None);
        }
        let index = scans.len() - 1;
        let selective = scans.iter().any(|scan| {
            let Some(ScanInput::Relationship(index)) = self
                .relation(scan.relation)
                .ok()
                .and_then(|relation| relation.input.clone())
            else {
                return false;
            };
            let relationship = &input.relationships[index];
            input
                .nodes
                .iter()
                .filter(|node| node.id == relationship.from || node.id == relationship.to)
                .any(|node| !node.node_ids.is_empty() || node.id_range.is_some())
        });
        if !selective && scans.iter().all(|scan| scan.memberships.is_empty()) {
            return Ok(None);
        }
        let (block, output) = self.edge_keys(
            input,
            scans,
            output_column,
            output_column,
            format!("e{index}p"),
        )?;
        let relation = self.derive(parent, block, format!("e{index}p"))?;
        Ok(Some(self.output_column(relation, output)?))
    }

    pub(super) fn edge_keys(
        &mut self,
        input: &crate::input::Input,
        scans: &[KeyScan<'catalog>],
        output_column: &str,
        output_label: &str,
        alias: String,
    ) -> Result<(BlockId, OutputId)> {
        let KeyScan {
            relation: original,
            predicates,
            memberships,
        } = scans.last().ok_or(GraphError::MissingOutput)?;
        let index = scans.len() - 1;
        let Some(ScanInput::Relationship(input_index)) = self.relation(*original)?.input else {
            return Err(GraphError::MissingOutput);
        };
        let relationship = &input.relationships[input_index];
        let Source::Stored(table) = self.relation(*original)?.source else {
            return Err(GraphError::MissingOutput);
        };
        let block = self.select(PhysicalOperation::One);
        let scan = self.scan_stored(block, table, alias)?;
        self.bind_scan(scan, ScanInput::Relationship(input_index))?;
        let mut operation = PhysicalOperation::source(scan);
        for predicate in predicates {
            operation = operation.filter(predicate.rebind(&|column| match column.port {
                Port::Stored(stored) if column.relation == *original => {
                    self.stored_port(scan, stored)
                }
                _ => Err(GraphError::OutsideBlock),
            })?);
        }
        for (column, candidate) in memberships {
            operation = self.narrow(
                block,
                operation,
                self.stored_column(scan, column)?,
                *candidate,
            )?;
        }
        if index > 0 {
            let Some(ScanInput::Relationship(previous_index)) =
                self.relation(scans[index - 1].relation)?.input
            else {
                return Err(GraphError::MissingOutput);
            };
            let previous = &input.relationships[previous_index];
            let (previous_start, previous_end) = previous.direction.edge_columns();
            let (start, end) = relationship.direction.edge_columns();
            let link = [
                (&previous.to, previous_end),
                (&previous.from, previous_start),
            ]
            .into_iter()
            .find_map(|(alias, column)| {
                [(&relationship.from, start), (&relationship.to, end)]
                    .into_iter()
                    .find(|(current, _)| current == &alias)
                    .map(|(_, current)| (column, current))
            });
            if let Some((previous_column, current_column)) = link
                && let Some(key) =
                    self.cascade_keys(block, input, &scans[..index], previous_column)?
            {
                operation = operation.membership(self.stored_column(scan, current_column)?, key);
            }
        }
        let output = self.project(
            block,
            output_label,
            Expression::Column(self.stored_column(scan, output_column)?),
        )?;
        *self.operation_mut(block)? = operation;
        Ok((block, output))
    }

    pub(super) fn narrow(
        &mut self,
        block: BlockId,
        input: PhysicalOperation<'catalog>,
        column: ColumnRef<'catalog>,
        (definition, output): (DefinitionId, OutputId),
    ) -> Result<PhysicalOperation<'catalog>> {
        let hint = self.definition_hint(definition)?.to_owned();
        let keys = self.reference(block, definition, hint)?;
        Ok(input.membership(column, self.output_column(keys, output)?))
    }
}
