use super::*;

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub fn relation_columns(&self, relation: RelationId) -> Result<Vec<ColumnRef<'a>>> {
        if let Source::Stored(table) = self.relation(relation)?.source {
            return table
                .columns()
                .map(|column| self.stored_port(relation, column))
                .collect();
        }
        let body = self
            .relation_body(relation)?
            .ok_or(GraphError::MissingOutput)?;
        self.outputs(body)?
            .map(|output| self.output_column(relation, output))
            .collect()
    }

    pub fn rewrite_query(
        mut self,
        block: BlockId,
        rewrite: impl FnOnce(&mut Self, QueryOperation<'a, L>) -> Result<QueryOperation<'a, L>>,
    ) -> Result<Self> {
        let original = self
            .block_mut(block)?
            .operation
            .take()
            .ok_or(GraphError::EmptyProjection)?;
        let input = original.input()?;
        let scalar = input.aggregate_input().is_some() && input.groups().is_empty();
        let contract = original
            .outputs
            .iter()
            .map(|output| (output.label.clone(), output.data_type.clone()))
            .collect::<Vec<_>>();
        let dependencies = self.block(block)?.required.clone();
        let replacement = rewrite(&mut self, original)?;
        let input = replacement.input()?;
        if replacement.block != block
            || scalar != (input.aggregate_input().is_some() && input.groups().is_empty())
            || replacement.outputs.len() != contract.len()
            || replacement
                .outputs
                .iter()
                .zip(contract)
                .any(|(output, (label, data_type))| {
                    output.label != label || output.data_type != data_type
                })
        {
            return Err(GraphError::OutputContract);
        }
        if !self.block(block)?.required.is_subset(&dependencies) {
            return Err(GraphError::DefinitionVisibility);
        }
        if self.block(block)?.operation.is_some() {
            return Err(GraphError::FinishedQuery);
        }
        self.block_mut(block)?.operation = Some(replacement);
        Ok(self)
    }

    pub fn extend_result(
        &mut self,
        block: BlockId,
        values: impl IntoIterator<Item = (String, Expression<'a>)>,
    ) -> Result<Vec<OutputId>> {
        if self.block(block)?.owner.is_some() {
            return Err(GraphError::OutputContract);
        }
        let input = self.operation(block)?;
        let outputs = values
            .into_iter()
            .map(|(label, value)| {
                let data_type = self.require_value(input, &value, false)?;
                Ok(Output {
                    label,
                    data_type,
                    value: Some(value),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let query = self
            .block_mut(block)?
            .operation
            .as_mut()
            .ok_or(GraphError::EmptyProjection)?;
        let first = query.outputs.len();
        let added = outputs.len();
        query.outputs.extend(outputs);
        Ok((first..first + added)
            .map(|slot| OutputId { block, slot })
            .collect())
    }

    pub fn rewrite_output(&mut self, output: OutputId, value: Expression<'a>) -> Result<()> {
        let data_type = self.require_value(self.operation(output.block)?, &value, false)?;
        if self.output(output)?.data_type != data_type {
            return Err(GraphError::OutputContract);
        }
        let query = self
            .block_mut(output.block)?
            .operation
            .as_mut()
            .ok_or(GraphError::EmptyProjection)?;
        query.outputs[output.slot].value = Some(value);
        Ok(())
    }

    pub fn restrict_scan(&mut self, relation: RelationId, predicate: Expression<'a>) -> Result<()> {
        let mut read = None;
        self.operation(relation.block)?
            .walk(None, &mut |operation, _| {
                if let OperationKind::Source {
                    relation: source,
                    read: mode,
                } = operation.kind()
                    && *source == relation
                {
                    read = Some(*mode);
                }
                Ok::<_, GraphError>(())
            })?;
        let source = self.read_relation(relation, read.ok_or(GraphError::OperationVisibility)?)?;
        let filtered = self.filter_relation(source, predicate)?;
        let query = self
            .block_mut(relation.block)?
            .operation
            .as_mut()
            .ok_or(GraphError::EmptyProjection)?;
        let QueryKind::Project(operation) = &mut query.kind else {
            return Err(GraphError::ExpectedSelect);
        };
        install_scan_filter(operation, relation, &mut Some(filtered));
        Ok(())
    }

    pub fn scalar_query(
        &mut self,
        parent: BlockId,
        output: OutputId,
        hint: impl Into<String>,
    ) -> Result<Expression<'a>> {
        self.output(output)?;
        let input = self.operation(output.block)?;
        if input.aggregate_input().is_none() || !input.groups().is_empty() {
            return Err(GraphError::AggregatePlacement);
        }
        self.require_attachment(parent, output.block)?;
        let declaration = self.block(parent)?;
        if declaration.operation.is_some()
            && self.block(output.block)?.required.iter().any(|definition| {
                definition.block != parent && !declaration.required.contains(definition)
            })
        {
            return Err(GraphError::DefinitionVisibility);
        }
        self.attach(parent, output.block)?;
        let relations = &mut self.block_mut(parent)?.relations;
        let relation = RelationId {
            block: parent,
            slot: relations.len(),
        };
        relations.push(Relation {
            hint: hint.into(),
            source: Source::Derived(output.block),
            input: None,
        });
        Ok(Expression::ScalarQuery(ColumnRef {
            relation,
            port: Port::Output(output),
        }))
    }
}

fn install_scan_filter<'a, L>(
    operation: &mut Relational<'a, L>,
    relation: RelationId,
    replacement: &mut Option<Relational<'a, L>>,
) {
    if matches!(operation.kind(), OperationKind::Source { relation: source, .. } if *source == relation)
    {
        *operation = replacement
            .take()
            .expect("one operation per relation occurrence");
        return;
    }
    for input in operation.inputs_mut() {
        install_scan_filter(input, relation, replacement);
    }
}
