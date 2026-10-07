use super::*;
use crate::input::{Input, InputRelationship};

pub(super) enum KeyRead {
    Raw,
    Current,
    Latest,
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn candidate(
        &mut self,
        parent: BlockId,
        original: (RelationId, KeyRead),
        key: &str,
        predicates: &[Expression<'a>],
        memberships: &[(&str, (DefinitionId, OutputId))],
        hint: &str,
    ) -> Result<(DefinitionId, OutputId)> {
        let (original, read) = original;
        let body = self.query_in(parent)?;
        let alias = self.relation(original)?.hint.clone();
        let (scan, mut operation) =
            self.key_source(body, original, &alias, &read, predicates, memberships)?;
        if matches!(read, KeyRead::Latest) {
            operation = self.latest_relation(
                operation,
                self.stored_column(scan, ontology::VERSION_COLUMN)?,
                None,
            )?;
        }
        let output = self.finish_key_query(operation, self.stored_column(scan, key)?, "id")?;
        Ok((self.define(parent, body, hint)?, output))
    }

    pub(super) fn cascade_keys(
        &mut self,
        parent: BlockId,
        input: &Input,
        scans: &[KeyScan<'a>],
        output_column: &str,
    ) -> Result<Option<ColumnRef<'a>>> {
        let Some(index) = scans.len().checked_sub(1) else {
            return Ok(None);
        };
        let mut selective = false;
        for scan in scans {
            let relationship = self.key_relationship(input, scan.relation)?;
            selective |= input
                .nodes
                .iter()
                .filter(|node| node.id == relationship.from || node.id == relationship.to)
                .any(|node| !node.node_ids.is_empty() || node.id_range.is_some());
        }
        if !selective && scans.iter().all(|scan| scan.memberships.is_empty()) {
            return Ok(None);
        }
        let hint = format!("e{index}p");
        let (body, output) = self.edge_keys(
            parent,
            input,
            scans,
            output_column,
            output_column,
            hint.clone(),
        )?;
        let relation = self.derive(parent, body, hint)?;
        Ok(Some(self.output_column(relation, output)?))
    }

    pub(super) fn edge_keys(
        &mut self,
        parent: BlockId,
        input: &Input,
        scans: &[KeyScan<'a>],
        output_column: &str,
        output_label: &str,
        alias: String,
    ) -> Result<(BlockId, OutputId)> {
        let (last, previous) = scans.split_last().ok_or(GraphError::MissingOutput)?;
        let body = self.query_in(parent)?;
        let (scan, mut operation) = self.key_source(
            body,
            last.relation,
            &alias,
            &KeyRead::Raw,
            &last.predicates,
            &last.memberships,
        )?;
        if let Some(upstream) = previous.last() {
            let upstream = self.key_relationship(input, upstream.relation)?;
            let current = self.key_relationship(input, last.relation)?;
            if let Some((upstream_column, current_column)) = connected_endpoints(upstream, current)
                && let Some(key) = self.cascade_keys(body, input, previous, upstream_column)?
            {
                operation = self.membership_relation(
                    operation,
                    self.stored_column(scan, current_column)?,
                    key,
                )?;
            }
        }
        let output = self.finish_key_query(
            operation,
            self.stored_column(scan, output_column)?,
            output_label,
        )?;
        Ok((body, output))
    }

    fn key_source(
        &mut self,
        block: BlockId,
        original: RelationId,
        hint: &str,
        read: &KeyRead,
        predicates: &[Expression<'a>],
        memberships: &[(&str, (DefinitionId, OutputId))],
    ) -> Result<(RelationId, PhysicalOperation<'a>)> {
        let declaration = self.relation(original)?;
        let Source::Stored(table) = declaration.source else {
            return Err(GraphError::LatestShape);
        };
        let input = declaration.input.clone();
        let scan = self.scan_stored(block, table, hint)?;
        if let Some(input) = input {
            self.bind_scan(scan, input)?;
        }
        let mode = if matches!(read, KeyRead::Current) {
            ReadMode::Current
        } else {
            ReadMode::Raw
        };
        let mut operation = self.read_relation(scan, mode)?;
        for predicate in predicates {
            let predicate = predicate.rebind(&|column| match column.port {
                Port::Stored(stored) if column.relation == original => {
                    self.stored_port(scan, stored)
                }
                _ => Err(GraphError::OutsideBlock),
            })?;
            operation = self.filter_relation(operation, predicate)?;
        }
        for (column, candidate) in memberships {
            operation = self.narrow(
                block,
                operation,
                self.stored_column(scan, column)?,
                *candidate,
            )?;
        }
        Ok((scan, operation))
    }

    fn key_relationship<'input>(
        &self,
        input: &'input Input,
        scan: RelationId,
    ) -> Result<&'input InputRelationship> {
        let Some(ScanInput::Relationship(index)) = self.relation(scan)?.input else {
            return Err(GraphError::MissingOutput);
        };
        input
            .relationships
            .get(index)
            .ok_or(GraphError::MissingOutput)
    }

    fn finish_key_query(
        &mut self,
        operation: PhysicalOperation<'a>,
        key: ColumnRef<'a>,
        label: &str,
    ) -> Result<OutputId> {
        let projection =
            self.project_values(operation, [(label.into(), Expression::Column(key))])?;
        let output = projection
            .outputs()
            .next()
            .ok_or(GraphError::EmptyProjection)?
            .0;
        self.finish_query(projection)?;
        Ok(output)
    }

    pub(super) fn narrow(
        &mut self,
        block: BlockId,
        input: PhysicalOperation<'a>,
        column: ColumnRef<'a>,
        (definition, output): (DefinitionId, OutputId),
    ) -> Result<PhysicalOperation<'a>> {
        let hint = self.definition_hint(definition)?.to_owned();
        let keys = self.reference(block, definition, hint)?;
        self.membership_relation(input, column, self.output_column(keys, output)?)
    }
}

fn connected_endpoints(
    upstream: &InputRelationship,
    current: &InputRelationship,
) -> Option<(&'static str, &'static str)> {
    let (upstream_start, upstream_end) = upstream.direction.edge_columns();
    let (current_start, current_end) = current.direction.edge_columns();
    [
        (&upstream.to, upstream_end),
        (&upstream.from, upstream_start),
    ]
    .into_iter()
    .find_map(|(alias, column)| {
        [(&current.from, current_start), (&current.to, current_end)]
            .into_iter()
            .find(|(candidate, _)| *candidate == alias)
            .map(|(_, current)| (column, current))
    })
}
