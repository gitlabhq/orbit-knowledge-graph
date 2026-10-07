use super::*;
use crate::input::{ColumnSelection, Input, InputNode};
use crate::passes::plan::{
    HydrationCompileOptions,
    hydration::{HydrationPathFilter, path_filter},
};

struct HydrationRow {
    body: BlockId,
    identity: OutputId,
    deleted: OutputId,
    properties: Vec<(String, OutputId)>,
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, LatestRows<'a>> {
    pub(super) fn hydration(
        &mut self,
        input: &Input,
        options: HydrationCompileOptions,
    ) -> Result<BlockId> {
        let mut arms = Vec::with_capacity(input.nodes.len());
        for (index, node) in input.nodes.iter().enumerate() {
            let row = self.hydration_row(index, node, options)?;
            arms.push(self.hydration_result(node, row)?);
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
        let root = self.query();
        let source = self.derive(root, body, "hydrate")?;
        let outputs = self
            .outputs(body)?
            .map(|output| {
                Ok((
                    self.output_label(output)?.to_owned(),
                    Expression::Column(self.output_column(source, output)?),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let operation =
            self.limit_relation(self.read_relation(source, ReadMode::Raw)?, input.limit)?;
        let projection = self.project_values(operation, outputs)?;
        self.finish_query(projection)
    }

    fn hydration_row(
        &mut self,
        index: usize,
        node: &InputNode,
        options: HydrationCompileOptions,
    ) -> Result<HydrationRow> {
        let Some(ColumnSelection::List(properties)) = &node.columns else {
            return Err(GraphError::UnsupportedInput(
                "hydration requires normalized columns".into(),
            ));
        };
        let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
        let table = self
            .catalog
            .entity_table(entity)
            .ok_or(GraphError::MissingOutput)?;
        let body = self.query();
        let scan = self.scan(body, table, &node.id)?;
        self.bind_scan(scan, ScanInput::Node(index))?;
        let mut operation = self.read_relation(scan, ReadMode::Raw)?;
        if let Some(paths) = path_filter(&node.traversal_paths, options) {
            let (paths, array) = match paths {
                HydrationPathFilter::PrefixUnion(paths) => (paths, false),
                HydrationPathFilter::PrefixSet(paths) => (paths, true),
            };
            operation = self.filter_relation(
                operation,
                Expression::Prefixes {
                    value: Box::new(Expression::Column(
                        self.stored_column(scan, ontology::TRAVERSAL_PATH_COLUMN)?,
                    )),
                    paths: Box::new(Expression::Strings(
                        paths
                            .into_iter()
                            .map(|path| path.as_str().to_owned())
                            .collect(),
                    )),
                    array,
                },
            )?;
        }
        if !node.node_ids.is_empty() {
            operation = self.filter_relation(
                operation,
                Expression::In(
                    Box::new(Expression::Column(
                        self.stored_column(scan, &node.id_property)?,
                    )),
                    Box::new(Expression::Integers(node.node_ids.clone())),
                ),
            )?;
        }
        let operation = self.latest_relation(
            operation,
            self.stored_column(scan, ontology::VERSION_COLUMN)?,
            None,
        )?;
        let mut projection = self.project_values(
            operation,
            [(
                node.id_property.clone(),
                Expression::Column(self.stored_column(scan, &node.id_property)?),
            )],
        )?;
        let identity = projection
            .outputs()
            .next()
            .ok_or(GraphError::EmptyProjection)?
            .0;
        let deleted = self.append_projection(
            &mut projection,
            ontology::DELETED_COLUMN,
            Expression::Column(self.stored_column(scan, ontology::DELETED_COLUMN)?),
        )?;
        let mut fields = Vec::new();
        for property in properties {
            if property == &node.id_property
                || property == ontology::DELETED_COLUMN
                || fields.iter().any(|(name, _)| name == property)
            {
                continue;
            }
            let output = self.append_projection(
                &mut projection,
                property,
                Expression::Column(self.stored_column(scan, property)?),
            )?;
            fields.push((property.clone(), output));
        }
        self.finish_query(projection)?;
        let properties = properties
            .iter()
            .map(|property| {
                let output = if property == &node.id_property {
                    identity
                } else if property == ontology::DELETED_COLUMN {
                    deleted
                } else {
                    fields
                        .iter()
                        .find(|(name, _)| name == property)
                        .ok_or(GraphError::MissingOutput)?
                        .1
                };
                Ok((property.clone(), output))
            })
            .collect::<Result<_>>()?;
        Ok(HydrationRow {
            body,
            identity,
            deleted,
            properties,
        })
    }

    fn hydration_result(&mut self, node: &InputNode, row: HydrationRow) -> Result<BlockId> {
        let block = self.query();
        let source = self.derive(block, row.body, &node.id)?;
        let operation = self.filter_relation(
            self.read_relation(source, ReadMode::Raw)?,
            Expression::equal(
                Expression::Column(self.output_column(source, row.deleted)?),
                Expression::Boolean(false),
            ),
        )?;
        let fields = row
            .properties
            .into_iter()
            .map(|(name, output)| {
                Ok((
                    name,
                    Expression::ToString(Box::new(Expression::Column(
                        self.output_column(source, output)?,
                    ))),
                ))
            })
            .collect::<Result<_>>()?;
        let projection = self.project_values(
            operation,
            [
                (
                    format!("{}_{}", node.id, node.id_property),
                    Expression::Column(self.output_column(source, row.identity)?),
                ),
                (
                    format!("{}_entity_type", node.id),
                    Expression::Text(node.entity.clone().ok_or(GraphError::MissingOutput)?),
                ),
                (format!("{}_props", node.id), Expression::JsonObject(fields)),
            ],
        )?;
        self.finish_query(projection)
    }
}
