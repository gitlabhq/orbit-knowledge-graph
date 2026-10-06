use super::*;
use crate::input::{ColumnSelection, Input};
use crate::passes::plan::{
    HydrationCompileOptions,
    hydration::{HydrationPathFilter, path_filter},
};

impl<'catalog, M: QueryDataModel + ?Sized>
    QueryGraph<'catalog, M, Expression<'catalog>, PhysicalOperation<'catalog>>
{
    pub(super) fn hydration(
        &mut self,
        input: &Input,
        options: HydrationCompileOptions,
    ) -> Result<BlockId> {
        let mut arms = Vec::new();
        for (index, node) in input.nodes.iter().enumerate() {
            let entity = node.entity.as_deref().ok_or(GraphError::MissingOutput)?;
            let table = self
                .catalog
                .entity_table(entity)
                .ok_or(GraphError::MissingOutput)?;
            let keys = self.select(PhysicalOperation::One);
            let scan = self.scan(keys, table, &node.id)?;
            self.bind_scan(scan, ScanInput::Node(index))?;
            let mut operation = PhysicalOperation::source(scan);
            if let Some(paths) = path_filter(&node.traversal_paths, options) {
                let (paths, array) = match paths {
                    HydrationPathFilter::PrefixUnion(paths) => (paths, false),
                    HydrationPathFilter::PrefixSet(paths) => (paths, true),
                };
                operation = operation.filter(Expression::Prefixes {
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
                });
            }
            if !node.node_ids.is_empty() {
                operation = operation.filter(Expression::In(
                    Box::new(Expression::Column(
                        self.stored_column(scan, &node.id_property)?,
                    )),
                    Box::new(Expression::Integers(node.node_ids.clone())),
                ));
            }
            *self.operation_mut(keys)? =
                operation.latest(self.stored_column(scan, ontology::VERSION_COLUMN)?, None);
            let Some(ColumnSelection::List(properties)) = &node.columns else {
                return Err(GraphError::UnsupportedInput(
                    "hydration requires normalized columns".into(),
                ));
            };
            let mut columns = vec![node.id_property.as_str(), ontology::DELETED_COLUMN];
            for property in properties {
                if !columns.contains(&property.as_str()) {
                    columns.push(property);
                }
            }
            let mut outputs = std::collections::HashMap::new();
            for property in columns {
                let output = self.project(
                    keys,
                    property,
                    Expression::Column(self.stored_column(scan, property)?),
                )?;
                outputs.insert(property, output);
            }
            let arm = self.select(PhysicalOperation::One);
            let relation = self.derive(arm, keys, &node.id)?;
            *self.operation_mut(arm)? =
                PhysicalOperation::source(relation).filter(Expression::equal(
                    Expression::Column(
                        self.output_column(relation, outputs[ontology::DELETED_COLUMN])?,
                    ),
                    Expression::Boolean(false),
                ));
            self.project(
                arm,
                format!("{}_{}", node.id, node.id_property),
                Expression::Column(
                    self.output_column(relation, outputs[node.id_property.as_str()])?,
                ),
            )?;
            self.project(
                arm,
                format!("{}_entity_type", node.id),
                Expression::Text(entity.into()),
            )?;
            let fields = properties
                .iter()
                .map(|property| {
                    Ok((
                        property.clone(),
                        Expression::ToString(Box::new(Expression::Column(
                            self.output_column(relation, outputs[property.as_str()])?,
                        ))),
                    ))
                })
                .collect::<Result<_>>()?;
            self.project(
                arm,
                format!("{}_props", node.id),
                Expression::JsonObject(fields),
            )?;
            arms.push(arm);
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
        let root = self.select(PhysicalOperation::One);
        let relation = self.derive(root, body, "hydrate")?;
        let outputs = self.outputs(body)?.collect::<Vec<_>>();
        for output in outputs {
            self.project(
                root,
                self.output_label(output)?.to_owned(),
                Expression::Column(self.output_column(relation, output)?),
            )?;
        }
        *self.operation_mut(root)? = PhysicalOperation::source(relation).limit(input.limit);
        Ok(root)
    }
}
