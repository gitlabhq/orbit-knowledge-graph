use query_data_model::{ClickHouseDataModel, QueryDataModel};

use super::backends::clickhouse;
use super::bind;
use super::generic::{Operation, Schema, ValueId, ValueType, Values};
use crate::ast::{Expr, Node as SqlNode, Query};
use crate::constants::HYDRATION_NODE_ALIAS;
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input};
use crate::lowering::{Context, EmitOperation, SqlFragment, resolve, scalar};
use crate::passes::hydrate::HydrationCompileOptions;
use crate::passes::response_policy::excerpt;

struct Properties {
    entity: String,
    identity: ValueId,
    fields: Vec<(String, ValueId, bool)>,
    outputs: Schema,
    max_chars: u32,
}

impl Operation for Properties {
    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema> {
        let [input] = inputs else {
            return Err(QueryError::PipelineInvariant(
                "hydration requires one input".into(),
            ));
        };

        for value in
            std::iter::once(&self.identity).chain(self.fields.iter().map(|(_, value, _)| value))
        {
            if !input.contains(value) {
                return Err(QueryError::PipelineInvariant(
                    "hydration field was not retained".into(),
                ));
            }
            values.data_type(*value)?;
        }

        Ok(self.outputs.clone())
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        map(&mut self.identity);
        for (_, value, _) in &mut self.fields {
            map(value);
        }
        self.outputs.iter_mut().for_each(map);
    }
}

impl EmitOperation for Properties {
    fn emit(&self, mut inputs: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let (from, bindings, _) = context.relation(inputs.pop().expect("verified hydration arity"));
        let mut entries = Vec::new();

        for (name, value, truncate) in &self.fields {
            let mut value = resolve(&bindings, *value)?;
            if *truncate {
                value = excerpt(value, self.max_chars);
            }
            entries.push(Expr::string(name));
            entries.push(Expr::func("toString", vec![value]));
        }

        let properties = if entries.is_empty() {
            Expr::string("{}")
        } else {
            Expr::func("toJSONString", vec![Expr::func("map", entries)])
        };

        Ok(SqlFragment {
            query: Query {
                from,
                ..Default::default()
            },
            exports: vec![
                (self.outputs[0], resolve(&bindings, self.identity)?),
                (self.outputs[1], Expr::string(&self.entity)),
                (self.outputs[2], properties),
            ],
        })
    }
}

pub fn plan(
    input: &Input,
    options: HydrationCompileOptions,
    model: &ClickHouseDataModel,
) -> Result<SqlNode> {
    let mut values = Values::default();
    let mut context = Context::default();
    let mut inputs = Vec::new();
    let max_chars = (8 * 1024 * 1024 / 4) / input.limit.max(1);

    for node in &input.nodes {
        if node.node_ids.is_empty() {
            continue;
        }
        if !node.filters.is_empty() || node.id_range.is_some() {
            return Err(QueryError::Validation(
                "hydration accepts authorized IDs, not query filters".into(),
            ));
        }

        let entity = node
            .entity
            .as_deref()
            .and_then(|name| model.entity(name))
            .ok_or_else(|| QueryError::ReferenceError("hydration entity is unavailable".into()))?;
        let Some(ColumnSelection::List(requested)) = &node.columns else {
            return Err(QueryError::Validation(
                "hydration requires explicit columns".into(),
            ));
        };
        let required = [(node.id_property.clone(), context.alias())];
        let single = Input {
            nodes: vec![node.clone()],
            ..Default::default()
        };
        let bound = bind::bind_node(&single, model, &required, None, values)?;
        values = bound.values;
        let physical = bound
            .root
            .expand_sources(&mut |source| clickhouse::select(source, model, &mut values))?;
        let layout = model
            .backend()
            .table_for_entity(entity.id)
            .expect("bound hydration table");

        let paths = orbit_utils::traversal_path::prune_to_leaves(&node.traversal_paths);
        let mut paths = paths;
        if let Some(budget) = options.path_segment_budget {
            while paths.iter().map(|path| path.segment_count()).sum::<usize>() > budget {
                let next: Vec<_> = paths.iter().map(|path| path.parent()).collect();
                if next == paths {
                    break;
                }
                paths = orbit_utils::traversal_path::TraversalPathTrie::from_paths(
                    &next.iter().collect::<Vec<_>>(),
                )
                .to_minimal_prefixes();
            }
        }

        let fragment =
            crate::lowering::lower_with_context(&physical, &values, &mut context, &scalar::emit)?;
        let mut fragment = fragment;
        if let Some(path) = layout.path_columns.first().filter(|_| !paths.is_empty()) {
            crate::ast::visit::visit_queries_mut(&mut fragment.query, &mut |query| {
                if let crate::ast::TableRef::Scan { alias, table, .. } = &query.from
                    && table == &layout.name
                {
                    let predicate = Expr::or_all(paths.iter().map(|prefix| {
                        Some(Expr::func(
                            "startsWith",
                            vec![Expr::col(alias, &path.name), Expr::string(prefix.as_str())],
                        ))
                    }))
                    .unwrap();
                    query.where_clause =
                        Expr::and_all([query.where_clause.take(), Some(predicate)]);
                }
                Ok(())
            })?;
        }

        let schema = physical.output(&values)?;
        let dependencies: std::collections::HashSet<_> = entity
            .properties
            .iter()
            .filter_map(|property| model.property_realization(*property))
            .filter_map(|realization| match realization {
                query_data_model::PropertyRealization::Virtual(source) => Some(&source.depends_on),
                _ => None,
            })
            .flatten()
            .collect();
        let fields = requested
            .iter()
            .zip(&schema)
            .map(|(name, value)| {
                let truncate = model
                    .property(&entity.name, name)
                    .is_some_and(|property| property.data_type == ontology::DataType::String)
                    && !dependencies.contains(name);
                (name.clone(), *value, truncate)
            })
            .collect();
        let outputs = vec![
            values.allocate(values.data_type(bound.required[0])?.clone()),
            values.allocate(ValueType::String),
            values.allocate(ValueType::String),
        ];
        let operation = Properties {
            entity: entity.name.clone(),
            identity: bound.required[0],
            fields,
            outputs: outputs.clone(),
            max_chars,
        };
        operation.output(&[schema], &values)?;
        inputs.push(operation.emit(vec![fragment], &mut context)?);
    }

    let names = [
        format!("{HYDRATION_NODE_ALIAS}_id"),
        format!("{HYDRATION_NODE_ALIAS}_entity_type"),
        format!("{HYDRATION_NODE_ALIAS}_props"),
    ]
    .map(crate::ast::Identifier::from);
    let mut queries = inputs
        .into_iter()
        .map(|fragment| fragment.into_query(&names))
        .collect::<Result<Vec<_>>>()?;
    if queries.is_empty() {
        return Err(QueryError::Validation(
            "hydration requires at least one authorized ID".into(),
        ));
    }
    let mut query = queries.remove(0);
    query.union_all = queries;
    query.limit = Some(input.limit);

    Ok(SqlNode::Query(Box::new(query)))
}
