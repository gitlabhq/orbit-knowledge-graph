use query_data_model::implementations::TableLayout;
use std::convert::Infallible;
use std::sync::Arc;

mod fusion;
pub use fusion::fuse_holder;

use query_data_model::{
    ClickHouseDataModel, EdgeField, Endpoint, QueryBackendCatalog, QueryDataModel,
};

use crate::ast::{Expr, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::lowering::{Context, EmitOperation, SqlFragment};
use crate::planning::bind::Source;
use crate::planning::generic::{
    Assignment, Expr as PlanExpr, Node, Op, Operation, Schema, ValueType, Values,
};
use crate::planning::optimize::Candidate;
use crate::planning::physical::{self, CurrentRows, Read, Scalar};

#[derive(Clone, PartialEq)]
pub struct Scan {
    read: Read,
    layout: Arc<TableLayout>,
    binding: Option<String>,
    relationship: Option<usize>,
    foreign_key: Option<ForeignKeySource>,
}

#[derive(Clone, PartialEq)]
struct ForeignKeySource {
    layout: Arc<TableLayout>,
    identity: String,
    foreign_key: String,
    holder: Endpoint,
    source_kind: String,
    target_kind: String,
    kind: String,
    fields: Vec<EdgeField>,
}

pub fn select(
    source: Source,
    model: &ClickHouseDataModel,
    values: &mut Values,
) -> Result<Node<Scan, Scalar, Infallible>> {
    let (binding, relationship) = match &source {
        Source::Entity { binding, .. } => (Some(binding.clone()), None),
        Source::Edge { relationship, .. } => (None, Some(*relationship)),
    };
    let foreign_key = match &source {
        Source::Edge {
            endpoints: Some((source, target)),
            relationships,
            fields,
            ..
        } if relationships.len() == 1
            && model
                .variant_scope(
                    &model.graph().relationship(relationships[0]).name,
                    &model.graph().entity(*source).name,
                    &model.graph().entity(*target).name,
                )
                .is_some_and(ontology::EdgeVariantScope::is_scope_preserving) =>
        {
            model
                .backend()
                .foreign_key(model.graph(), relationships, *source, *target)
                .and_then(|key| {
                    let holder = match key.holder {
                        Endpoint::Source => *source,
                        Endpoint::Target => *target,
                    };
                    let identity = model
                        .graph()
                        .property_id(holder, &model.graph().property(key.referenced_key).name)?;
                    Some(ForeignKeySource {
                        layout: model
                            .backend()
                            .table_layout(model.backend().entity_table(holder)?)?,
                        identity: model.property_column(identity)?.into(),
                        foreign_key: model.property_column(key.property)?.into(),
                        holder: key.holder,
                        source_kind: model.graph().entity(*source).name.clone(),
                        target_kind: model.graph().entity(*target).name.clone(),
                        kind: model.graph().relationship(relationships[0]).name.clone(),
                        fields: fields.iter().map(|(_, field)| *field).collect(),
                    })
                })
        }
        _ => None,
    };
    let plan = physical::select_source(source, model, CurrentRows::Final, values)?;

    plan.map_sources(&mut |read| {
        let layout = model.backend().table_layout(&read.table).ok_or_else(|| {
            QueryError::ReferenceError(format!("{} has no current-row deletion column", read.table))
        })?;

        Ok(Scan {
            read,
            layout,
            binding: binding.clone(),
            relationship,
            foreign_key: foreign_key.clone(),
        })
    })
}

pub fn realize_foreign_key(
    candidate: &Candidate<Scan, Scalar, Infallible>,
) -> Result<Vec<Candidate<Scan, Scalar, Infallible>>> {
    let Op::Read(scan) = &candidate.program.root.op else {
        return Ok(vec![]);
    };
    let Some(key) = &scan.foreign_key else {
        return Ok(vec![]);
    };
    let mut rewritten = candidate.clone();
    let identity = rewritten.values.allocate(ValueType::Int64);
    let foreign_key = rewritten.values.allocate(ValueType::Int64);
    let assignments = scan
        .read
        .columns
        .iter()
        .zip(&key.fields)
        .map(|((output, _), field)| {
            let expression = match field {
                EdgeField::SourceId => PlanExpr::Value(if key.holder == Endpoint::Source {
                    identity
                } else {
                    foreign_key
                }),
                EdgeField::TargetId => PlanExpr::Value(if key.holder == Endpoint::Target {
                    identity
                } else {
                    foreign_key
                }),
                EdgeField::SourceKind => PlanExpr::String(key.source_kind.clone()),
                EdgeField::TargetKind => PlanExpr::String(key.target_kind.clone()),
                EdgeField::RelationshipKind => PlanExpr::String(key.kind.clone()),
            };
            Assignment {
                output: *output,
                expression,
            }
        })
        .collect();

    rewritten.program.root = Node {
        op: Op::Project(assignments),
        inputs: vec![Node {
            op: Op::Read(Scan {
                read: Read {
                    table: key.layout.name.clone(),
                    columns: vec![
                        (identity, key.identity.clone()),
                        (foreign_key, key.foreign_key.clone()),
                    ],
                    current_rows: CurrentRows::Final,
                },
                layout: Arc::clone(&key.layout),
                binding: None,
                relationship: scan.relationship,
                foreign_key: None,
            }),
            inputs: vec![],
        }],
    };

    Ok(vec![rewritten])
}

impl Scan {
    pub fn explain(&self) -> crate::planning::explain::SExpression {
        use crate::planning::explain::SExpression;

        SExpression::node(
            "CurrentRows",
            [
                self.read.explain(),
                SExpression::node(
                    "Deleted",
                    [SExpression::atom(self.layout.deletion_column())],
                ),
                SExpression::node("Binding", self.binding.iter().map(SExpression::atom)),
                SExpression::node(
                    "Relationship",
                    self.relationship.iter().map(SExpression::atom),
                ),
            ],
        )
    }
}

impl Operation for Scan {
    fn retain_outputs(&mut self, required: &Schema) -> bool {
        if !self
            .read
            .columns
            .iter()
            .any(|(value, _)| required.contains(value))
        {
            return false;
        }
        if let Some(key) = &mut self.foreign_key {
            key.fields = self
                .read
                .columns
                .iter()
                .zip(&key.fields)
                .filter_map(|((value, _), field)| required.contains(value).then_some(*field))
                .collect();
        }
        self.read.retain_outputs(required)
    }

    fn map_values(&mut self, map: &mut impl FnMut(&mut crate::planning::generic::ValueId)) {
        self.read.map_values(map);
    }

    fn unique_keys(&self) -> Vec<Schema> {
        let key = self.layout.current_row_key();
        if key.is_empty() {
            return vec![];
        }

        key.iter()
            .map(|column| {
                self.read
                    .columns
                    .iter()
                    .find(|(_, name)| name == column)
                    .map(|(value, _)| *value)
            })
            .collect::<Option<Schema>>()
            .into_iter()
            .collect()
    }

    fn key_coverage(&self) -> crate::planning::generic::facts::KeyCoverage {
        crate::planning::generic::facts::KeyCoverage::Exact
    }

    fn output(&self, inputs: &[Schema], values: &Values) -> Result<Schema> {
        self.read.output(inputs, values)
    }
}

impl EmitOperation for Scan {
    fn emit(&self, _: Vec<SqlFragment>, context: &mut Context) -> Result<SqlFragment> {
        let alias = context.alias();
        if let Some(binding) = &self.binding {
            context
                .source_bindings
                .insert(alias.clone(), binding.clone());
        }

        let mut from = TableRef::scan_final(&self.read.table, &alias);
        if let Some(index) = self.relationship {
            from = from.with_relationship(index);
        }

        Ok(SqlFragment {
            query: Query {
                from,
                where_clause: Some(Expr::eq(
                    Expr::col(&alias, self.layout.deletion_column()),
                    Expr::lit(false),
                )),
                ..Default::default()
            },
            exports: self
                .read
                .columns
                .iter()
                .map(|(value, column)| (*value, Expr::col(&alias, column)))
                .collect(),
        })
    }
}
