use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, node_outputs, node_predicates,
    property_filter,
};
use crate::error::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::bindings::{ColumnRef, QueryBindings, RelationId, RelationSource};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

use super::{Hop, NodePlan};

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub definitions: Vec<(crate::bindings::Definition, PhysicalPlan)>,
    pub outputs: Vec<Projection>,
    pub bindings: Vec<BindingSource>,
}

pub struct BindingSource {
    pub node: String,
    pub alias: String,
    pub column: String,
    pub joined: bool,
}

impl BindingSource {
    pub(super) fn table(alias: &str) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            column: DEFAULT_PRIMARY_KEY.into(),
            joined: true,
        }
    }
}

pub(super) fn key_membership(
    alias: &str,
    column: &str,
    name: crate::bindings::Definition,
) -> Predicate {
    Predicate::Membership {
        column: Column::new(alias, column),
        key: name.exports()[0].clone(),
        definition: name,
    }
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub source: PhysicalSource,
    pub outputs: Vec<Projection>,
}

#[derive(Clone)]
pub enum PhysicalSource {
    KeyFilter {
        value: Column,
        keys: Box<PhysicalPlan>,
        input: Box<Self>,
    },
    Union {
        alias: String,
        arms: Vec<PhysicalPlan>,
        relationship: usize,
    },
    Scan {
        relation: RelationId,
        alias: String,
        final_: bool,
        relationship: Option<usize>,
    },
    Filter {
        predicates: Vec<Predicate>,
        input: Box<Self>,
    },
    Scope {
        alias: String,
        input: Box<PhysicalPlan>,
    },
    Join {
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
        left: Box<Self>,
        right: Box<Self>,
    },
    Latest {
        sort_key: Vec<ColumnRef>,
        alias: String,
        aggregate_condition: Vec<Predicate>,
        input: Box<Self>,
    },
}

impl PhysicalSource {
    pub fn scan(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        table: &str,
        alias: &str,
        final_: bool,
        relationship: Option<usize>,
    ) -> Result<Self> {
        let storage = model.query_backend().storage();
        let table = storage
            .resolve_table(table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let relation = bindings
            .scan(storage, bindings.root(), table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        Ok(Self::Scan {
            relation,
            alias: alias.into(),
            final_,
            relationship,
        })
    }

    pub fn current_rows(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        table: &str,
        alias: &str,
        columns: &[String],
        predicates: Vec<Predicate>,
    ) -> Result<Self> {
        let scan = Self::scan(bindings, model, table, alias, false, None)?;
        let table = model.table(table).expect("bound scan table");
        if *table.row_semantics() == query_data_model::storage::RowSemantics::Current {
            return Ok(scan.filter(predicates));
        }
        let mut outputs = Vec::new();
        for name in columns
            .first()
            .map(String::as_str)
            .into_iter()
            .chain([ontology::DELETED_COLUMN])
            .chain(columns.iter().skip(1).map(String::as_str))
        {
            if !outputs
                .iter()
                .any(|projection: &Projection| projection.name.name() == name)
            {
                outputs.push(Projection::col(alias, name));
            }
        }
        Ok(Self::Scope {
            alias: alias.into(),
            input: Box::new(PhysicalPlan {
                source: scan.latest(bindings, model, predicates, vec![])?,
                outputs,
            }),
        }
        .filter(vec![super::requirements::live(alias)]))
    }

    pub(super) fn latest(
        self,
        bindings: &QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        predicates: Vec<Predicate>,
        aggregate_condition: Vec<Predicate>,
    ) -> Result<Self> {
        if let Self::Filter {
            predicates: mut existing,
            input,
        } = self
        {
            existing.extend(predicates);
            return input.latest(bindings, model, existing, aggregate_condition);
        }
        let Self::Scan {
            relation,
            ref alias,
            ..
        } = self
        else {
            return Err(QueryError::Lowering(
                "latest-row selection requires a stored scan".into(),
            ));
        };
        let RelationSource::Scan(table) = bindings
            .source(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
        else {
            return Err(QueryError::Lowering(
                "latest-row source is not a stored table".into(),
            ));
        };
        let layout = model.query_backend().storage().table(*table);
        if layout.sort_key().is_empty() {
            return Err(QueryError::Lowering(format!(
                "table '{}' has no latest-row key",
                layout.name()
            )));
        }
        let sort_key = layout
            .sort_key()
            .iter()
            .map(|column| {
                bindings
                    .stored_column(
                        bindings.root(),
                        relation,
                        query_data_model::storage::StoredColumnRef {
                            table: *table,
                            column: *column,
                        },
                    )
                    .map_err(|error| QueryError::Lowering(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Latest {
            sort_key,
            alias: alias.clone(),
            aggregate_condition,
            input: Box::new(self.filter(predicates)),
        })
    }

    pub fn scoped(self, alias: &str) -> Self {
        Self::Scope {
            alias: alias.into(),
            input: Box::new(PhysicalPlan {
                source: self,
                outputs: vec![],
            }),
        }
    }
    pub(super) fn inner_join(self, right: Self, endpoints: (Column, Column)) -> Self {
        Self::Join {
            endpoints,
            predicates: vec![],
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    fn node(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
        final_: bool,
    ) -> Result<Self> {
        Self::scan(
            bindings,
            model,
            node.table.as_deref().ok_or_else(|| {
                QueryError::Lowering(format!("node '{}' has no table", node.alias))
            })?,
            &node.alias,
            final_,
            None,
        )
    }

    pub(crate) fn filter(self, predicates: Vec<Predicate>) -> Self {
        if predicates.is_empty() {
            self
        } else {
            Self::Filter {
                predicates,
                input: Box::new(self),
            }
        }
    }

    pub(super) fn cascade(self, hop: &Hop, alias: &str, upstream: Option<&PhysicalPlan>) -> Self {
        match upstream {
            Some(keys) => Self::KeyFilter {
                value: Column::new(
                    alias,
                    &hop.join_prev.as_ref().expect("cascade join").curr_col,
                ),
                keys: Box::new(keys.clone()),
                input: Box::new(self),
            },
            None => self,
        }
    }

    pub(super) fn edge_keys(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        hop: &Hop,
        alias: &str,
        predicates: Vec<Predicate>,
        upstream: Option<&PhysicalPlan>,
    ) -> Result<Self> {
        let source = Self::Filter {
            predicates,
            input: Box::new(Self::scan(
                bindings,
                model,
                &hop.edge_table,
                alias,
                false,
                Some(hop.input_index),
            )?),
        };
        Ok(source.cascade(hop, alias, upstream))
    }
}

impl PhysicalPlan {
    pub fn define(self, hint: impl Into<String>) -> (crate::bindings::Definition, Self) {
        let definition = crate::bindings::Definition::new(
            hint,
            self.outputs
                .iter()
                .map(|output| output.name.clone())
                .collect(),
        );
        (definition, self)
    }
    pub fn candidate_keys(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
        column: &str,
        extra: Vec<Predicate>,
    ) -> Result<Self> {
        Self::keys(bindings, model, node, column, false, extra)
    }

    pub fn filtered_keys(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
        column: &str,
    ) -> Result<Self> {
        Self::keys(bindings, model, node, column, true, vec![])
    }

    fn keys(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
        column: &str,
        final_: bool,
        extra: Vec<Predicate>,
    ) -> Result<Self> {
        let mut predicates = node_predicates(node);
        predicates.extend(extra);
        Ok(Self {
            source: PhysicalSource::node(bindings, model, node, final_)?.filter(predicates),
            outputs: vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, column)),
                DEFAULT_PRIMARY_KEY,
            )],
        })
    }

    pub fn node_scan(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
        narrowing: Option<Predicate>,
    ) -> Result<Self> {
        let mut source = PhysicalSource::node(bindings, model, node, narrowing.is_none())?;
        if let Some(narrowing) = narrowing {
            let table = model
                .table(node.table.as_deref().expect("bound node table"))
                .expect("bound node table");
            let mut predicates = vec![narrowing];
            predicates.extend(
                node.filters
                    .iter()
                    .filter(|(column, filter)| {
                        table
                            .column_id(column)
                            .is_ok_and(|column| table.sort_key().contains(&column))
                            && filter.filter.rhs_column.is_none()
                    })
                    .map(|(column, filter)| property_filter(&node.alias, column, filter)),
            );
            if table
                .column_id(DEFAULT_PRIMARY_KEY)
                .is_ok_and(|column| table.sort_key().contains(&column))
            {
                if !node.node_ids.is_empty() {
                    predicates.push(id_list(&node.alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
                }
                if let Some(range) = &node.id_range {
                    predicates.push(Predicate::IdRange {
                        column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        start: range.start,
                        end: range.end,
                    });
                }
            }
            source = source.latest(bindings, model, predicates, vec![])?;
        }
        Ok(Self {
            source: source.filter(node_predicates(node)).scoped(&node.alias),
            outputs: node_outputs(node),
        })
    }

    pub fn single_node(
        bindings: &mut QueryBindings,
        model: &(impl QueryDataModel + ?Sized),
        node: &NodePlan,
    ) -> Result<Self> {
        Ok(Self {
            outputs: node_outputs(node),
            source: PhysicalSource::node(bindings, model, node, true)?
                .filter(node_predicates(node)),
        })
    }
}
