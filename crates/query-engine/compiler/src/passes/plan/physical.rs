use super::requirements::{Column, OutputValue, Predicate, Projection, id_list};
use crate::error::{QueryError, Result};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::bindings::{ColumnRef, DefinitionId, RelationId, RelationSource, ScopeId};
use query_data_model::storage::StoredColumnRef;
use query_data_model::{QueryBackendCatalog, QueryDataModel};

use super::NodePlan;
use super::context::PlanningContext;

pub struct ExecutionPlan {
    pub source: PhysicalSource,
    pub definitions: Vec<(DefinitionId, PhysicalPlan)>,
    pub outputs: Vec<Projection>,
    pub bindings: std::collections::HashMap<String, super::NodeBinding<NodeIdentity, Column>>,
}

#[derive(Clone)]
pub enum KeyConstraint {
    Ids(StoredColumnRef, Vec<i64>),
    Membership(StoredColumnRef, DefinitionId),
}

#[derive(Clone)]
pub enum NodeIdentity {
    Column(Column),
    Pinned(i64),
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub scope: ScopeId,
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
        relation: RelationId,
        arms: Vec<PhysicalPlan>,
        relationship: usize,
    },
    Scan {
        relation: RelationId,
        final_: bool,
        relationship: Option<usize>,
    },
    Filter {
        predicates: Vec<Predicate>,
        input: Box<Self>,
    },
    Scope {
        relation: RelationId,
        input: Box<PhysicalPlan>,
    },
    Join {
        endpoints: (Column, Column),
        predicates: Vec<Predicate>,
        left: Box<Self>,
        right: Box<Self>,
    },
    Latest {
        version: Column,
        relation: RelationId,
        scope: ScopeId,
        sort_key: Vec<ColumnRef>,
        aggregate_condition: Vec<Predicate>,
        input: Box<Self>,
    },
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn node_binding(
        &self,
        node: &str,
        identity: Column,
        relation: Option<RelationId>,
    ) -> Result<(String, super::NodeBinding<NodeIdentity, Column>)> {
        Ok((
            node.into(),
            super::NodeBinding::Values {
                identity: NodeIdentity::Column(identity),
                relation,
                traversal_path: self
                    .optional_column(identity.relation(), ontology::TRAVERSAL_PATH_COLUMN)?,
            },
        ))
    }
    pub fn projection(
        &mut self,
        scope: ScopeId,
        value: OutputValue,
        name: impl Into<String>,
    ) -> Result<Projection> {
        let export = match value {
            OutputValue::Column(column) => self.bindings.project_column(scope, column),
            _ => self.bindings.project(scope),
        }
        .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.exports.insert(export, name.into());
        Ok(Projection {
            value,
            name: export,
        })
    }

    pub fn define(
        &mut self,
        parent: ScopeId,
        hint: impl Into<String>,
        plan: PhysicalPlan,
    ) -> Result<(DefinitionId, PhysicalPlan)> {
        let definition = self
            .bindings
            .define(parent, plan.scope)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.definition_name(definition, &hint.into());
        Ok((definition, plan))
    }

    pub fn key_membership(
        &mut self,
        column: Column,
        definition: DefinitionId,
    ) -> Result<Predicate> {
        let scope = self
            .bindings
            .relation_scope(column.relation())
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let relation = self
            .bindings
            .reference(scope, definition)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let key = *self
            .bindings
            .exports(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
            .first()
            .ok_or_else(|| QueryError::Lowering("membership definition has no output".into()))?;
        Ok(Predicate::Membership {
            column,
            definition,
            key,
        })
    }
    pub fn deletion_column(&self, relation: RelationId, table: &str) -> Result<Option<Column>> {
        let storage = self.model.query_backend().storage();
        let table_id = storage
            .resolve_table(table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let layout = storage.table(table_id);
        let query_data_model::storage::RowSemantics::Versioned {
            deletion: Some(deletion),
            ..
        } = layout.row_semantics()
        else {
            return Ok(None);
        };
        self.stored_column(
            relation,
            StoredColumnRef {
                table: table_id,
                column: deletion.column,
            },
        )
        .map(Some)
    }

    pub fn stored_column(&self, relation: RelationId, column: StoredColumnRef) -> Result<Column> {
        let scope = self
            .bindings
            .relation_scope(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.bindings
            .stored_column(scope, relation, column)
            .map_err(|error| QueryError::Lowering(error.to_string()))
    }

    pub fn optional_column(&self, relation: RelationId, name: &str) -> Result<Option<Column>> {
        let scope = self
            .bindings
            .relation_scope(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let storage = self.model.query_backend().storage();
        let export = self.bindings.exports(relation).map_err(|error| QueryError::Lowering(error.to_string()))?.iter()
            .find(|export| matches!(self.bindings.origin(**export), Ok(query_data_model::bindings::ExportOrigin::Stored(column)) if storage.column(column).name() == name));
        export
            .map(|export| {
                self.bindings
                    .column(scope, relation, *export)
                    .map_err(|error| QueryError::Lowering(error.to_string()))
            })
            .transpose()
    }

    pub fn column(&self, relation: RelationId, name: &str) -> Result<Column> {
        self.optional_column(relation, name)?
            .ok_or_else(|| QueryError::Lowering(format!("relation has no column '{name}'")))
    }

    pub(super) fn publish(
        &mut self,
        parent: ScopeId,
        body: ScopeId,
        source: RelationId,
    ) -> Result<RelationId> {
        let exports = self
            .bindings
            .exports(source)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
            .to_vec();
        for source_export in exports {
            let column = self
                .bindings
                .column(body, source, source_export)
                .map_err(|error| QueryError::Lowering(error.to_string()))?;
            self.projection(
                body,
                OutputValue::Column(column),
                self.names.exports[&source_export].clone(),
            )?;
        }
        let relation = self
            .bindings
            .derived(parent, body)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(
            self.bindings,
            relation,
            &self.names.relations[&source].clone(),
        )?;
        Ok(relation)
    }

    pub fn scoped(
        &mut self,
        parent: ScopeId,
        body: ScopeId,
        source: PhysicalSource,
    ) -> Result<PhysicalSource> {
        let relation = self.publish(parent, body, source.relation())?;
        Ok(PhysicalSource::Scope {
            relation,
            input: Box::new(PhysicalPlan {
                scope: body,
                source,
                outputs: vec![],
            }),
        })
    }
    pub fn child_scope(&mut self, parent: ScopeId) -> Result<ScopeId> {
        self.bindings
            .scope(parent)
            .map_err(|error| QueryError::Lowering(error.to_string()))
    }

    pub fn scan(
        &mut self,
        scope: ScopeId,
        table: &str,
        alias: &str,
        final_: bool,
        relationship: Option<usize>,
    ) -> Result<PhysicalSource> {
        let storage = self.model.query_backend().storage();
        let table = storage
            .resolve_table(table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        let relation = self
            .bindings
            .scan(storage, scope, table)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names
            .tables
            .insert(table, storage.table(table).name().into());
        self.names.relation_name(self.bindings, relation, alias)?;
        for (export, column) in self
            .bindings
            .exports(relation)
            .expect("scan exports")
            .iter()
            .zip(storage.table(table).columns())
        {
            self.names.exports.insert(*export, column.name().into());
        }
        Ok(PhysicalSource::Scan {
            relation,
            final_,
            relationship,
        })
    }

    pub fn current_rows(
        &mut self,
        scope: ScopeId,
        scan: PhysicalSource,
        table: &str,
        alias: &str,
        columns: &[String],
        predicates: Vec<Predicate>,
    ) -> Result<PhysicalSource> {
        let table = self
            .model
            .table(table)
            .ok_or_else(|| QueryError::Lowering(format!("unknown table '{table}'")))?;
        if *table.row_semantics() == query_data_model::storage::RowSemantics::Current {
            return Ok(scan.filter(predicates));
        }
        let body = self
            .bindings
            .relation_scope(scan.relation())
            .expect("scan scope");
        let mut outputs = Vec::new();
        let deletion = self.deletion_column(scan.relation(), table.name())?;
        let deletion_name = deletion.map(|column| self.names.exports[&column.export()].clone());
        for name in columns
            .first()
            .map(String::as_str)
            .into_iter()
            .chain(deletion_name.as_deref())
            .chain(columns.iter().skip(1).map(String::as_str))
        {
            if !outputs
                .iter()
                .any(|projection: &Projection| self.names.exports[&projection.name] == name)
            {
                outputs.push(self.projection(
                    body,
                    OutputValue::Column(self.column(scan.relation(), name)?),
                    name,
                )?);
            }
        }
        let relation = self
            .bindings
            .derived(scope, body)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
        self.names.relation_name(self.bindings, relation, alias)?;
        let live = self
            .deletion_column(relation, table.name())?
            .map(super::requirements::live)
            .into_iter()
            .collect();
        let source = self.latest(scan, predicates, vec![])?;
        Ok(PhysicalSource::Scope {
            relation,
            input: Box::new(PhysicalPlan {
                scope: body,
                source,
                outputs,
            }),
        }
        .filter(live))
    }

    pub(super) fn latest(
        &self,
        source: PhysicalSource,
        predicates: Vec<Predicate>,
        aggregate_condition: Vec<Predicate>,
    ) -> Result<PhysicalSource> {
        if let PhysicalSource::Filter {
            predicates: mut existing,
            input,
        } = source
        {
            existing.extend(predicates);
            return self.latest(*input, existing, aggregate_condition);
        }
        let PhysicalSource::Scan { relation, .. } = source else {
            return Err(QueryError::Lowering(
                "latest-row selection requires a stored scan".into(),
            ));
        };
        let RelationSource::Scan(table) = self
            .bindings
            .source(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?
        else {
            return Err(QueryError::Lowering(
                "latest-row source is not a stored table".into(),
            ));
        };
        let layout = self.model.query_backend().storage().table(*table);
        if *layout.row_semantics() == query_data_model::storage::RowSemantics::Current {
            return Ok(source.filter(predicates).filter(aggregate_condition));
        }
        let scope = self
            .bindings
            .relation_scope(relation)
            .map_err(|error| QueryError::Lowering(error.to_string()))?;
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
                self.bindings
                    .stored_column(
                        scope,
                        relation,
                        query_data_model::storage::StoredColumnRef {
                            table: *table,
                            column: *column,
                        },
                    )
                    .map_err(|error| QueryError::Lowering(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let query_data_model::storage::RowSemantics::Versioned { version, .. } =
            layout.row_semantics()
        else {
            return Err(QueryError::Lowering(
                "latest-row selection requires versioned storage".into(),
            ));
        };
        let version = self.stored_column(
            relation,
            StoredColumnRef {
                table: *table,
                column: *version,
            },
        )?;
        Ok(PhysicalSource::Latest {
            version,
            relation,
            scope,
            sort_key,
            aggregate_condition,
            input: Box::new(source.filter(predicates)),
        })
    }
}

impl PhysicalSource {
    pub fn relation(&self) -> RelationId {
        match self {
            Self::Scan { relation, .. }
            | Self::Scope { relation, .. }
            | Self::Union { relation, .. }
            | Self::Latest { relation, .. } => *relation,
            Self::Filter { input, .. } | Self::KeyFilter { input, .. } => input.relation(),
            Self::Join { .. } => panic!("join has multiple relations"),
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

    pub(super) fn cascade(self, value: Column, upstream: Option<&PhysicalPlan>) -> Self {
        match upstream {
            Some(keys) => Self::KeyFilter {
                value,
                keys: Box::new(keys.clone()),
                input: Box::new(self),
            },
            None => self,
        }
    }
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn node(&self, alias: &str) -> Result<&NodePlan> {
        self.nodes
            .get(alias)
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
    }

    pub(super) fn node_source(
        &mut self,
        scope: ScopeId,
        alias: &str,
        final_: bool,
    ) -> Result<PhysicalSource> {
        let table = self
            .node(alias)?
            .table
            .as_deref()
            .and_then(|table| self.model.table(table))
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' has no table")))?;
        self.scan(scope, table.name(), alias, final_, None)
    }

    pub(super) fn cascade(
        &self,
        source: PhysicalSource,
        index: usize,
        upstream: Option<&PhysicalPlan>,
    ) -> Result<PhysicalSource> {
        if upstream.is_none() {
            return Ok(source);
        }
        let column = self.column(
            source.relation(),
            &self.hops[index]
                .join_prev
                .as_ref()
                .expect("cascade join")
                .curr_col,
        )?;
        Ok(source.cascade(column, upstream))
    }

    pub(super) fn edge_scan(
        &mut self,
        scope: ScopeId,
        index: usize,
        alias: &str,
        final_: bool,
    ) -> Result<PhysicalSource> {
        let hop = &self.hops[index];
        let table = self.model.table(&hop.edge_table).ok_or_else(|| {
            QueryError::Lowering(format!("unknown edge table '{}'", hop.edge_table))
        })?;
        self.scan(scope, table.name(), alias, final_, Some(hop.input_index))
    }

    pub fn candidate_keys(
        &mut self,
        alias: &str,
        column: &str,
        extra: Vec<KeyConstraint>,
    ) -> Result<PhysicalPlan> {
        self.keys(alias, column, false, extra)
    }

    pub fn filtered_keys(&mut self, alias: &str, column: &str) -> Result<PhysicalPlan> {
        self.keys(alias, column, true, vec![])
    }

    fn keys(
        &mut self,
        alias: &str,
        column: &str,
        final_: bool,
        extra: Vec<KeyConstraint>,
    ) -> Result<PhysicalPlan> {
        let scope = self.child_scope(self.bindings.root())?;
        let source = self.node_source(scope, alias, final_)?;
        let relation = source.relation();
        let mut predicates = self.node_predicates(relation, self.node(alias)?)?;
        for constraint in extra {
            predicates.push(match constraint {
                KeyConstraint::Ids(column, values) => {
                    id_list(self.stored_column(relation, column)?, &values)
                }
                KeyConstraint::Membership(column, definition) => {
                    self.key_membership(self.stored_column(relation, column)?, definition)?
                }
            });
        }
        Ok(PhysicalPlan {
            scope,
            source: source.filter(predicates),
            outputs: vec![self.projection(
                scope,
                OutputValue::Column(self.column(relation, column)?),
                DEFAULT_PRIMARY_KEY,
            )?],
        })
    }

    pub fn node_scan(
        &mut self,
        alias: &str,
        narrowing: Option<(&str, DefinitionId)>,
    ) -> Result<PhysicalPlan> {
        let scope = self.bindings.root();
        let body = self.child_scope(scope)?;
        let versioned = self
            .node(alias)?
            .table
            .as_deref()
            .and_then(|table| self.model.table(table))
            .is_some_and(|table| {
                matches!(
                    table.row_semantics(),
                    query_data_model::storage::RowSemantics::Versioned { .. }
                )
            });
        let scan_scope = if narrowing.is_some() && versioned {
            self.child_scope(body)?
        } else {
            body
        };
        let mut source = self.node_source(scan_scope, alias, narrowing.is_none())?;
        let relation = source.relation();
        let node = self.node(alias)?;
        if let Some((column, definition)) = narrowing {
            let table = self
                .model
                .table(node.table.as_deref().expect("bound node table"))
                .expect("bound node table");
            let mut predicates =
                vec![self.key_membership(self.column(relation, column)?, definition)?];
            let node = self.node(alias)?;
            predicates.extend(
                node.filters
                    .iter()
                    .filter(|(column, filter)| {
                        table
                            .column_id(column)
                            .is_ok_and(|column| table.sort_key().contains(&column))
                            && filter.filter.rhs_column.is_none()
                    })
                    .map(|(column, filter)| {
                        self.property_filter(self.column(relation, column)?, filter)
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            if table
                .column_id(DEFAULT_PRIMARY_KEY)
                .is_ok_and(|column| table.sort_key().contains(&column))
            {
                if !node.node_ids.is_empty() {
                    predicates.push(id_list(
                        self.column(relation, DEFAULT_PRIMARY_KEY)?,
                        &node.node_ids,
                    ));
                }
                if let Some(range) = &node.id_range {
                    predicates.push(Predicate::IdRange {
                        column: self.column(relation, DEFAULT_PRIMARY_KEY)?,
                        start: range.start,
                        end: range.end,
                    });
                }
            }
            source = self.latest(source, predicates, vec![])?;
        }
        let relation = if scan_scope != body {
            self.publish(body, scan_scope, relation)?
        } else {
            relation
        };
        if let PhysicalSource::Latest {
            relation: output, ..
        } = &mut source
        {
            *output = relation;
        }
        let predicates = self.node_predicates(relation, self.node(alias)?)?;
        let source = self.scoped(scope, body, source.filter(predicates))?;
        let outputs = self.node_outputs(source.relation(), alias)?;
        self.node_relations.insert(alias.into(), source.relation());
        Ok(PhysicalPlan {
            scope,
            source,
            outputs,
        })
    }

    pub fn node_plan(&mut self, scope: ScopeId, alias: &str) -> Result<PhysicalPlan> {
        let source = self.node_source(scope, alias, true)?;
        self.node_relations.insert(alias.into(), source.relation());
        let predicates = self.node_predicates(source.relation(), self.node(alias)?)?;
        Ok(PhysicalPlan {
            scope,
            outputs: self.node_outputs(source.relation(), alias)?,
            source: source.filter(predicates),
        })
    }
}
