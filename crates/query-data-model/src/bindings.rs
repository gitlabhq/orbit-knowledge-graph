use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::{StorageCatalog, StoredColumnRef};
use crate::{ColumnId, TableId};

static NEXT_QUERY: AtomicU64 = AtomicU64::new(1);

macro_rules! binding_id {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name { query: u64, index: usize }
    )+};
}

binding_id!(ScopeId, RelationId, DefinitionId, ExportId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnRef {
    relation: RelationId,
    export: ExportId,
}

impl ColumnRef {
    pub fn relation(self) -> RelationId {
        self.relation
    }
    pub fn export(self) -> ExportId {
        self.export
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportOrigin {
    Stored(StoredColumnRef),
    Projected(ScopeId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationSource {
    Scan(TableId),
    Derived(ScopeId),
    Definition(DefinitionId),
    Union(Vec<ScopeId>),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BindingError {
    #[error("binding belongs to a different query")]
    ForeignQuery,
    #[error("binding is outside the current scope")]
    OutsideScope,
    #[error("column is not exposed by this relation")]
    MissingExport,
    #[error("UNION requires at least one arm with matching output counts")]
    UnionShape,
}

struct Scope {
    parent: Option<ScopeId>,
    outputs: Vec<ExportId>,
    sealed: bool,
}

struct Relation {
    scope: ScopeId,
    source: RelationSource,
    exports: Vec<ExportId>,
}

struct Definition {
    scope: ScopeId,
    body: ScopeId,
}

pub struct QueryBindings {
    query: u64,
    scopes: Vec<Scope>,
    relations: Vec<Relation>,
    definitions: Vec<Definition>,
    exports: Vec<ExportOrigin>,
}

impl Default for QueryBindings {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBindings {
    pub fn new() -> Self {
        Self {
            query: NEXT_QUERY.fetch_add(1, Ordering::Relaxed),
            scopes: vec![Scope {
                parent: None,
                outputs: vec![],
                sealed: false,
            }],
            relations: vec![],
            definitions: vec![],
            exports: vec![],
        }
    }

    pub fn root(&self) -> ScopeId {
        ScopeId {
            query: self.query,
            index: 0,
        }
    }

    pub fn scope(&mut self, parent: ScopeId) -> Result<ScopeId, BindingError> {
        self.check(parent.query)?;
        let id = ScopeId {
            query: self.query,
            index: self.scopes.len(),
        };
        self.scopes.push(Scope {
            parent: Some(parent),
            outputs: vec![],
            sealed: false,
        });
        Ok(id)
    }

    pub fn project(&mut self, scope: ScopeId) -> Result<ExportId, BindingError> {
        self.check(scope.query)?;
        if self.scopes[scope.index].sealed {
            return Err(BindingError::OutsideScope);
        }
        let export = self.allocate_export(ExportOrigin::Projected(scope));
        self.scopes[scope.index].outputs.push(export);
        Ok(export)
    }

    pub fn outputs(&self, scope: ScopeId) -> Result<&[ExportId], BindingError> {
        self.check(scope.query)?;
        Ok(&self.scopes[scope.index].outputs)
    }

    pub fn scan<T>(
        &mut self,
        storage: &StorageCatalog<T>,
        scope: ScopeId,
        table: TableId,
    ) -> Result<RelationId, BindingError> {
        self.check(scope.query)?;
        let exports = (0..storage.table(table).columns().len())
            .map(|index| {
                self.allocate_export(ExportOrigin::Stored(StoredColumnRef {
                    table,
                    column: ColumnId(index),
                }))
            })
            .collect();
        Ok(self.relation(scope, RelationSource::Scan(table), exports))
    }

    pub fn stored_column(
        &self,
        scope: ScopeId,
        relation: RelationId,
        column: StoredColumnRef,
    ) -> Result<ColumnRef, BindingError> {
        self.check(relation.query)?;
        let source = &self.relations[relation.index];
        if source.source != RelationSource::Scan(column.table) {
            return Err(BindingError::MissingExport);
        }
        let export = source
            .exports
            .get(column.column.index())
            .copied()
            .ok_or(BindingError::MissingExport)?;
        self.column(scope, relation, export)
    }

    pub fn derived(&mut self, scope: ScopeId, body: ScopeId) -> Result<RelationId, BindingError> {
        self.seal_child(scope, body)?;
        Ok(self.relation(
            scope,
            RelationSource::Derived(body),
            self.scopes[body.index].outputs.clone(),
        ))
    }

    pub fn define(&mut self, scope: ScopeId, body: ScopeId) -> Result<DefinitionId, BindingError> {
        self.seal_child(scope, body)?;
        let id = DefinitionId {
            query: self.query,
            index: self.definitions.len(),
        };
        self.definitions.push(Definition { scope, body });
        Ok(id)
    }

    pub fn reference(
        &mut self,
        scope: ScopeId,
        definition: DefinitionId,
    ) -> Result<RelationId, BindingError> {
        self.check(scope.query)?;
        self.check(definition.query)?;
        let declared = &self.definitions[definition.index];
        let mut current = Some(scope);
        while let Some(ancestor) = current {
            if ancestor == declared.scope {
                return Ok(self.relation(
                    scope,
                    RelationSource::Definition(definition),
                    self.scopes[declared.body.index].outputs.clone(),
                ));
            }
            current = self.scopes[ancestor.index].parent;
        }
        Err(BindingError::OutsideScope)
    }

    pub fn definition_body(&self, definition: DefinitionId) -> Result<ScopeId, BindingError> {
        self.check(definition.query)?;
        Ok(self.definitions[definition.index].body)
    }

    pub fn union(&mut self, scope: ScopeId, arms: &[ScopeId]) -> Result<RelationId, BindingError> {
        self.check(scope.query)?;
        let first = *arms.first().ok_or(BindingError::UnionShape)?;
        self.check(first.query)?;
        let width = self.scopes[first.index].outputs.len();
        for arm in arms {
            self.check(arm.query)?;
            if self.scopes[arm.index].parent != Some(scope) {
                return Err(BindingError::OutsideScope);
            }
            if self.scopes[arm.index].outputs.len() != width {
                return Err(BindingError::UnionShape);
            }
        }
        for arm in arms {
            self.scopes[arm.index].sealed = true;
        }
        Ok(self.relation(
            scope,
            RelationSource::Union(arms.to_vec()),
            self.scopes[first.index].outputs.clone(),
        ))
    }

    pub fn column(
        &self,
        scope: ScopeId,
        relation: RelationId,
        export: ExportId,
    ) -> Result<ColumnRef, BindingError> {
        self.check(scope.query)?;
        self.check(relation.query)?;
        self.check(export.query)?;
        let source = &self.relations[relation.index];
        if source.scope != scope {
            return Err(BindingError::OutsideScope);
        }
        if !source.exports.contains(&export) {
            return Err(BindingError::MissingExport);
        }
        Ok(ColumnRef { relation, export })
    }

    pub fn source(&self, relation: RelationId) -> Result<&RelationSource, BindingError> {
        self.check(relation.query)?;
        Ok(&self.relations[relation.index].source)
    }

    pub fn exports(&self, relation: RelationId) -> Result<&[ExportId], BindingError> {
        self.check(relation.query)?;
        Ok(&self.relations[relation.index].exports)
    }

    pub fn origin(&self, export: ExportId) -> Result<ExportOrigin, BindingError> {
        self.check(export.query)?;
        Ok(self.exports[export.index])
    }

    fn seal_child(&mut self, scope: ScopeId, body: ScopeId) -> Result<(), BindingError> {
        self.check(scope.query)?;
        self.check(body.query)?;
        if self.scopes[body.index].parent != Some(scope) {
            return Err(BindingError::OutsideScope);
        }
        self.scopes[body.index].sealed = true;
        Ok(())
    }

    fn allocate_export(&mut self, origin: ExportOrigin) -> ExportId {
        let id = ExportId {
            query: self.query,
            index: self.exports.len(),
        };
        self.exports.push(origin);
        id
    }

    fn relation(
        &mut self,
        scope: ScopeId,
        source: RelationSource,
        exports: Vec<ExportId>,
    ) -> RelationId {
        let id = RelationId {
            query: self.query,
            index: self.relations.len(),
        };
        self.relations.push(Relation {
            scope,
            source,
            exports,
        });
        id
    }

    fn check(&self, query: u64) -> Result<(), BindingError> {
        if query == self.query {
            Ok(())
        } else {
            Err(BindingError::ForeignQuery)
        }
    }
}
