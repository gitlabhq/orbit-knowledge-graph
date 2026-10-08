use query_data_model::QueryDataModel;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod api;
mod construct;
mod expr;
mod plan;
mod rewrite;
mod sql;

pub use api::{
    Aggregate, Column, Expr, ExprKind, Function, Named, Operator, Order, QueryScope, ValueType,
    array, array_concat, count, lit, singleton_if, tuple,
};

static NEXT_GRAPH: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("reference belongs to another graph or query")]
    Scope,
    #[error("unknown table or column: {0}")]
    Unknown(String),
    #[error("column is not available from this input")]
    Column,
    #[error("expression has incompatible types")]
    Type,
    #[error("aggregate must be a measure and cannot contain another aggregate")]
    Aggregate,
    #[error("a source occurrence cannot appear in both join inputs")]
    ReusedSource,
    #[error("query is unfinished or already has an owner")]
    Ownership,
    #[error("CTE is not visible in this query")]
    CteScope,
    #[error("query outputs must be nonempty and UNION arms must have matching types")]
    Outputs,
    #[error("latest-row selection needs one raw scan and its full replacement key")]
    Latest,
    #[error("replacement changes the consumer's input contract")]
    Replacement,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for crate::error::QueryError {
    fn from(error: Error) -> Self {
        Self::Lowering(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryId {
    graph: u64,
    slot: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cte {
    query: QueryId,
    scope: QueryId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Read {
    Raw,
    Current,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Join {
    Inner,
    Cross,
    Semi,
    Membership,
}

#[derive(Debug)]
pub struct Rows<'a> {
    scope: QueryId,
    lowered: bool,
    kind: OperationKind<'a>,
    columns: Vec<Column>,
    sources: HashSet<u64>,
}

#[derive(Debug)]
pub enum OperationKind<'a> {
    Scan {
        table: query_data_model::storage::StoredTableRef<'a>,
        label: Option<String>,
        read: Read,
        source: u64,
    },
    Read {
        query: QueryId,
        source: u64,
    },
    Unit,
    Filter {
        input: Box<Rows<'a>>,
        predicate: Expr,
    },
    Join {
        left: Box<Rows<'a>>,
        right: Box<Rows<'a>>,
        kind: Join,
        condition: Expr,
    },
    Select {
        input: Box<Rows<'a>>,
        values: Vec<Named>,
    },
    Aggregate {
        input: Box<Rows<'a>>,
        groups: Vec<Named>,
        measures: Vec<Named>,
    },
    Expand {
        input: Box<Rows<'a>>,
        value: Named,
    },
    Sort {
        input: Box<Rows<'a>>,
        keys: Vec<Order>,
    },
    Limit {
        input: Box<Rows<'a>>,
        count: u32,
    },
    Latest {
        input: Box<Rows<'a>>,
        keys: Vec<Column>,
        version: Column,
    },
    FirstBy {
        input: Box<Rows<'a>>,
        keys: Vec<Column>,
    },
    Union {
        arms: Vec<QueryId>,
    },
}

impl<'a> Rows<'a> {
    pub fn labeled(mut self, label: impl Into<String>) -> Result<Self> {
        let OperationKind::Scan { label: name, .. } = &mut self.kind else {
            return Err(Error::Replacement);
        };
        *name = Some(label.into());
        Ok(self)
    }

    pub fn kind(&self) -> &OperationKind<'a> {
        &self.kind
    }
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
    pub fn column(&self, name: &str) -> Result<Column> {
        let mut matches = self.columns.iter().filter(|column| column.name() == name);
        let column = matches.next().ok_or_else(|| Error::Unknown(name.into()))?;
        if matches.next().is_some() {
            return Err(Error::Column);
        }
        Ok(column.clone())
    }

    pub fn column_from(&self, label: &str, name: &str) -> Result<Column> {
        let mut found = None;
        self.walk(&mut |rows| {
            if let OperationKind::Scan {
                label: Some(source),
                ..
            } = rows.kind()
                && source == label
            {
                let column = rows.column(name)?;
                if self.columns.contains(&column) && found.replace(column).is_some() {
                    return Err(Error::Column);
                }
            }
            Ok(())
        })?;
        found.ok_or(Error::Column)
    }
    pub fn remove_filter(self) -> Result<Self> {
        match self.kind {
            OperationKind::Filter { input, .. } => Ok(*input),
            _ => Err(Error::Replacement),
        }
    }
    fn scalar(&self) -> bool {
        match &self.kind {
            OperationKind::Unit => true,
            OperationKind::Aggregate { groups, .. } => groups.is_empty(),
            OperationKind::Select { input, .. } | OperationKind::Sort { input, .. } => {
                input.scalar()
            }
            OperationKind::Limit { input, count } => *count > 0 && input.scalar(),
            _ => false,
        }
    }
    fn wrap(self, make: impl FnOnce(Box<Self>) -> OperationKind<'a>) -> Self {
        Self {
            scope: self.scope,
            lowered: self.lowered,
            columns: self.columns.clone(),
            sources: self.sources.clone(),
            kind: make(Box::new(self)),
        }
    }
}

struct Query<'a> {
    parent: Option<QueryId>,
    attached: bool,
    visible: HashSet<Cte>,
    definitions: Vec<(String, Cte)>,
    rows: Option<Rows<'a>>,
}

pub struct QueryGraph<'a, M: QueryDataModel + ?Sized> {
    catalog: &'a M,
    id: u64,
    next_source: u64,
    lowered: bool,
    queries: Vec<Query<'a>>,
}

pub struct LoweredGraph<'a, M: QueryDataModel + ?Sized>(QueryGraph<'a, M>);

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M> {
    pub fn new(catalog: &'a M) -> Self {
        Self {
            catalog,
            id: NEXT_GRAPH.fetch_add(1, Ordering::Relaxed),
            next_source: 0,
            lowered: false,
            queries: vec![],
        }
    }

    pub fn query(
        &mut self,
        build: impl FnOnce(&mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>>,
    ) -> Result<QueryId> {
        self.build_query(None, HashSet::new(), build)
    }

    pub fn catalog(&self) -> &'a M {
        self.catalog
    }
    pub fn rows(&self, id: QueryId) -> Result<&Rows<'a>> {
        self.get(id)?.rows.as_ref().ok_or(Error::Ownership)
    }

    pub fn definitions(&self, id: QueryId) -> Result<impl Iterator<Item = (&str, QueryId)>> {
        Ok(self
            .get(id)?
            .definitions
            .iter()
            .map(|(name, definition)| (name.as_str(), definition.query)))
    }

    fn build_query(
        &mut self,
        parent: Option<QueryId>,
        visible: HashSet<Cte>,
        build: impl FnOnce(&mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>>,
    ) -> Result<QueryId> {
        let id = QueryId {
            graph: self.id,
            slot: self.queries.len(),
        };
        self.queries.push(Query {
            parent,
            attached: false,
            visible,
            definitions: vec![],
            rows: None,
        });
        let scope = &mut QueryScope { graph: self, id };
        let rows = build(scope)?;
        scope.require_rows(&rows)?;
        if rows.columns.is_empty() {
            return Err(Error::Outputs);
        }
        self.queries[id.slot].rows = Some(rows);
        Ok(id)
    }

    fn get(&self, id: QueryId) -> Result<&Query<'a>> {
        if id.graph != self.id {
            return Err(Error::Scope);
        }
        self.queries.get(id.slot).ok_or(Error::Ownership)
    }

    fn source(&mut self) -> u64 {
        let source = self.next_source;
        self.next_source += 1;
        source
    }

    fn columns(
        &mut self,
        scope: QueryId,
        values: impl IntoIterator<Item = (String, ValueType)>,
    ) -> (u64, Vec<Column>) {
        let source = self.source();
        let columns = values
            .into_iter()
            .enumerate()
            .map(|(slot, (name, data_type))| {
                Column(Arc::new(expr::ColumnData {
                    scope,
                    source,
                    slot,
                    name,
                    data_type,
                }))
            })
            .collect();
        (source, columns)
    }
}
