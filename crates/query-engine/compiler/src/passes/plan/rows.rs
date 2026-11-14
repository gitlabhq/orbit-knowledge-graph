//! The planner's relations: every relation a later step may read again is a share, so the planner
//! states what is reused and `Catalog::materialize` decides how.

use query_data_model::Catalog;
use query_data_model::constants::TRAVERSAL_PATH_COLUMN;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    JoinKind, Relation, RelationalBuilder, RelationalError, RelationalPlan, Subquery,
};

use super::predicates::{Column, Predicate, Projection};
use crate::error::{QueryError, Result};
use crate::passes::plan::build::predicates::Lowering;
use crate::passes::plan::build::{and, column, computed, eq, stored};
use crate::tp_bounds::{TpBound, TpBounds};

#[derive(Clone, PartialEq)]
pub struct Rel {
    pub rows: Subquery,
    pub alias: Option<String>,
    pub lookup: bool,
}

/// Rows pinned by unique keys: every selective predicate is a point lookup, which costs less to
/// repeat at each read than to materialize. `None` when no predicate is selective.
pub(super) fn point_lookup(predicates: &[Predicate], id_range_span: i64) -> Option<bool> {
    let mut selective = predicates
        .iter()
        .filter(|p| p.is_selective(id_range_span))
        .peekable();
    selective.peek()?;
    Some(selective.all(Predicate::is_point_lookup))
}

#[derive(Clone)]
pub struct BindingSource {
    pub node: String,
    pub alias: String,
    /// The columns of `alias` that carry each part of the node's identity.
    pub key: Vec<String>,
    /// The node's identity columns, which `key` carries.
    pub identity: Vec<String>,
    pub property_source: Option<String>,
    pub traversal_path: Option<Column>,
}

impl BindingSource {
    pub(super) fn column(&self, property: &str) -> Option<Column> {
        if property == TRAVERSAL_PATH_COLUMN {
            return self.traversal_path.clone();
        }
        match self.identity.iter().position(|part| part == property) {
            Some(part) => Some(Column::new(&self.alias, &self.key[part])),
            None => self
                .property_source
                .as_ref()
                .map(|source| Column::new(source, property)),
        }
    }

    /// The one column carrying the node's identity, for a join that keys the node by one column.
    pub(super) fn id(&self) -> Result<Column> {
        match self.key.as_slice() {
            [column] => Ok(Column::new(&self.alias, column)),
            _ => Err(QueryError::PipelineInvariant(format!(
                "node '{}' has a composite identity in a single-key slot, which the loader rejects",
                self.node
            ))),
        }
    }

    pub(super) fn table(alias: &str, identity: &[String]) -> Self {
        Self {
            node: alias.into(),
            alias: alias.into(),
            key: identity.to_vec(),
            identity: identity.to_vec(),
            property_source: Some(alias.into()),
            traversal_path: Some(Column::new(alias, TRAVERSAL_PATH_COLUMN)),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum ReadScope {
    Unscoped,
    Scoped,
    HighFanOut,
    Edge,
}

impl ReadScope {
    pub fn reads_final(self, predicates: &[Predicate]) -> bool {
        match self {
            Self::Unscoped | Self::Edge => false,
            Self::Scoped => true,
            Self::HighFanOut => !predicates.iter().any(Predicate::narrows_keys),
        }
    }
}

/// Builds a plan whose construction can fail with planner errors as well as builder rejections.
pub(crate) fn build<B: RelationalBackend>(
    catalog: &Catalog<B>,
    construct: impl FnOnce(&mut RelationalBuilder<'_, B>) -> Result<Relation>,
) -> Result<RelationalPlan<B>> {
    let mut failure = None;
    let plan = catalog.build(|q| {
        construct(q).map_err(|error| {
            failure = Some(error);
            RelationalError::Incomplete("a planned query")
        })
    });
    match failure {
        Some(error) => Err(error),
        None => plan.map_err(QueryError::from),
    }
}

pub(crate) struct Reads<'q, 'c, B: RelationalBackend> {
    pub q: &'q mut RelationalBuilder<'c, B>,
    tp_bounds: Option<&'q TpBounds>,
    pub(super) selective_latest: bool,
    pub(super) id_range_span: i64,
    pub(super) memo: Vec<(super::current::ReadKey, Rel)>,
}

impl<'q, 'c, B: RelationalBackend> Reads<'q, 'c, B> {
    pub fn new(
        q: &'q mut RelationalBuilder<'c, B>,
        tp_bounds: Option<&'q TpBounds>,
        selective_latest: bool,
        id_range_span: i64,
    ) -> Self {
        Self {
            q,
            tp_bounds,
            selective_latest,
            id_range_span,
            memo: Vec::new(),
        }
    }

    pub fn node_bound(&self, alias: &str) -> Option<&'q TpBound> {
        self.tp_bounds.and_then(|bounds| bounds.node(alias))
    }

    pub fn relationship_bound(&self, index: usize) -> Option<&'q TpBound> {
        self.tp_bounds.and_then(|bounds| bounds.relationship(index))
    }

    pub fn subquery(
        &mut self,
        name: &str,
        build: impl FnOnce(&mut Reads<'_, 'c, B>) -> Result<Relation>,
    ) -> Result<Subquery> {
        let mut failure = None;
        let (tp_bounds, selective_latest) = (self.tp_bounds, self.selective_latest);
        let span = self.id_range_span;
        let body = self.q.subquery(name, |q| {
            build(&mut Reads::new(q, tp_bounds, selective_latest, span)).map_err(|error| {
                failure = Some(error);
                RelationalError::Incomplete("a planned subquery")
            })
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(body?),
        }
    }

    pub fn read(&mut self, rel: &Rel, alias: Option<&str>) -> Result<Relation> {
        Ok(self.q.read(&rel.rows, alias.or(rel.alias.as_deref()))?)
    }

    /// One filter per predicate, so a reader that inlines the rows keeps each as its own conjunct.
    pub fn filter(&mut self, mut rows: Relation, predicates: &[Predicate]) -> Result<Relation> {
        for predicate in predicates {
            if stored::<B>() && matches!(predicate, Predicate::Live { .. }) {
                continue;
            }
            let lowered = Lowering { scope: &[&rows] }.predicate(self.q, predicate)?;
            rows = self.q.filter(rows, lowered)?;
        }
        Ok(rows)
    }

    pub fn bound(
        &mut self,
        rows: Relation,
        bound: Option<&TpBound>,
        alias: &str,
    ) -> Result<Relation> {
        let Some(bound) = bound else {
            return Ok(rows);
        };
        let lowered = crate::tp_bounds::bound(self.q, &[&rows], bound, alias)?;
        Ok(self.q.filter(rows, lowered)?)
    }

    pub fn share(&mut self, rows: Relation, alias: Option<&str>, lookup: bool) -> Result<Rel> {
        let rows = self.q.select_all(rows)?;
        self.shared(rows, alias, lookup)
    }

    pub fn shared(&mut self, rows: Relation, alias: Option<&str>, lookup: bool) -> Result<Rel> {
        let rows = match lookup {
            true => self.q.share_lookup(rows)?,
            false => self.q.share(rows)?,
        };
        Ok(Rel {
            rows,
            alias: alias.map(str::to_string),
            lookup,
        })
    }

    pub fn filtered(&mut self, rel: &Rel, predicates: &[Predicate]) -> Result<Rel> {
        if predicates.is_empty() {
            return Ok(rel.clone());
        }
        let rows = self.read(rel, None)?;
        let rows = self.filter(rows, predicates)?;
        let lookup = point_lookup(predicates, self.id_range_span).unwrap_or(rel.lookup);
        self.share(rows, rel.alias.as_deref(), lookup)
    }

    pub fn select(&mut self, rows: Relation, outputs: &[Projection]) -> Result<Relation> {
        let items = Lowering { scope: &[&rows] }.projections::<B>(outputs)?;
        let (rows, columns) = computed(self.q, rows, items)?;
        Ok(self.q.select(rows, columns)?)
    }

    pub fn project(
        &mut self,
        rel: &Rel,
        alias: Option<&str>,
        outputs: &[Projection],
    ) -> Result<Rel> {
        let rows = self.read(rel, alias)?;
        let rows = self.select(rows, outputs)?;
        self.shared(rows, None, false)
    }

    pub fn join(
        &mut self,
        left: Relation,
        right: Relation,
        (from, to): (Column, Column),
        predicates: &[Predicate],
    ) -> Result<Relation> {
        let lowering = Lowering {
            scope: &[&left, &right],
        };
        let mut on = eq(column(lowering.scope, &from)?, column(lowering.scope, &to)?);
        for predicate in predicates {
            on = and(on, lowering.predicate(self.q, predicate)?);
        }
        Ok(self.q.join(left, right, JoinKind::Inner(on))?)
    }

    /// The arms united in their own block under `alias`.
    pub fn union(
        &mut self,
        alias: &str,
        arms: impl FnOnce(&mut Reads<'_, 'c, B>) -> Result<Vec<Relation>>,
    ) -> Result<Rel> {
        let united = self.subquery(alias, |reads| {
            let arms = arms(reads)?;
            Ok(reads.q.union(arms)?)
        })?;
        let rows = self.q.read(&united, Some(alias))?;
        self.share(rows, Some(alias), false)
    }
}
