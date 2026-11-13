//! A hydration request: one single-entity logical plan per entity (an id pin and the columns to
//! read), each with the caller scope its rows are read under.

use orbit_server_config::QueryLimits;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Model;
use query_data_model::plan::logical::{Columns, LogicalPlan, PlanError, Projection};

use crate::error::{QueryError, Result};
use orbit_authorization::Key;

#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationCompileOptions {
    pub dynamic: bool,
    pub path_segment_budget: Option<usize>,
}

#[derive(Debug)]
pub struct HydrationRequest {
    pub arms: Vec<HydrationArm>,
    pub options: HydrationCompileOptions,
}

#[derive(Debug)]
pub struct HydrationArm {
    pub plan: LogicalPlan,
    pub traversal_paths: Vec<TraversalPath>,
}

impl HydrationArm {
    pub fn new(
        model: &Model,
        limits: &QueryLimits,
        entity: &str,
        ids: &[Key],
        columns: &[String],
        traversal_paths: Vec<TraversalPath>,
    ) -> Result<Self> {
        let limits = QueryLimits {
            pinned_ids: limits.hydration_rows,
            ..*limits
        };
        let catalog = model.logical(&limits);
        let ids = ids
            .iter()
            .map(crate::logical::anchors::literal)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| QueryError::Validation("a composite key cannot pin one row".into()))?;
        let key = identity(model, model.entity(entity).map_err(PlanError::from)?)?;
        let plan = catalog.build(|q| {
            let declared = catalog.entity(entity)?;
            let name = q.internal_name("hydrate")?;
            let rows = q.entities_as(declared, &name)?;
            let binding = rows.endpoint()?;
            let id = binding.property(key)?;
            let rows = q.pin(rows, id, &ids)?;
            let entity = binding.entity().ok_or(PlanError::NoEntity)?;
            let listed = columns
                .iter()
                .map(|column| Ok(model.property(entity, column)?.id))
                .collect::<std::result::Result<_, PlanError>>()?;
            let projection = Projection::Graph {
                binding: binding.output(),
                columns: Columns::Listed(listed),
            };
            q.select(rows, [projection.named(&*name)])
        })?;
        Ok(Self {
            plan,
            traversal_paths,
        })
    }

    /// Keeps the caller's paths only on an arm whose entity's rows hold a scope key column.
    pub(crate) fn scope_on<B: crate::passes::codegen::SqlBackend>(
        &mut self,
        catalog: &query_data_model::Catalog<B>,
    ) {
        let query = crate::logical::Query::new(&self.plan, catalog.model());
        let keyed = query.bindings().into_iter().all(|binding| {
            catalog.guard(binding.entity).is_some_and(|guard| {
                matches!(guard.rule, query_data_model::catalog::Rule::Column(_))
            })
        });
        if !keyed {
            self.traversal_paths.clear();
        }
    }
}

/// The name of the one property that identifies the entity's rows.
pub(crate) fn identity(model: &Model, entity: query_data_model::EntityId) -> Result<&str> {
    let graph = model.graph();
    match graph.entity(entity).identity.as_slice() {
        [identity] => Ok(&graph.property(*identity).name),
        _ => Err(QueryError::Validation(format!(
            "{} has a composite identity, which no row read keys by",
            graph.entity(entity).name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use query_data_model::{Catalog, ClickHouse};

    #[test]
    fn every_entity_hydrates_within_the_column_cap() {
        let model = Catalog::<ClickHouse>::embedded("server").unwrap();
        let columns = crate::testkit::limits().columns;
        for entity in model.model().graph().entities() {
            assert!(
                entity.properties.len() <= columns,
                "{} has {} properties",
                entity.name,
                entity.properties.len()
            );
        }
    }
}
