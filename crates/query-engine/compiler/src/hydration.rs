//! A hydration request: one single-entity logical plan per entity (an id pin and the columns to
//! read), each with the caller scope its rows are read under.

use orbit_server_config::QueryLimits;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::plan::logical::{Columns, LogicalPlan, Predicate, Projection};
use query_data_model::{EntityId, Model, PropertyId};

use crate::error::Result;
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
    /// The identity keys the arm reads, or every row when none are named; a composite key is
    /// one value per identity part.
    pub keys: Option<Vec<Key>>,
    pub traversal_paths: Vec<TraversalPath>,
}

impl HydrationArm {
    pub fn new(
        model: &Model,
        limits: &QueryLimits,
        entity: EntityId,
        ids: &[Key],
        columns: &[PropertyId],
        traversal_paths: Vec<TraversalPath>,
    ) -> Result<Self> {
        let limits = QueryLimits {
            pinned_ids: limits.hydration_rows,
            in_values: limits.in_values.max(limits.hydration_rows),
            ..*limits
        };
        let catalog = model.logical(&limits);
        let identity = model.graph().entity(entity).identity.clone();
        let plan = catalog.build(|q| {
            let declared = catalog.entity(entity);
            let name = q.internal_name("hydrate")?;
            let rows = q.entities_as(declared, &name)?;
            let binding = rows.endpoint()?;
            let rows = match identity.as_slice() {
                [key] => q.pin(
                    rows,
                    binding.property(*key)?,
                    &crate::dag::keys::single(ids)?,
                )?,
                parts => {
                    let predicate = Predicate::InTuple {
                        left: parts
                            .iter()
                            .map(|part| binding.property(*part))
                            .collect::<std::result::Result<_, _>>()?,
                        values: ids
                            .iter()
                            .map(|id| crate::dag::keys::tuple(id, parts.len()))
                            .collect::<std::result::Result<_, _>>()?,
                    };
                    q.filter(rows, predicate)?
                }
            };
            let projection = Projection::Graph {
                binding: binding.output(),
                columns: Columns::Listed(columns.to_vec()),
            };
            q.select(rows, [projection.named(&*name)])
        })?;
        Ok(Self {
            plan,
            keys: Some(ids.to_vec()),
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
