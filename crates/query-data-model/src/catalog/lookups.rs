use super::Diagnostics;
pub use crate::spec::Lookup;
use crate::spec::ServiceFragment;
use crate::{GraphCatalog, PropertyId};

#[derive(Debug, Default)]
pub struct Lookups {
    lookups: Vec<Lookup>,
    by_property: Vec<Option<usize>>,
}

impl Lookups {
    pub fn get(&self, property: PropertyId) -> Option<&Lookup> {
        self.by_property[property.index()].map(|index| &self.lookups[index])
    }

    pub fn iter(&self) -> impl Iterator<Item = &Lookup> {
        self.lookups.iter()
    }

    pub(super) fn served(&self) -> impl Iterator<Item = PropertyId> + '_ {
        self.lookups
            .iter()
            .flat_map(|lookup| lookup.provides.iter().copied())
    }

    pub(super) fn resolve(
        graph: &GraphCatalog,
        fragments: &[&ServiceFragment],
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut lookups = Self {
            lookups: Vec::new(),
            by_property: vec![None; graph.properties().count()],
        };
        for fragment in fragments {
            lookups.declare(graph, fragment, diagnostics);
        }
        lookups
    }

    fn declare(
        &mut self,
        graph: &GraphCatalog,
        fragment: &ServiceFragment,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.lookups {
            let Some(entity) = diagnostics.require(graph.entity_id(&spec.entity), package, || {
                format!("lookup {name} names unknown entity {}", spec.entity)
            }) else {
                continue;
            };
            let mut properties = |names: &[String]| -> Vec<PropertyId> {
                names
                    .iter()
                    .filter_map(|property| {
                        diagnostics.require(graph.property_id(entity, property), package, || {
                            format!(
                                "lookup {name} names unknown property {}.{property}",
                                spec.entity
                            )
                        })
                    })
                    .collect()
            };
            let provides = properties(&spec.provides);
            let keys = properties(&spec.keys);
            if let Some(shared) = provides.iter().find(|property| keys.contains(property)) {
                let shared = &graph.property(*shared).name;
                diagnostics.error(
                    package,
                    format!("lookup {name} uses {shared} as both key and output"),
                );
            }
            let index = self.lookups.len();
            for property in &provides {
                if self.by_property[property.index()].replace(index).is_some() {
                    let property = &graph.property(*property).name;
                    diagnostics.error(
                        package,
                        format!("{}.{property} has two lookups", spec.entity),
                    );
                }
            }
            self.lookups.push(Lookup {
                service: fragment.service.clone(),
                name: name.clone(),
                operation: spec.operation.clone().unwrap_or_else(|| name.clone()),
                entity,
                provides,
                keys,
                operators: spec.operators.clone(),
                disabled: spec.disabled,
            });
        }
    }
}
