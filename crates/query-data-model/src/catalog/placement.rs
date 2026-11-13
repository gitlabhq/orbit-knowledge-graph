//! Placement: the one store a read of an entity runs on, from what the read needs and what each
//! store declares it serves.

use schemars::JsonSchema;
use serde::Deserialize;

use super::{Diagnostics, Model, Store, registered};
use crate::plan::logical::Matching;
use crate::{EntityId, PropertyId, StoreId};

/// A search a store's backend runs over a property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Trigram,
    Structural,
}

/// What a read applies to a property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// The property equals a value.
    Equal,
    /// The property, alone or in a tuple, is one of a list of values.
    OneOf,
    /// The property is bounded by `<`, `<=`, `>` or `>=` a value.
    Bound,
    /// The property's text matches.
    Text(Matching),
    /// Any other condition: `<>`, a null test, a negation or a comparison with an expression.
    Other,
    Search(SearchMode),
}

impl Operation {
    const ALL: [Self; 12] = [
        Self::Equal,
        Self::OneOf,
        Self::Bound,
        Self::Text(Matching::Contains),
        Self::Text(Matching::StartsWith),
        Self::Text(Matching::EndsWith),
        Self::Text(Matching::Token),
        Self::Text(Matching::AllTokens),
        Self::Text(Matching::AnyTokens),
        Self::Other,
        Self::Search(SearchMode::Trigram),
        Self::Search(SearchMode::Structural),
    ];

    fn describe(self) -> &'static str {
        match self {
            Self::Equal => "equality",
            Self::OneOf => "a list match",
            Self::Bound => "a range bound",
            Self::Text(Matching::Contains) => "a contains match",
            Self::Text(Matching::StartsWith) => "a starts_with match",
            Self::Text(Matching::EndsWith) => "an ends_with match",
            Self::Text(Matching::Token) => "a token match",
            Self::Text(Matching::AllTokens) => "an all_tokens match",
            Self::Text(Matching::AnyTokens) => "an any_tokens match",
            Self::Other => "a condition",
            Self::Search(SearchMode::Trigram) => "a trigram search",
            Self::Search(SearchMode::Structural) => "a structural search",
        }
    }
}

/// One thing a read needs from the store it runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    Read(PropertyId),
    Apply(PropertyId, Operation),
}

/// What a read of one entity needs: the properties it reads and the operations it applies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Needs(Vec<Need>);

impl Needs {
    pub fn read(&mut self, property: PropertyId) {
        self.add(Need::Read(property));
    }

    pub fn apply(&mut self, property: PropertyId, operation: Operation) {
        self.add(Need::Apply(property, operation));
    }

    fn add(&mut self, need: Need) {
        if !self.0.contains(&need) {
            self.0.push(need);
        }
    }
}

/// Why no store serves a read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unplaced {
    #[error("entity '{entity}' is not available in this data model")]
    Unstored { entity: String },
    #[error("no store serves {need}")]
    Unserved { need: String },
    #[error("no one store of {entity} serves both {first} and {second}")]
    Split {
        entity: String,
        first: String,
        second: String,
    },
}

impl Model {
    /// The store a read of `entity` runs on: its complete store when that serves every need, else
    /// the one partial store that does. The loader rejects two partial stores that could both
    /// serve what the complete store cannot, so the answer never depends on store order.
    pub fn get_store(&self, entity: EntityId, needs: &Needs) -> Result<StoreId, Unplaced> {
        let complete = self.complete(entity).ok_or_else(|| Unplaced::Unstored {
            entity: self.graph.entity(entity).name.clone(),
        })?;
        let unmet = |store: &Store| {
            needs
                .0
                .iter()
                .copied()
                .find(|need| !self.serves(store, entity, *need))
        };
        let Some(missed) = unmet(complete) else {
            return Ok(complete.id);
        };
        let mut partial = self.partial(entity);
        if let Some(store) = partial.clone().find(|store| unmet(store).is_none()) {
            return Ok(store.id);
        }
        let describe = |need| self.describe(entity, need);
        match partial.find(|store| self.serves(store, entity, missed)) {
            Some(store) => Err(Unplaced::Split {
                entity: self.graph.entity(entity).name.clone(),
                first: describe(missed),
                second: describe(unmet(store).unwrap_or(missed)),
            }),
            None => Err(Unplaced::Unserved {
                need: describe(missed),
            }),
        }
    }

    /// The one store that holds or excludes every property of the entity.
    pub(crate) fn complete(&self, entity: EntityId) -> Option<&Store> {
        self.stores.iter().find(|store| store.complete(entity))
    }

    fn partial(&self, entity: EntityId) -> impl Iterator<Item = &Store> + Clone {
        self.stores
            .iter()
            .filter(move |store| store.source(entity).is_some() && !store.complete(entity))
    }

    /// Whether `store` serves `need` on a read of `entity`, from what it declares: the columns it
    /// lists, its backend's capabilities, its search modes and push-downs, and its accepts. A
    /// lookup serves its properties on the complete store's reads.
    fn serves(&self, store: &Store, entity: EntityId, need: Need) -> bool {
        let Some(source) = store.source(entity) else {
            return false;
        };
        let looked_up = |property| source.complete && self.lookups.get(property).is_some();
        let capabilities = self.connection(store.connection()).capabilities();
        match need {
            Need::Read(property) => store.column(property).is_some() || looked_up(property),
            Need::Apply(property, operation @ Operation::Search(_)) => {
                capabilities.search && store.pushes_down(entity, property, operation)
            }
            Need::Apply(property, operation) => {
                looked_up(property)
                    || store.column(property).is_some()
                        && (capabilities.joins
                            || matches!(operation, Operation::Equal | Operation::OneOf)
                                && store.accepts(entity, property)
                            || store.pushes_down(entity, property, operation))
            }
        }
    }

    fn describe(&self, entity: EntityId, need: Need) -> String {
        let name = |property: PropertyId| {
            format!(
                "{}.{}",
                self.graph.entity(entity).name,
                self.graph.property(property).name
            )
        };
        match need {
            Need::Read(property) => format!("a read of {}", name(property)),
            Need::Apply(property, operation) => {
                format!("{} on {}", operation.describe(), name(property))
            }
        }
    }

    /// No two partial stores of an entity both serve a need its complete store does not, so
    /// placement never picks between them.
    pub(super) fn check_placement(&self, diagnostics: &mut Diagnostics) {
        for entity in self.graph.entities() {
            let Some(complete) = self.complete(entity.id) else {
                continue;
            };
            let partial: Vec<&Store> = self.partial(entity.id).collect();
            let needs = entity.properties.iter().flat_map(|property| {
                std::iter::once(Need::Read(*property)).chain(
                    Operation::ALL
                        .iter()
                        .map(|operation| Need::Apply(*property, *operation)),
                )
            });
            for need in needs {
                if self.serves(complete, entity.id, need) {
                    continue;
                }
                let mut serving = partial
                    .iter()
                    .filter(|store| self.serves(store, entity.id, need));
                if let (Some(first), Some(second)) = (serving.next(), serving.next()) {
                    diagnostics.error(
                        &second.package,
                        format!(
                            "stores {} and {} both serve {}, which the complete store {} does not; placement cannot choose between them",
                            first.name,
                            second.name,
                            self.describe(entity.id, need),
                            complete.name
                        ),
                    );
                    break;
                }
            }
        }
    }
}

impl Store {
    /// Whether the store's backend runs `operation` on `property` of `entity` itself.
    fn pushes_down(&self, entity: EntityId, property: PropertyId, operation: Operation) -> bool {
        let (Some(facts), Some(backend)) = (&self.facts, registered(&self.kind)) else {
            return false;
        };
        backend.serves(facts, entity, property, operation)
    }
}
