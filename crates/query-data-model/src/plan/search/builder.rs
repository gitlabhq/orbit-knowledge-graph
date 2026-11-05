use super::{FieldType, Index, Ranking, SearchError, SearchLanguage, SearchOperation, SearchPlan};
use crate::catalog::Catalog;
use crate::implementations::search::Search;
use crate::plan::tracking::{self, Construction, Declare, Declared, Named, Output, Outputs, Ref};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    output: Ref,
    value: FieldType,
}

impl Field {
    pub fn named(self, name: impl Into<String>) -> Named<Self> {
        Named {
            item: self,
            name: name.into(),
        }
    }
}

#[derive(Debug)]
pub struct Matches {
    rows: tracking::Rows,
    fields: Vec<(String, Field)>,
}

impl Matches {
    pub fn field(&self, name: &str) -> Result<Field, SearchError> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, field)| *field)
            .ok_or_else(|| SearchError::UnknownField {
                name: name.to_string(),
                available: self.fields.iter().map(|(name, _)| name.clone()).collect(),
            })
    }

    fn ranked(&self) -> bool {
        self.fields
            .iter()
            .any(|(_, field)| field.value == FieldType::Score)
    }
}

pub struct SearchBuilder<'c> {
    catalog: &'c Catalog<Search>,
    construction: Construction<SearchLanguage>,
}

impl<'c> SearchBuilder<'c> {
    pub(super) fn new(
        catalog: &'c Catalog<Search>,
        construction: Construction<SearchLanguage>,
    ) -> Self {
        Self {
            catalog,
            construction,
        }
    }

    pub(super) fn finish(
        self,
        root: Result<Matches, SearchError>,
    ) -> Result<SearchPlan, SearchError> {
        Ok(self.construction.finish(root?.rows)?)
    }

    fn connect(
        &mut self,
        operation: SearchOperation,
        inputs: Vec<Matches>,
        check: impl FnOnce(&[Matches]) -> Result<Outputs<FieldType>, SearchError>,
    ) -> Result<Matches, SearchError> {
        self.construction.attempt(|construction| {
            let outputs = check(&inputs)?;
            let inputs = inputs.into_iter().map(|matches| matches.rows).collect();
            let rows = construction.connect(operation, inputs, outputs)?.rows;
            let mut fields = Vec::new();
            for output in construction.outputs(&rows)? {
                if let (Some(name), value) = construction.value(output)? {
                    let value = *value;
                    fields.push((name.to_string(), Field { output, value }));
                }
            }
            Ok(Matches { rows, fields })
        })
    }

    pub fn search(&mut self, index: Declared<Index>, text: &str) -> Result<Matches, SearchError> {
        let index = self
            .construction
            .attempt(|construction| construction.declared(index))?;
        let catalog = self.catalog;
        let operation = SearchOperation::Search {
            index,
            text: text.to_string(),
        };
        self.connect(operation, Vec::new(), |_| {
            let store = &catalog.model().stores()[index.store];
            let facts = catalog
                .backend(&store.name)
                .filter(|_| store.connection == catalog.connection())
                .ok_or_else(|| SearchError::OtherConnection(store.name.clone()))?;
            let graph = catalog.graph();
            let fields: Vec<Declare<FieldType>> = graph
                .entity(index.entity)
                .properties
                .iter()
                .filter(|property| store.column(**property).is_some())
                .map(|property| {
                    let value = match facts.is_text(*property) {
                        true => FieldType::Text,
                        false => FieldType::Keyword(graph.property(*property).data_type),
                    };
                    Declare::named(&graph.property(*property).name, value)
                })
                .collect();
            if !fields.iter().any(|field| field.value == FieldType::Text) {
                return Err(SearchError::NoText(store.name.clone()));
            }
            Ok(Outputs::extend(fields))
        })
    }

    pub fn rank(&mut self, matches: Matches, ranking: Ranking) -> Result<Matches, SearchError> {
        self.connect(SearchOperation::Rank(ranking), vec![matches], |inputs| {
            if inputs.iter().any(Matches::ranked) {
                return Err(SearchError::Ranked);
            }
            Ok(Outputs::extend([Declare::named("score", FieldType::Score)]))
        })
    }

    pub fn top(&mut self, matches: Matches, count: u64) -> Result<Matches, SearchError> {
        self.connect(
            SearchOperation::Top(count),
            vec![matches],
            |inputs| match inputs.iter().all(Matches::ranked) {
                true => Ok(Outputs::preserve()),
                false => Err(SearchError::Unranked),
            },
        )
    }

    pub fn select(
        &mut self,
        matches: Matches,
        fields: impl IntoIterator<Item = Named<Field>>,
    ) -> Result<Matches, SearchError> {
        let (fields, outputs): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .map(|named| {
                let declare = Declare::named(named.name, named.item.value);
                (named.item.output, Output::Declare(declare))
            })
            .unzip();
        self.connect(SearchOperation::Select(fields), vec![matches], |_| {
            Ok(Outputs::replace(outputs))
        })
    }
}
