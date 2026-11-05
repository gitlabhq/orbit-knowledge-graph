use query_data_model::implementations::relational::plan::{Column, Relation, RelationalError};

use crate::ast;

pub(super) type Binding = (ast::Expr, Column);

pub(super) struct Scope<'r> {
    pub(super) relations: Vec<&'r Relation>,
    pub(super) groups: &'r [Binding],
    pub(super) measures: &'r [Binding],
    pub(super) lambdas: Vec<String>,
}

impl<'r> Scope<'r> {
    pub(super) fn new(relations: Vec<&'r Relation>) -> Self {
        Self {
            relations,
            groups: &[],
            measures: &[],
            lambdas: Vec::new(),
        }
    }

    pub(super) fn aggregated(
        relation: &'r Relation,
        groups: &'r [Binding],
        measures: &'r [Binding],
    ) -> Self {
        Self {
            relations: vec![relation],
            groups,
            measures,
            lambdas: Vec::new(),
        }
    }

    pub(super) fn qualified(&self, range: &str, name: &str) -> Result<Column, RelationalError> {
        self.first(|relation| relation.qualified(range, name))
    }

    pub(super) fn named(&self, name: &str) -> Result<Column, RelationalError> {
        self.first(|relation| relation.column(name))
    }

    pub(super) fn bound(bindings: &[Binding], expr: &ast::Expr) -> Option<Column> {
        bindings
            .iter()
            .find(|(bound, _)| bound == expr)
            .map(|(_, column)| *column)
    }

    fn first(
        &self,
        find: impl Fn(&Relation) -> Result<Column, RelationalError>,
    ) -> Result<Column, RelationalError> {
        let mut last = Err(RelationalError::Incomplete("a column lookup"));
        for relation in &self.relations {
            last = find(relation);
            if last.is_ok() {
                return last;
            }
        }
        last
    }
}
