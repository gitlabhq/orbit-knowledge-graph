use crate::DataType;

use super::stage::Stage;
use super::{Expr, RelationalError};
use crate::plan::tracking::{Ref, Rows};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    pub(super) output: Ref,
    pub(super) data_type: Option<DataType>,
}

impl Column {
    pub fn expr(self) -> Expr {
        Expr::Column(self.output)
    }

    pub fn output(self) -> Ref {
        self.output
    }

    pub fn data_type(self) -> Option<DataType> {
        self.data_type
    }
}

#[derive(Debug, Clone)]
pub(super) struct Visible {
    pub(super) range: Option<String>,
    pub(super) name: String,
    pub(super) column: Column,
}

#[derive(Debug)]
pub struct Relation {
    pub(super) rows: Rows,
    pub(super) stage: Stage,
    pub(super) visible: Vec<Visible>,
    pub(super) computed: Vec<(Ref, Option<String>)>,
}

impl Relation {
    pub fn column(&self, name: &str) -> Result<Column, RelationalError> {
        self.find(name, |visible| {
            visible.name == name && visible.range.is_none()
        })
        .or_else(|_| self.find(name, |visible| visible.name == name))
    }

    pub fn qualified(&self, range: &str, name: &str) -> Result<Column, RelationalError> {
        self.find(&format!("{range}.{name}"), |visible| {
            visible.name == name && visible.range.as_deref() == Some(range)
        })
    }

    fn find(
        &self,
        name: &str,
        matches: impl Fn(&Visible) -> bool,
    ) -> Result<Column, RelationalError> {
        let mut found = self.visible.iter().filter(|visible| matches(visible));
        match (found.next(), found.next()) {
            (Some(visible), None) => Ok(visible.column),
            (Some(_), Some(_)) => Err(RelationalError::Ambiguous(name.to_string())),
            (None, _) => Err(RelationalError::UnknownColumn {
                name: name.to_string(),
                available: self.visible.iter().map(Visible::describe).collect(),
            }),
        }
    }
}

impl Visible {
    fn describe(&self) -> String {
        match &self.range {
            Some(range) => format!("{range}.{}", self.name),
            None => self.name.clone(),
        }
    }
}
