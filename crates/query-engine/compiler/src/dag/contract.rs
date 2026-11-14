//! The rows a stage that feeds Combine returns: an ordered list of typed columns, each a part of
//! a node, an edge or a match. The stage's planner projects exactly these columns in this order,
//! and its executor decodes the wire rows into them.

use query_data_model::{DataType, EdgeField, PropertyId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contract {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub output: Output,
    pub data_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Node {
        binding: String,
        handle: Handle,
    },
    /// A field of the edge the stage's `step`-th relationship step follows.
    Edge {
        step: usize,
        field: EdgeField,
    },
    /// The nodes a variable-length step passes through.
    Path {
        step: usize,
    },
    Line,
    Capture(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handle {
    /// The part at this index of the entity's identity.
    Identity(usize),
    Property(PropertyId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Scalar(DataType),
    Path,
}

impl Contract {
    pub fn position(&self, output: &Output) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.output == *output)
    }

    /// The position of `output`, added at the end when the contract lacks it.
    pub(super) fn push(&mut self, output: Output, data_type: ColumnType) -> usize {
        self.position(&output).unwrap_or_else(|| {
            self.columns.push(Column { output, data_type });
            self.columns.len() - 1
        })
    }
}
