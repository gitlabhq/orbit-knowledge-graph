//! The Combine plan language: the query engine's own physical plan over the rows earlier stages
//! of an execution DAG returned. It joins them, then applies the result columns, order and limit.

use crate::DataType;
use crate::catalog::ModelId;
use crate::plan::logical::Order;
use crate::plan::tracking::{
    self, Construction, Declare, Each, Language, Output, Outputs, Plan, Rebuilder, Ref, Rejected,
    Step,
};

#[derive(Debug)]
pub struct CombineLanguage;

pub type CombinePlan = Plan<CombineLanguage>;

/// A column of a stage's output contract: the stage, and the column's position in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Position {
    pub stage: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CombineOperation {
    /// The rows one stage returned, one output per listed contract position.
    Rows {
        stage: usize,
        columns: Vec<usize>,
    },
    /// The pairs of left and right rows whose columns are equal on every pair, each pair read as
    /// keys of its declared type.
    Join {
        on: Vec<(Ref, Ref, DataType)>,
    },
    Select(Vec<Ref>),
    Order(Vec<(Ref, Order)>),
    Limit {
        rows: u64,
        skip: u64,
    },
    /// Constant text columns, each a name and the value every row holds.
    Tag(Vec<(String, String)>),
    /// Text columns that each hold a composite key, built from its parts read as their types.
    Composite(Vec<Vec<(Ref, DataType)>>),
    /// Text columns cut to `chars` characters, with `suffix` after any that were cut.
    Excerpt {
        columns: Vec<Ref>,
        chars: u32,
        suffix: String,
    },
}

impl Language for CombineLanguage {
    type Operation = CombineOperation;
    type Value = Option<Position>;

    fn references(operation: &mut CombineOperation, visit: &mut dyn FnMut(&mut Ref)) {
        match operation {
            CombineOperation::Join { on } => on.iter_mut().for_each(|(left, right, _)| {
                visit(left);
                visit(right);
            }),
            CombineOperation::Select(columns) => columns.iter_mut().for_each(visit),
            CombineOperation::Order(keys) => keys.iter_mut().for_each(|(key, _)| visit(key)),
            CombineOperation::Excerpt { columns, .. } => columns.iter_mut().for_each(visit),
            CombineOperation::Composite(keys) => {
                keys.iter_mut().flatten().for_each(|(part, _)| visit(part))
            }
            CombineOperation::Rows { .. }
            | CombineOperation::Limit { .. }
            | CombineOperation::Tag(_) => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CombineError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("the rows read no column {} of stage {}", .0.column, .0.stage)]
    UnknownColumn(Position),
    #[error("the rows hold no result column {0}")]
    UnknownOutput(String),
    #[error("a join matches on at least one column pair")]
    EmptyJoin,
}

/// Rows of the plan under construction.
#[derive(Debug)]
pub struct Rel {
    rows: tracking::Rows,
}

pub struct CombineBuilder {
    construction: Construction<CombineLanguage>,
}

impl CombineBuilder {
    pub fn build(
        snapshot: ModelId,
        construct: impl FnOnce(&mut Self) -> Result<Rel, CombineError>,
    ) -> Result<CombinePlan, CombineError> {
        let mut builder = Self {
            construction: Construction::new(snapshot),
        };
        let root = construct(&mut builder);
        builder.finish(root)
    }

    /// Replays `plan`; `visit` keeps, replaces or extends each operation.
    pub fn rebuild(
        snapshot: ModelId,
        plan: &CombinePlan,
        visit: impl FnMut(&mut Self, &mut Step<'_, '_, Self>) -> Result<Rel, CombineError>,
    ) -> Result<CombinePlan, CombineError> {
        let mut builder = Self {
            construction: Construction::new(snapshot),
        };
        let root = tracking::rebuild(&mut builder, plan, &mut Each(visit));
        builder.finish(root)
    }

    fn finish(self, root: Result<Rel, CombineError>) -> Result<CombinePlan, CombineError> {
        Ok(self.construction.finish(root?.rows)?)
    }

    /// The output that holds the stage column at `position` in `rel`.
    pub fn column(&self, rel: &Rel, position: Position) -> Result<Ref, CombineError> {
        self.construction
            .outputs(&rel.rows)?
            .into_iter()
            .find(|output| {
                self.construction
                    .value(*output)
                    .is_ok_and(|(_, value)| *value == Some(position))
            })
            .ok_or(CombineError::UnknownColumn(position))
    }

    pub fn rows(&mut self, stage: usize, columns: &[usize]) -> Result<Rel, CombineError> {
        let declared = columns.iter().map(|column| {
            let position = Position {
                stage,
                column: *column,
            };
            Declare::named(format!("{stage}.{column}"), Some(position))
        });
        self.connect(
            CombineOperation::Rows {
                stage,
                columns: columns.to_vec(),
            },
            Vec::new(),
            Outputs::extend(declared),
        )
    }

    pub fn join(
        &mut self,
        left: Rel,
        right: Rel,
        on: Vec<(Ref, Ref, DataType)>,
    ) -> Result<Rel, CombineError> {
        if on.is_empty() {
            return Err(CombineError::EmptyJoin);
        }
        let mut kept = self.construction.outputs(&left.rows)?;
        kept.extend(self.construction.outputs(&right.rows)?);
        let kept = kept.into_iter().map(Output::Keep);
        self.connect(
            CombineOperation::Join { on },
            vec![left, right],
            Outputs::replace(kept),
        )
    }

    pub fn select(&mut self, rel: Rel, columns: Vec<(String, Ref)>) -> Result<Rel, CombineError> {
        let refs = columns.iter().map(|(_, output)| *output).collect();
        let declared = columns
            .into_iter()
            .map(|(name, _)| Output::Declare(Declare::named(name, None)));
        self.connect(
            CombineOperation::Select(refs),
            vec![rel],
            Outputs::replace(declared),
        )
    }

    pub fn order(&mut self, rel: Rel, keys: Vec<(Ref, Order)>) -> Result<Rel, CombineError> {
        self.connect(
            CombineOperation::Order(keys),
            vec![rel],
            Outputs::preserve(),
        )
    }

    pub fn tag(&mut self, rel: Rel, columns: Vec<(String, String)>) -> Result<Rel, CombineError> {
        let declared: Vec<_> = columns
            .iter()
            .map(|(name, _)| Declare::named(name, None))
            .collect();
        self.connect(
            CombineOperation::Tag(columns),
            vec![rel],
            Outputs::extend(declared),
        )
    }

    /// One text column per named composite key, from its parts.
    pub fn composite(
        &mut self,
        rel: Rel,
        keys: Vec<(String, Vec<(Ref, DataType)>)>,
    ) -> Result<Rel, CombineError> {
        let declared: Vec<_> = keys
            .iter()
            .map(|(name, _)| Declare::named(name, None))
            .collect();
        let parts = keys.into_iter().map(|(_, parts)| parts).collect();
        self.connect(
            CombineOperation::Composite(parts),
            vec![rel],
            Outputs::extend(declared),
        )
    }

    pub fn excerpt(
        &mut self,
        rel: Rel,
        columns: Vec<Ref>,
        chars: u32,
        suffix: &str,
    ) -> Result<Rel, CombineError> {
        let operation = CombineOperation::Excerpt {
            columns,
            chars,
            suffix: suffix.to_string(),
        };
        self.connect(operation, vec![rel], Outputs::preserve())
    }

    /// The output of `rel` declared `name`, as the result columns are.
    pub fn output(&self, rel: &Rel, name: &str) -> Result<Ref, CombineError> {
        self.construction
            .outputs(&rel.rows)?
            .into_iter()
            .find(|output| {
                self.construction
                    .value(*output)
                    .is_ok_and(|(declared, _)| declared == Some(name))
            })
            .ok_or_else(|| CombineError::UnknownOutput(name.to_string()))
    }

    pub fn limit(&mut self, rel: Rel, rows: u64, skip: u64) -> Result<Rel, CombineError> {
        let operation = CombineOperation::Limit { rows, skip };
        self.connect(operation, vec![rel], Outputs::preserve())
    }

    fn connect(
        &mut self,
        operation: CombineOperation,
        inputs: Vec<Rel>,
        outputs: Outputs<Option<Position>>,
    ) -> Result<Rel, CombineError> {
        let inputs = inputs.into_iter().map(|rel| rel.rows).collect();
        let connected = self.construction.connect(operation, inputs, outputs)?;
        Ok(Rel {
            rows: connected.rows,
        })
    }
}

impl Rebuilder for CombineBuilder {
    type Language = CombineLanguage;
    type Old = CombinePlan;
    type Rows = Rel;
    type Error = CombineError;

    fn plan(old: &CombinePlan) -> &CombinePlan {
        old
    }

    fn construction(&mut self) -> &mut Construction<CombineLanguage> {
        &mut self.construction
    }

    fn rows(rows: &Rel) -> &tracking::Rows {
        &rows.rows
    }

    fn apply(
        &mut self,
        operation: CombineOperation,
        inputs: Vec<Rel>,
        outputs: Outputs<Option<Position>>,
    ) -> Result<Rel, CombineError> {
        self.connect(operation, inputs, outputs)
    }
}
