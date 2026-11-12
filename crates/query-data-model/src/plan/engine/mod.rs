//! The engine plan language: the query engine's own physical plan over the rows earlier stages
//! of an execution DAG returned. It joins them, then applies the result columns, order and limit.

use crate::DataType;
use crate::catalog::ModelId;
use crate::plan::logical::Order;
use crate::plan::tracking::{
    self, Construction, Declare, Each, Language, Output, Outputs, Plan, Rebuilder, Ref, Rejected,
    Step,
};

#[derive(Debug)]
pub struct EngineLanguage;

pub type EnginePlan = Plan<EngineLanguage>;

#[derive(Debug, Clone, PartialEq)]
pub enum EngineOperation {
    /// The rows one stage returned, one output per column.
    Rows {
        stage: usize,
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
    /// Text columns cut to `chars` characters, with `suffix` after any that were cut.
    Excerpt {
        columns: Vec<Ref>,
        chars: u32,
        suffix: String,
    },
}

impl Language for EngineLanguage {
    type Operation = EngineOperation;
    type Value = ();

    fn references(operation: &mut EngineOperation, visit: &mut dyn FnMut(&mut Ref)) {
        match operation {
            EngineOperation::Join { on } => on.iter_mut().for_each(|(left, right, _)| {
                visit(left);
                visit(right);
            }),
            EngineOperation::Select(columns) => columns.iter_mut().for_each(visit),
            EngineOperation::Order(keys) => keys.iter_mut().for_each(|(key, _)| visit(key)),
            EngineOperation::Excerpt { columns, .. } => columns.iter_mut().for_each(visit),
            EngineOperation::Rows { .. }
            | EngineOperation::Limit { .. }
            | EngineOperation::Tag(_) => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Rejected(#[from] Rejected),
    #[error("stage {stage} returns no column {column}")]
    UnknownColumn { stage: usize, column: String },
    #[error("a join matches on at least one column pair")]
    EmptyJoin,
}

/// The name a stage's column carries in the engine plan.
pub fn qualified(stage: usize, column: &str) -> String {
    format!("{stage}.{column}")
}

/// Rows of the plan under construction.
#[derive(Debug)]
pub struct Rel {
    rows: tracking::Rows,
}

pub struct EngineBuilder {
    construction: Construction<EngineLanguage>,
}

impl EngineBuilder {
    pub fn build(
        snapshot: ModelId,
        construct: impl FnOnce(&mut Self) -> Result<Rel, EngineError>,
    ) -> Result<EnginePlan, EngineError> {
        let mut builder = Self {
            construction: Construction::new(snapshot),
        };
        let root = construct(&mut builder);
        builder.finish(root)
    }

    /// Replays `plan`; `visit` keeps, replaces or extends each operation.
    pub fn rebuild(
        snapshot: ModelId,
        plan: &EnginePlan,
        visit: impl FnMut(&mut Self, &mut Step<'_, '_, Self>) -> Result<Rel, EngineError>,
    ) -> Result<EnginePlan, EngineError> {
        let mut builder = Self {
            construction: Construction::new(snapshot),
        };
        let root = tracking::rebuild(&mut builder, plan, &mut Each(visit));
        builder.finish(root)
    }

    fn finish(self, root: Result<Rel, EngineError>) -> Result<EnginePlan, EngineError> {
        Ok(self.construction.finish(root?.rows)?)
    }

    /// The output that holds `column` of `stage` in `rel`.
    pub fn column(&self, rel: &Rel, stage: usize, column: &str) -> Result<Ref, EngineError> {
        let wanted = qualified(stage, column);
        self.construction
            .outputs(&rel.rows)?
            .into_iter()
            .find(|output| {
                self.construction
                    .value(*output)
                    .is_ok_and(|(name, _)| name == Some(wanted.as_str()))
            })
            .ok_or(EngineError::UnknownColumn {
                stage,
                column: column.to_string(),
            })
    }

    pub fn rows(&mut self, stage: usize, columns: &[String]) -> Result<Rel, EngineError> {
        let declared = columns
            .iter()
            .map(|column| Declare::named(qualified(stage, column), ()));
        self.connect(
            EngineOperation::Rows { stage },
            Vec::new(),
            Outputs::extend(declared),
        )
    }

    pub fn join(
        &mut self,
        left: Rel,
        right: Rel,
        on: Vec<(Ref, Ref, DataType)>,
    ) -> Result<Rel, EngineError> {
        if on.is_empty() {
            return Err(EngineError::EmptyJoin);
        }
        let mut kept = self.construction.outputs(&left.rows)?;
        kept.extend(self.construction.outputs(&right.rows)?);
        let kept = kept.into_iter().map(Output::Keep);
        self.connect(
            EngineOperation::Join { on },
            vec![left, right],
            Outputs::replace(kept),
        )
    }

    pub fn select(&mut self, rel: Rel, columns: Vec<(String, Ref)>) -> Result<Rel, EngineError> {
        let refs = columns.iter().map(|(_, output)| *output).collect();
        let declared = columns
            .into_iter()
            .map(|(name, _)| Output::Declare(Declare::named(name, ())));
        self.connect(
            EngineOperation::Select(refs),
            vec![rel],
            Outputs::replace(declared),
        )
    }

    pub fn order(&mut self, rel: Rel, keys: Vec<(Ref, Order)>) -> Result<Rel, EngineError> {
        self.connect(EngineOperation::Order(keys), vec![rel], Outputs::preserve())
    }

    pub fn tag(&mut self, rel: Rel, columns: Vec<(String, String)>) -> Result<Rel, EngineError> {
        let declared = columns.iter().map(|(name, _)| Declare::named(name, ()));
        let declared: Vec<_> = declared.collect();
        self.connect(
            EngineOperation::Tag(columns),
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
    ) -> Result<Rel, EngineError> {
        let operation = EngineOperation::Excerpt {
            columns,
            chars,
            suffix: suffix.to_string(),
        };
        self.connect(operation, vec![rel], Outputs::preserve())
    }

    /// The output of `rel` declared `name`, as the result columns are.
    pub fn output(&self, rel: &Rel, name: &str) -> Result<Ref, EngineError> {
        self.construction
            .outputs(&rel.rows)?
            .into_iter()
            .find(|output| {
                self.construction
                    .value(*output)
                    .is_ok_and(|(declared, _)| declared == Some(name))
            })
            .ok_or(EngineError::UnknownColumn {
                stage: usize::MAX,
                column: name.to_string(),
            })
    }

    pub fn limit(&mut self, rel: Rel, rows: u64, skip: u64) -> Result<Rel, EngineError> {
        let operation = EngineOperation::Limit { rows, skip };
        self.connect(operation, vec![rel], Outputs::preserve())
    }

    fn connect(
        &mut self,
        operation: EngineOperation,
        inputs: Vec<Rel>,
        outputs: Outputs<()>,
    ) -> Result<Rel, EngineError> {
        let inputs = inputs.into_iter().map(|rel| rel.rows).collect();
        let connected = self.construction.connect(operation, inputs, outputs)?;
        Ok(Rel {
            rows: connected.rows,
        })
    }
}

impl Rebuilder for EngineBuilder {
    type Language = EngineLanguage;
    type Old = EnginePlan;
    type Rows = Rel;
    type Error = EngineError;

    fn plan(old: &EnginePlan) -> &EnginePlan {
        old
    }

    fn construction(&mut self) -> &mut Construction<EngineLanguage> {
        &mut self.construction
    }

    fn rows(rows: &Rel) -> &tracking::Rows {
        &rows.rows
    }

    fn apply(
        &mut self,
        operation: EngineOperation,
        inputs: Vec<Rel>,
        outputs: Outputs<()>,
    ) -> Result<Rel, EngineError> {
        self.connect(operation, inputs, outputs)
    }
}
