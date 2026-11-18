use std::collections::BTreeMap;

use anyhow::anyhow;

use crate::payload::{Payload, TaskInput};
use crate::worker::{TaskError, Worker};

const MAX_CHILDREN: usize = 10_000;

pub struct Outcome<O> {
    completion: Completion<O>,
    children: Vec<Result<TaskInput, serde_json::Error>>,
    rows: BTreeMap<&'static str, u64>,
}

pub(crate) enum Completion<O> {
    Done(O),
    NoChange,
}

pub(crate) struct EncodedOutcome {
    pub(crate) completion: Completion<Vec<u8>>,
    pub(crate) children: Vec<TaskInput>,
    pub(crate) rows: BTreeMap<&'static str, u64>,
}

impl<O: Payload> Outcome<O> {
    pub fn done(output: O) -> Self {
        Self::from_completion(Completion::Done(output))
    }

    pub fn no_change() -> Self {
        Self::from_completion(Completion::NoChange)
    }

    pub fn spawn<W: Worker>(mut self, inputs: impl IntoIterator<Item = W::Input>) -> Self {
        let encoded = inputs
            .into_iter()
            .map(|input| TaskInput::encode::<W>(&input));
        self.children.extend(encoded);
        self
    }

    pub fn rows(mut self, label: &'static str, count: u64) -> Self {
        *self.rows.entry(label).or_default() += count;
        self
    }

    pub(crate) fn encode(self) -> Result<EncodedOutcome, TaskError> {
        if self.children.len() > MAX_CHILDREN {
            return Err(TaskError::permanent(anyhow!(
                "an outcome spawned {} children, and the limit is {MAX_CHILDREN}",
                self.children.len()
            )));
        }
        let children = self
            .children
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(TaskError::permanent)?;
        let completion = match self.completion {
            Completion::Done(output) => {
                Completion::Done(serde_json::to_vec(&output).map_err(TaskError::permanent)?)
            }
            Completion::NoChange => Completion::NoChange,
        };
        Ok(EncodedOutcome {
            completion,
            children,
            rows: self.rows,
        })
    }

    fn from_completion(completion: Completion<O>) -> Self {
        Self {
            completion,
            children: Vec::new(),
            rows: BTreeMap::new(),
        }
    }
}
