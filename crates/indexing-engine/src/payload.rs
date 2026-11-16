use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::worker::Worker;

pub trait Payload: Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T> Payload for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tenant(String);

impl Tenant {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for Tenant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    pub fn new(kind: &str, id: impl fmt::Display) -> Self {
        Self(format!("{kind}:{id}"))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TaskInput {
    pub(crate) worker: &'static str,
    pub(crate) key: Key,
    pub(crate) bytes: Vec<u8>,
}

impl TaskInput {
    pub(crate) fn encode<W: Worker>(input: &W::Input) -> Result<Self, serde_json::Error> {
        Ok(Self {
            worker: W::NAME,
            key: W::key(input),
            bytes: serde_json::to_vec(input)?,
        })
    }
}
