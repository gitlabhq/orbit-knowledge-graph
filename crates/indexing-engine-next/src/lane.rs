use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lane(Cow<'static, str>);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub struct LaneId {
    worker: String,
    lane: Lane,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("the lane {0:?} must look like <worker>/<lane>")]
pub struct InvalidLaneId(String);

impl Lane {
    pub const DEFAULT: Lane = Lane::new("default");
    pub const BACKFILL: Lane = Lane::new("backfill");

    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }
}

impl LaneId {
    pub fn new(worker: impl Into<String>, lane: Lane) -> Self {
        Self {
            worker: worker.into(),
            lane,
        }
    }

    pub fn worker(&self) -> &str {
        &self.worker
    }

    pub fn lane(&self) -> &Lane {
        &self.lane
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for LaneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.worker, self.lane)
    }
}

impl FromStr for LaneId {
    type Err = InvalidLaneId;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidLaneId(id.to_owned());
        let (worker, lane) = id.split_once('/').ok_or_else(invalid)?;
        if worker.is_empty() || lane.is_empty() || lane.contains('/') {
            return Err(invalid());
        }
        Ok(Self::new(worker, Lane(Cow::Owned(lane.to_owned()))))
    }
}

impl TryFrom<String> for LaneId {
    type Error = InvalidLaneId;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        id.parse()
    }
}
