use std::borrow::Cow;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Lane(Cow<'static, str>);

impl Lane {
    pub const DEFAULT: Lane = Lane::new("default");
    pub const BACKFILL: Lane = Lane::new("backfill");

    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    pub(crate) fn format_id(&self, worker: &str) -> String {
        format!("{worker}/{}", self.0)
    }

    pub(crate) fn parse_id(id: &str) -> Option<(&str, Lane)> {
        let (worker, lane) = id.split_once('/')?;
        if worker.is_empty() || lane.is_empty() || lane.contains('/') {
            return None;
        }
        Some((worker, Lane(Cow::Owned(lane.to_owned()))))
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
