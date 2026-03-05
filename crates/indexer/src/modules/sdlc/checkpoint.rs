use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// State machine: no entry = first run; `cursor_values: Some` = interrupted
/// mid-page, resume from cursor; `cursor_values: None` = completed, watermark
/// becomes the next `last_watermark`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct Checkpoint {
    pub watermark: DateTime<Utc>,
    pub cursor_values: Option<Vec<String>>,
}
