use crate::observer::IndexingMode;

/// Whether a write blocks until the store confirms it durably landed. `Durable` is mandatory where
/// a dropped write is unrecoverable: the watermark advances with no NATS retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDurability {
    FireAndForget,
    Durable,
}

/// Mode inverts durability. Full: data writes use configured settings (`None`) since a lost page
/// re-pulls, but completion must persist or the watermark never advances. Incremental: each data
/// write must persist (watermark advances, no NATS retry); a lost completion re-derives next run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunDurability {
    pub data_writes: Option<WriteDurability>,
    pub completion: WriteDurability,
}

impl RunDurability {
    pub fn for_mode(mode: IndexingMode) -> Self {
        match mode {
            IndexingMode::Full => Self {
                data_writes: None,
                completion: WriteDurability::Durable,
            },
            IndexingMode::Incremental => Self {
                data_writes: Some(WriteDurability::Durable),
                completion: WriteDurability::FireAndForget,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_durability_inverts_page_and_completion() {
        let full = RunDurability::for_mode(IndexingMode::Full);
        assert_eq!(full.data_writes, None);
        assert_eq!(full.completion, WriteDurability::Durable);

        let incremental = RunDurability::for_mode(IndexingMode::Incremental);
        assert_eq!(incremental.data_writes, Some(WriteDurability::Durable));
        assert_eq!(incremental.completion, WriteDurability::FireAndForget);
    }
}
