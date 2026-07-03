use crate::observer::IndexingMode;

/// Whether a write blocks until the store confirms it durably landed. `Durable` is mandatory where
/// a dropped write is unrecoverable: the watermark advances with no NATS retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDurability {
    FireAndForget,
    Durable,
}

/// Full: data writes coalesce server-side (Durable), completion must persist. Incremental: each write persists, completion can be lost and re-derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunDurability {
    pub data_writes: Option<WriteDurability>,
    pub completion: WriteDurability,
}

impl RunDurability {
    pub fn for_mode(mode: IndexingMode) -> Self {
        match mode {
            IndexingMode::Full => Self {
                data_writes: Some(WriteDurability::Durable),
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
        assert_eq!(full.data_writes, Some(WriteDurability::Durable));
        assert_eq!(full.completion, WriteDurability::Durable);

        let incremental = RunDurability::for_mode(IndexingMode::Incremental);
        assert_eq!(incremental.data_writes, Some(WriteDurability::Durable));
        assert_eq!(incremental.completion, WriteDurability::FireAndForget);
    }
}
