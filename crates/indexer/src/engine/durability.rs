use crate::observer::IndexingMode;

/// Whether a write blocks until the store confirms it durably landed. `Durable` is mandatory where
/// a dropped write is unrecoverable: the watermark advances with no NATS retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDurability {
    FireAndForget,
    Durable,
}

/// Data-write and completion durability invert by mode: a full load re-pulls lost data but must
/// persist its completion; an incremental must persist data writes (the watermark advances with no
/// NATS retry) but re-derives a lost completion next dispatch. Per-page progress checkpoints are
/// always [`WriteDurability::FireAndForget`] (see `ClickHouseCheckpointStore::save_progress`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunDurability {
    pub data_writes: WriteDurability,
    pub completion: WriteDurability,
}

impl RunDurability {
    pub fn for_mode(mode: IndexingMode) -> Self {
        match mode {
            IndexingMode::Full => Self {
                data_writes: WriteDurability::FireAndForget,
                completion: WriteDurability::Durable,
            },
            IndexingMode::Incremental => Self {
                data_writes: WriteDurability::Durable,
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
        assert_eq!(full.data_writes, WriteDurability::FireAndForget);
        assert_eq!(full.completion, WriteDurability::Durable);

        let incremental = RunDurability::for_mode(IndexingMode::Incremental);
        assert_eq!(incremental.data_writes, WriteDurability::Durable);
        assert_eq!(incremental.completion, WriteDurability::FireAndForget);
    }
}
