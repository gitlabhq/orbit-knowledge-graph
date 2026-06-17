use crate::observer::IndexingMode;

/// Whether a write blocks until ClickHouse confirms the async insert flushed. `Durable` is
/// mandatory where a dropped write is unrecoverable: the watermark advances with no NATS retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDurability {
    FireAndForget,
    Durable,
}

impl WriteDurability {
    /// Empty for `FireAndForget` so the deployment's `insert_settings` apply unchanged.
    pub(crate) fn insert_overrides(self) -> &'static [(&'static str, &'static str)] {
        match self {
            WriteDurability::Durable => &[("async_insert", "1"), ("wait_for_async_insert", "1")],
            WriteDurability::FireAndForget => &[],
        }
    }
}

/// Page and completion durability invert by mode: a full load re-pulls lost pages but must persist
/// completion; an incremental must persist pages (the watermark advances, no NATS retry).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunDurability {
    pub page: WriteDurability,
    pub completion: WriteDurability,
}

impl RunDurability {
    pub fn for_mode(mode: IndexingMode) -> Self {
        match mode {
            IndexingMode::Full => Self {
                page: WriteDurability::FireAndForget,
                completion: WriteDurability::Durable,
            },
            IndexingMode::Incremental => Self {
                page: WriteDurability::Durable,
                completion: WriteDurability::FireAndForget,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_pins_async_insert_and_wait() {
        assert_eq!(
            WriteDurability::Durable.insert_overrides(),
            &[("async_insert", "1"), ("wait_for_async_insert", "1")]
        );
    }

    #[test]
    fn fire_and_forget_defers_to_config() {
        assert!(WriteDurability::FireAndForget.insert_overrides().is_empty());
    }

    #[test]
    fn run_durability_inverts_page_and_completion() {
        let full = RunDurability::for_mode(IndexingMode::Full);
        assert_eq!(full.page, WriteDurability::FireAndForget);
        assert_eq!(full.completion, WriteDurability::Durable);

        let incremental = RunDurability::for_mode(IndexingMode::Incremental);
        assert_eq!(incremental.page, WriteDurability::Durable);
        assert_eq!(incremental.completion, WriteDurability::FireAndForget);
    }
}
