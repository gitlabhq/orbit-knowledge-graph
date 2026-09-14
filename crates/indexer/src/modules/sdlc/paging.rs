//! Sizes datalake pages by bytes: the page budget, the next page's row limit
//! and the read block size derived from the previous page's row width.

/// Two pages are resident during read-ahead, so the indexer peak is about twice this.
pub(super) const PAGE_BYTE_BUDGET: u64 = 2 << 30;

/// Floor for a shrunken limit so a page of a few giant rows cannot turn paging into a row-by-row crawl.
const ADAPTIVE_MIN_ROWS: u64 = 1_000;

/// A few blocks wait in the decode queue while a page fills; 64 MiB keeps that queue small next to the budget.
const TARGET_BLOCK_BYTES: u64 = 64 << 20;

/// ClickHouse's default `max_block_size` in rows; a hint at or above it changes nothing, so none is sent.
const CLICKHOUSE_DEFAULT_MAX_BLOCK_SIZE: u64 = 65_409;

pub(super) fn block_size_for(rows: u64, bytes: u64, floor: u64) -> Option<u64> {
    if rows == 0 || bytes == 0 {
        return None;
    }
    let fit = TARGET_BLOCK_BYTES.saturating_mul(rows) / bytes;
    if fit >= CLICKHOUSE_DEFAULT_MAX_BLOCK_SIZE {
        return None;
    }
    Some(fit.max(floor))
}

pub(super) fn next_page_limit(
    current: u64,
    plan_limit: u64,
    rows: u64,
    bytes: u64,
    truncated: bool,
) -> u64 {
    if truncated {
        rows.max(ADAPTIVE_MIN_ROWS).min(plan_limit)
    } else if current < plan_limit && bytes.saturating_mul(2) < PAGE_BYTE_BUDGET {
        current.saturating_mul(2).min(plan_limit)
    } else {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_page_limit_shrinks_to_the_rows_that_fit_the_budget() {
        assert_eq!(
            next_page_limit(500_000, 500_000, 134_000, PAGE_BYTE_BUDGET - 1, true),
            134_000
        );
        assert_eq!(
            next_page_limit(500_000, 500_000, 3, PAGE_BYTE_BUDGET, true),
            ADAPTIVE_MIN_ROWS
        );
    }

    #[test]
    fn block_size_follows_the_previous_page_row_width() {
        assert_eq!(block_size_for(500_000, 100 << 20, 1_024), None);
        assert_eq!(
            block_size_for(196_227, 1_780 << 20, 1_024),
            Some(TARGET_BLOCK_BYTES * 196_227 / (1_780 << 20))
        );
        assert_eq!(block_size_for(10, 10 << 30, 1_024), Some(1_024));
        assert_eq!(block_size_for(0, 0, 1_024), None);
    }

    #[test]
    fn next_page_limit_grows_back_only_while_pages_stay_small() {
        assert_eq!(
            next_page_limit(134_000, 500_000, 134_000, PAGE_BYTE_BUDGET / 4, false),
            268_000
        );
        assert_eq!(
            next_page_limit(300_000, 500_000, 300_000, PAGE_BYTE_BUDGET / 4, false),
            500_000
        );
        assert_eq!(
            next_page_limit(134_000, 500_000, 134_000, PAGE_BYTE_BUDGET / 2, false),
            134_000
        );
        assert_eq!(
            next_page_limit(500_000, 500_000, 500_000, 1, false),
            500_000
        );
    }
}
