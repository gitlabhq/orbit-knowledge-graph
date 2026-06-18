//! The single filtering policy for code indexing, shared by every file source
//! (Gitaly tar, directory walk) as a [`FileStreamHooks`] implementation.
//!
//! It decides, per file, whether to load the bytes ([`Decision::Keep`]) or
//! record a bare node only ([`Decision::ListOnly`]): excluded extensions and
//! oversize files are settled from the header; binary blobs and minified /
//! long-line bundles from the content. Resolver inputs (`Cargo.toml`,
//! `package.json`, `.gitignore`, …) are never in the denylist, so they survive.
//! It also charges a total-bytes [`Counter`] across every file so a
//! pathologically large repository aborts the whole stream.

use std::path::Path;

use gkg_utils::fs_stream::{CapExceeded, Counter, Decision, FileInventoryEntry, FileStreamHooks};
use rustc_hash::FxHashMap;

use super::{is_excluded_from_indexing, looks_binary};

/// git's binary heuristic looks at the first 8 KiB; matching it keeps a NUL deep
/// inside a large text file from being misread as binary.
const BINARY_SNIFF_BYTES: usize = 8000;

const MAX_LINE_LENGTH: usize = 64 * 1024;
const MAX_AVG_LINE_LENGTH: usize = 16 * 1024;
const MINIFIED_SIZE_THRESHOLD: usize = 5_000;

/// Build-artifact bundles recognized by name, before any read. The content
/// heuristic in `on_content` catches unnamed ones, but a small minified bundle
/// can stay under the content thresholds, so the name match still earns its keep.
const MINIFIED_SUFFIXES: &[&str] = &[".min.js", ".min.mjs", ".min.cjs"];

/// Why [`CodeFilter`] declined to load a file. Low-cardinality, snake_case for
/// metric labels.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum FilterSkip {
    Oversize,
    ExcludedExtension,
    Binary,
    Minified,
    LineTooLong,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkipTally {
    pub count: u64,
    pub bytes: u64,
}

/// The code-indexing filter. Construct one per repository stream.
pub struct CodeFilter {
    max_file_size: u64,
    total_bytes: Counter,
    skips: FxHashMap<FilterSkip, SkipTally>,
}

impl CodeFilter {
    /// `max_file_size` and `max_total_bytes` are byte caps; `0` means unlimited.
    pub fn new(max_file_size: u64, max_total_bytes: u64) -> Self {
        Self {
            max_file_size,
            total_bytes: Counter::new("total_bytes", max_total_bytes),
            skips: FxHashMap::default(),
        }
    }

    /// Per-reason `(count, bytes)` of files recorded as nodes but not loaded.
    pub fn skips(&self) -> impl Iterator<Item = (FilterSkip, SkipTally)> + '_ {
        self.skips.iter().map(|(reason, tally)| (*reason, *tally))
    }

    fn record(&mut self, reason: FilterSkip, bytes: u64) -> Decision {
        let tally = self.skips.entry(reason).or_default();
        tally.count += 1;
        tally.bytes += bytes;
        Decision::ListOnly
    }
}

impl FileStreamHooks for CodeFilter {
    fn admit(&mut self, file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        self.total_bytes.add(file.size)
    }

    fn on_header(&mut self, file: &FileInventoryEntry) -> Decision {
        if self.max_file_size != 0 && file.size > self.max_file_size {
            return self.record(FilterSkip::Oversize, file.size);
        }
        if is_excluded_from_indexing(Path::new(&file.path)) {
            return self.record(FilterSkip::ExcludedExtension, file.size);
        }
        if MINIFIED_SUFFIXES.iter().any(|s| file.path.ends_with(s)) {
            return self.record(FilterSkip::Minified, file.size);
        }
        Decision::Keep
    }

    fn on_content(&mut self, file: &FileInventoryEntry, content: &[u8]) -> Decision {
        let sniff = &content[..content.len().min(BINARY_SNIFF_BYTES)];
        if looks_binary(sniff) {
            return self.record(FilterSkip::Binary, file.size);
        }
        if let Some(reason) = minified_skip(content) {
            return self.record(reason, file.size);
        }
        Decision::Keep
    }
}

/// Detect machine-generated bundles by line shape: a single line over
/// [`MAX_LINE_LENGTH`], or a high average line length over a non-trivial file.
/// Split on `\n` and `\r` so classic-Mac line endings can't hide as one line.
fn minified_skip(content: &[u8]) -> Option<FilterSkip> {
    let mut line_count = 0usize;
    let mut current_line_len = 0usize;
    for &byte in content {
        if byte == b'\n' || byte == b'\r' {
            current_line_len = 0;
            line_count += 1;
        } else {
            current_line_len += 1;
            if current_line_len > MAX_LINE_LENGTH {
                return Some(FilterSkip::LineTooLong);
            }
        }
    }
    if current_line_len > 0 {
        line_count += 1;
    }
    let line_count = line_count.max(1);
    if content.len() / line_count > MAX_AVG_LINE_LENGTH && content.len() > MINIFIED_SIZE_THRESHOLD {
        return Some(FilterSkip::Minified);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Keep,
        }
    }

    #[test]
    fn keeps_source_and_resolver_inputs() {
        let mut f = CodeFilter::new(0, 0);
        for path in ["src/main.rs", "Cargo.toml", "package.json", ".gitignore"] {
            assert_eq!(f.on_header(&entry(path, 100)), Decision::Keep, "{path}");
            assert_eq!(
                f.on_content(&entry(path, 100), b"contents\n"),
                Decision::Keep
            );
        }
    }

    #[test]
    fn list_only_for_excluded_oversize_binary_minified() {
        let mut f = CodeFilter::new(50, 0);
        assert_eq!(f.on_header(&entry("logo.png", 10)), Decision::ListOnly);
        assert_eq!(f.on_header(&entry("big.rs", 999)), Decision::ListOnly);
        assert_eq!(
            f.on_content(&entry("x.bin", 10), b"a\x00b"),
            Decision::ListOnly
        );
        let minified = vec![b'a'; MAX_LINE_LENGTH + 1];
        assert_eq!(
            f.on_content(&entry("bundle.js", 10), &minified),
            Decision::ListOnly
        );
    }

    #[test]
    fn minified_bundles_are_list_only_by_name_without_reading() {
        let mut f = CodeFilter::new(0, 0);
        for path in ["vendor/jquery.min.js", "a/b.min.mjs", "c.min.cjs"] {
            assert_eq!(f.on_header(&entry(path, 200)), Decision::ListOnly, "{path}");
        }
        // The leading dot must be literal — these are real source, not bundles.
        for path in ["src/admin.js", "src/examine.js"] {
            assert_eq!(f.on_header(&entry(path, 200)), Decision::Keep, "{path}");
        }
    }

    #[test]
    fn total_bytes_cap_charges_every_file_then_trips() {
        let mut f = CodeFilter::new(0, 100);
        assert!(f.admit(&entry("a.png", 60)).is_ok());
        assert!(
            f.admit(&entry("b.png", 60)).is_err(),
            "excluded files still count toward the total-bytes cap"
        );
    }
}
