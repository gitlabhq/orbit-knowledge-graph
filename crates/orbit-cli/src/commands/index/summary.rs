use code_graph::v2::FamilyFileCount;

use super::DetailedStats;
use crate::tui;

pub(super) fn format_discovery_summary(
    total_files: usize,
    parseable_files: usize,
    files_per_family: &[FamilyFileCount],
) -> String {
    let mut summary = vec![count_noun(total_files, "file")];
    if parseable_files != total_files {
        summary.push(format!(
            "{} parseable",
            tui::format_with_thousands(parseable_files)
        ));
    }
    if !files_per_family.is_empty() {
        summary.push(name_largest_languages(files_per_family));
    }
    summary.join(" · ")
}

pub(super) fn count_noun(count: usize, noun: &str) -> String {
    let plural = match count {
        1 => "",
        _ => "s",
    };
    format!("{} {noun}{plural}", tui::format_with_thousands(count))
}

pub(super) fn format_timings(detailed: &DetailedStats) -> String {
    let phases = &detailed.phase_timings;
    let mut rows = vec![format!(
        "discovery {:.0} ms · structure {:.0} ms · languages {:.0} ms · total {:.0} ms",
        phases.file_discovery_ms,
        phases.structural_graph_ms,
        phases.language_processing_ms,
        phases.total_ms
    )];
    rows.extend(detailed.language_timings.iter().map(|timing| {
        format!(
            "{:<12} {:>6} files  parse {:>7.0} ms  resolve {:>7.0} ms",
            timing.language, timing.file_count, timing.parse_ms, timing.resolve_ms
        )
    }));
    rows.join("\n")
}

fn name_largest_languages(files_per_family: &[FamilyFileCount]) -> String {
    const SHOWN: usize = 3;
    let mut largest_first: Vec<&FamilyFileCount> = files_per_family.iter().collect();
    largest_first.sort_by(|a, b| b.files.cmp(&a.files).then(a.family.cmp(&b.family)));
    let names: Vec<&str> = largest_first
        .iter()
        .take(SHOWN)
        .map(|count| count.family.as_str())
        .collect();
    match largest_first.len().saturating_sub(SHOWN) {
        0 => names.join(", "),
        hidden => format!("{} +{hidden} more", names.join(", ")),
    }
}
