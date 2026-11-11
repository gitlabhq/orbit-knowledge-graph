use std::path::Path;

use orbit_utils::markdown::{FQN_SEPARATOR, LineIndex, sections, segment_path};

use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{CanonicalDefinition, CanonicalImport, DefKind, Fqn, Position, Range};

#[derive(Default)]
pub struct MarkdownDsl;

impl DslLanguage for MarkdownDsl {
    fn name() -> &'static str {
        "markdown"
    }

    fn language() -> Language {
        Language::Markdown
    }

    fn parser() -> LanguageParser {
        LanguageParser::Custom(parse_markdown)
    }
}

fn parse_markdown(
    source: &str,
    file_path: &str,
) -> (Vec<CanonicalDefinition>, Vec<CanonicalImport>) {
    let stem = Path::new(file_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(file_path);
    let lines = LineIndex::new(source);
    let position = |(line, column)| Position::new(line, column);
    let sections = sections(source);
    let definitions = sections
        .iter()
        .enumerate()
        .map(|(index, section)| {
            let parts: Vec<&str> = std::iter::once(stem)
                .chain(segment_path(&sections, index))
                .collect();
            CanonicalDefinition {
                definition_type: "Section",
                kind: DefKind::Module,
                name: section.name.clone(),
                fqn: Fqn::from_parts(&parts, FQN_SEPARATOR),
                range: Range::new(
                    position(lines.position(section.start)),
                    position(lines.end_position(source, section.end)),
                    (section.start, section.end),
                ),
                is_top_level: section.parent.is_none(),
                metadata: None,
            }
        })
        .collect();
    (definitions, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        MarkdownDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "guide.md",
                crate::v2::config::Language::Markdown,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "guide.md",
                    format!("parse error: {:?}", e),
                )
            })
    }

    #[test]
    fn headings_become_section_definitions() {
        let result = parse("# Setup\n\nSome intro text.\n\n# Usage\n\nRun the thing.\n").unwrap();
        let names: Vec<&str> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Section")
            .map(|d| d.name.as_str())
            .collect();
        assert_eq!(names, vec!["Setup", "Usage"]);
        assert!(
            result
                .definitions
                .iter()
                .all(|d| d.definition_type != "Section" || d.kind == DefKind::Module)
        );
    }

    #[test]
    fn nested_headings_scope_under_their_parent_section() {
        let result =
            parse("# Setup\n\n## Prerequisites\n\nInstall things.\n\n## Install\n\nRun make.\n")
                .unwrap();
        let prereqs = result
            .definitions
            .iter()
            .find(|d| d.name == "Prerequisites")
            .unwrap();
        assert_eq!(prereqs.fqn.to_string(), "guide#Setup#Prerequisites");
        assert!(!prereqs.is_top_level);
        let setup = result
            .definitions
            .iter()
            .find(|d| d.name == "Setup")
            .unwrap();
        assert!(setup.is_top_level);
    }

    #[test]
    fn section_spans_run_to_the_next_same_level_heading() {
        let src = "# Setup\n\n## Prerequisites\n\nInstall things.\n\n# Usage\n\nRun it.\n";
        let result = parse(src).unwrap();
        let setup = result
            .definitions
            .iter()
            .find(|d| d.name == "Setup")
            .unwrap();
        let usage = result
            .definitions
            .iter()
            .find(|d| d.name == "Usage")
            .unwrap();
        assert_eq!(setup.range.byte_offset.1, usage.range.byte_offset.0);
        assert_eq!(usage.range.byte_offset.1, src.len());
        assert!(
            setup.range.byte_offset.1
                > result
                    .definitions
                    .iter()
                    .find(|d| d.name == "Prerequisites")
                    .unwrap()
                    .range
                    .byte_offset
                    .0
        );
    }

    #[test]
    fn front_matter_does_not_become_a_section() {
        let result =
            parse("---\ntitle: \"ADR 000\"\nauthors: [ \"@x\" ]\n---\n\n# Real Heading\n\nBody.\n")
                .unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["Real Heading"]);
    }

    #[test]
    fn a_bare_heading_marker_produces_no_section() {
        let result = parse("#\n\ntext\n\n# Named\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["Named"]);
    }

    #[test]
    fn section_emission_is_capped_per_file() {
        let headings: String = (0..orbit_utils::markdown::MAX_SECTIONS_PER_FILE + 50)
            .map(|i| format!("# h{i}\n"))
            .collect();
        let result = parse(&headings).unwrap();
        assert_eq!(
            result.definitions.len(),
            orbit_utils::markdown::MAX_SECTIONS_PER_FILE
        );
    }

    #[test]
    fn every_range_is_in_bounds_and_on_char_boundaries() {
        let cases = [
            "> > > *em* `c`\n> > ## Quoted \u{e9}h\n",
            "- [x] task\n  - nested\n    1. ### deep\n",
            "| a | b |\n|---|---|\n| l | \u{4f60}\u{597d} |\n\n## After table\n",
            "# A\n\n***bold em*** ~~strike~~ **unclosed\n",
            "Setext \u{e9}\n===\n\ntext\n\nSub\n---\n",
            "\u{feff}# BOM lead\n",
            "# No trailing newline",
        ];
        for src in cases {
            let (definitions, _) = super::parse_markdown(src, "g.md");
            for range in definitions.iter().map(|d| d.range) {
                let (start, end) = range.byte_offset;
                assert!(start <= end, "inverted range {range} in {src:?}");
                assert!(end <= src.len(), "out of bounds {range} in {src:?}");
                assert!(
                    src.get(start..end).is_some(),
                    "mid-char boundary {range} in {src:?}"
                );
            }
        }
    }

    #[test]
    fn duplicate_sibling_headings_get_suffixed_fqns_but_keep_their_name() {
        let result =
            parse("# Setup\n\n## Examples\n\na\n\n## Examples\n\nb\n\n## Examples\n\nc\n").unwrap();
        let fqns: Vec<String> = result
            .definitions
            .iter()
            .map(|d| d.fqn.to_string())
            .collect();
        assert_eq!(
            fqns,
            vec![
                "guide#Setup",
                "guide#Setup#Examples",
                "guide#Setup#Examples-1",
                "guide#Setup#Examples-2",
            ]
        );
        assert!(result.definitions[1..].iter().all(|d| d.name == "Examples"));
    }

    #[test]
    fn generated_suffixes_skip_segments_a_real_heading_already_uses() {
        let result = parse("# Examples\n\n# Examples-1\n\n# Examples\n").unwrap();
        let fqns: Vec<String> = result
            .definitions
            .iter()
            .map(|d| d.fqn.to_string())
            .collect();
        assert_eq!(
            fqns,
            vec!["guide#Examples", "guide#Examples-1", "guide#Examples-2"]
        );
    }

    #[test]
    fn same_heading_under_different_parents_is_not_suffixed() {
        let result = parse("# A\n\n## X\n\n# B\n\n## X\n").unwrap();
        let fqns: Vec<String> = result
            .definitions
            .iter()
            .map(|d| d.fqn.to_string())
            .collect();
        assert_eq!(fqns, vec!["guide#A", "guide#A#X", "guide#B", "guide#B#X"]);
    }

    #[test]
    fn children_scope_under_the_deduped_parent_segment() {
        let result = parse("# A\n\n## C\n\n# A\n\n## C\n").unwrap();
        let fqns: Vec<String> = result
            .definitions
            .iter()
            .map(|d| d.fqn.to_string())
            .collect();
        assert_eq!(
            fqns,
            vec!["guide#A", "guide#A#C", "guide#A-1", "guide#A-1#C"]
        );
    }

    #[test]
    fn a_file_without_headings_produces_no_sections() {
        let result = parse("just some prose\n\nwith paragraphs\n").unwrap();
        assert!(
            result
                .definitions
                .iter()
                .all(|d| d.definition_type != "Section")
        );
    }
}
