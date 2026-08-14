use std::path::Path;

use comrak::nodes::{AstNode, LineColumn, NodeValue, Sourcepos};
use comrak::{Arena, Options, parse_document};

use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, Fqn, ImportBindingKind, ImportMode, Position,
    Range,
};

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

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            lexical_file_links: true,
            ..LanguageHooks::default()
        }
    }
}

const MAX_SECTIONS_PER_FILE: usize = 10_000;
const MAX_LINKS_PER_FILE: usize = 10_000;

fn parse_markdown(
    source: &str,
    file_path: &str,
) -> (Vec<CanonicalDefinition>, Vec<CanonicalImport>) {
    let arena = Arena::new();
    let mut options = Options::default();
    options.extension.front_matter_delimiter = Some("---".to_string());
    let root = parse_document(&arena, source, &options);
    let lines = LineIndex::new(source);

    (
        section_definitions(root, source, file_path, &lines),
        link_imports(root, source, file_path, &lines),
    )
}

struct SectionFrame {
    level: u8,
    segment: String,
    def_idx: usize,
    child_names: rustc_hash::FxHashMap<String, usize>,
}

fn section_definitions<'a>(
    root: &'a AstNode<'a>,
    source: &str,
    file_path: &str,
    lines: &LineIndex,
) -> Vec<CanonicalDefinition> {
    let stem = Path::new(file_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(file_path);

    let mut stack: Vec<SectionFrame> = Vec::new();
    let mut root_names: rustc_hash::FxHashMap<String, usize> = rustc_hash::FxHashMap::default();
    let mut definitions: Vec<CanonicalDefinition> = Vec::new();
    let close = |stack: &mut Vec<SectionFrame>,
                 definitions: &mut Vec<CanonicalDefinition>,
                 level: u8,
                 end: usize| {
        while stack.last().is_some_and(|f| f.level >= level) {
            let frame = stack.pop().expect("non-empty stack");
            definitions[frame.def_idx].range.end = lines.position(end);
            definitions[frame.def_idx].range.byte_offset.1 = end;
        }
    };

    for node in root.descendants() {
        let data = node.data.borrow();
        let NodeValue::Heading(heading) = &data.value else {
            continue;
        };
        let name = inline_text(node);
        if name.is_empty() {
            continue;
        }
        if definitions.len() == MAX_SECTIONS_PER_FILE {
            tracing::debug!(file_path, cap = MAX_SECTIONS_PER_FILE, "section cap hit");
            break;
        }
        let start = lines.byte(data.sourcepos.start);
        close(&mut stack, &mut definitions, heading.level, start);
        let segment = {
            let seen = stack
                .last_mut()
                .map_or(&mut root_names, |f| &mut f.child_names);
            let count = seen.entry(name.clone()).or_insert(0);
            let segment = if *count == 0 {
                name.clone()
            } else {
                format!("{name}-{count}")
            };
            *count += 1;
            segment
        };
        let fqn = {
            let parts: Vec<&str> = std::iter::once(stem)
                .chain(stack.iter().map(|f| f.segment.as_str()))
                .chain(std::iter::once(segment.as_str()))
                .collect();
            Fqn::from_parts(&parts, "#")
        };
        let is_top_level = stack.is_empty();
        stack.push(SectionFrame {
            level: heading.level,
            segment,
            def_idx: definitions.len(),
            child_names: rustc_hash::FxHashMap::default(),
        });
        definitions.push(CanonicalDefinition {
            definition_type: "Section",
            kind: DefKind::Module,
            name,
            fqn,
            range: Range::new(
                lines.position(start),
                lines.position(source.len()),
                (start, source.len()),
            ),
            is_top_level,
            metadata: None,
        });
    }
    definitions
}

fn link_imports<'a>(
    root: &'a AstNode<'a>,
    source: &str,
    file_path: &str,
    lines: &LineIndex,
) -> Vec<CanonicalImport> {
    let mut imports = Vec::new();
    for node in root.descendants() {
        let data = node.data.borrow();
        let url = match &data.value {
            NodeValue::Link(link) | NodeValue::Image(link) => &link.url,
            _ => continue,
        };
        let Some(target) = repo_link_target(url) else {
            continue;
        };
        if imports.len() == MAX_LINKS_PER_FILE {
            tracing::debug!(file_path, cap = MAX_LINKS_PER_FILE, "link cap hit");
            break;
        }
        let name = target
            .rsplit('/')
            .next()
            .map(|f| f.rsplit_once('.').map_or(f, |(base, _)| base))
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        imports.push(CanonicalImport {
            import_type: "Link",
            binding_kind: ImportBindingKind::Named,
            mode: ImportMode::Declarative,
            path: target,
            name,
            alias: None,
            scope_fqn: None,
            range: lines.range(data.sourcepos, source),
            is_type_only: false,
            wildcard: false,
        });
    }
    imports
}

fn inline_text<'a>(node: &'a AstNode<'a>) -> String {
    let mut text = String::new();
    for child in node.descendants().skip(1) {
        match &child.data.borrow().value {
            NodeValue::Text(t) => text.push_str(t),
            NodeValue::Code(code) => text.push_str(&code.literal),
            NodeValue::SoftBreak | NodeValue::LineBreak => text.push(' '),
            _ => {}
        }
    }
    text.trim().to_string()
}

fn repo_link_target(raw: &str) -> Option<String> {
    let target = raw.trim().trim_start_matches('<').trim_end_matches('>');
    let target = target.split(['#', '?']).next().unwrap_or("").trim();
    (!target.is_empty()
        && !target.starts_with("//")
        && !target.contains(':')
        && !target.contains(char::is_whitespace))
    .then(|| target.to_string())
}

struct LineIndex {
    starts: Vec<usize>,
    len: usize,
}

impl LineIndex {
    fn new(source: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(
            source
                .bytes()
                .enumerate()
                .filter_map(|(i, b)| (b == b'\n').then_some(i + 1)),
        );
        Self {
            starts,
            len: source.len(),
        }
    }

    fn byte(&self, lc: LineColumn) -> usize {
        self.starts
            .get(lc.line.saturating_sub(1))
            .map_or(0, |start| start + lc.column.saturating_sub(1))
            .min(self.len)
    }

    fn position(&self, offset: usize) -> Position {
        let line = self
            .starts
            .partition_point(|start| *start <= offset)
            .saturating_sub(1);
        Position::new(line, offset - self.starts[line])
    }

    fn range(&self, sourcepos: Sourcepos, source: &str) -> Range {
        let start = self.byte(sourcepos.start);
        let end_start = self.byte(sourcepos.end);
        let end = source
            .get(end_start..)
            .and_then(|rest| rest.chars().next())
            .map_or(self.len, |c| end_start + c.len_utf8());
        Range::new(self.position(start), self.position(end), (start, end))
    }
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
    fn relative_links_become_imports_and_external_links_are_ignored() {
        let result = parse(
            "# Guide\n\nSee [security](../design/security.md#auth) and [the API](https://example.com/api?v=2),\nplus [setup](./setup.md) and [root](/docs/root.md) but not [mail](mailto:x@y.z) or [cdn](//cdn.example.com/x.md).\n",
        )
        .unwrap();
        let links: Vec<(&str, &str)> = result
            .imports
            .iter()
            .map(|i| (i.import_type, i.path.as_str()))
            .collect();
        assert_eq!(
            links,
            vec![
                ("Link", "../design/security.md"),
                ("Link", "./setup.md"),
                ("Link", "/docs/root.md"),
            ]
        );
        assert_eq!(result.imports[0].name.as_deref(), Some("security"));
    }

    #[test]
    fn reference_style_links_resolve_to_their_definition() {
        let result = parse(
            "# Guide\n\nSee [the security doc][sec] and [setup] too.\n\n[sec]: ../design/security.md\n[setup]: ./setup.md\n",
        )
        .unwrap();
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["../design/security.md", "./setup.md"]);
    }

    #[test]
    fn repeated_links_to_one_target_get_distinct_ranges() {
        let src = "# Guide\n\nIntro [setup](./setup.md) and later [again](./setup.md).\n";
        let result = parse(src).unwrap();
        assert_eq!(result.imports.len(), 2);
        let offsets: Vec<(usize, usize)> =
            result.imports.iter().map(|i| i.range.byte_offset).collect();
        assert_ne!(offsets[0], offsets[1]);
        assert_eq!(&src[offsets[0].0..offsets[0].1], "[setup](./setup.md)");
        assert_eq!(&src[offsets[1].0..offsets[1].1], "[again](./setup.md)");
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
    fn image_destinations_are_treated_as_file_links() {
        let result =
            parse("# G\n\n![diagram](assets/arch.png) and ![ext](https://x.io/a.png)\n").unwrap();
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["assets/arch.png"]);
    }

    #[test]
    fn a_bare_heading_marker_produces_no_section() {
        let result = parse("#\n\ntext\n\n# Named\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["Named"]);
    }

    #[test]
    fn multibyte_text_keeps_link_ranges_on_char_boundaries() {
        let src = "# \u{dc}n\u{ef}code\n\nS\u{e9}e [s\u{e9}tup](./s\u{e9}tup.md) und [zwei](docs/z\u{e4}hler.md).\n";
        let result = parse(src).unwrap();
        assert_eq!(result.imports.len(), 2);
        assert_eq!(
            &src[result.imports[0].range.byte_offset.0..result.imports[0].range.byte_offset.1],
            "[s\u{e9}tup](./s\u{e9}tup.md)"
        );
        assert_eq!(result.imports[1].path, "docs/z\u{e4}hler.md");
        assert_eq!(result.definitions[0].name, "\u{dc}n\u{ef}code");
    }

    #[test]
    fn section_and_link_emission_is_capped_per_file() {
        let headings: String = (0..MAX_SECTIONS_PER_FILE + 50)
            .map(|i| format!("# h{i}\n"))
            .collect();
        let links: String = (0..MAX_LINKS_PER_FILE + 50)
            .map(|i| format!("[l{i}](./f{i}.md) "))
            .collect();
        let result = parse(&format!("{headings}\n{links}\n")).unwrap();
        assert_eq!(result.definitions.len(), MAX_SECTIONS_PER_FILE);
        assert_eq!(result.imports.len(), MAX_LINKS_PER_FILE);
    }

    #[test]
    fn every_range_is_in_bounds_and_on_char_boundaries() {
        let cases = [
            "> > > [deep](./a.md) *em [b](./b.md)* `c`\n> > ## Quoted \u{e9}h\n",
            "- [x] task [t](./t.md)\n  - nested\n    1. [n](./n.md)\n",
            "| a | b |\n|---|---|\n| [l](./l.md) | \u{4f60}\u{597d} |\n",
            "# A\n\n***bold em [x](./x.md)*** ~~strike~~ **unclosed [y](./y.md)\n",
            "Setext \u{e9}\n===\n\ntext [z](./z.md)\n\nSub\n---\n",
            "\u{feff}# BOM lead\n[a](./a.md)\n",
            "# No trailing newline [e](./e.md)",
        ];
        for src in cases {
            let (definitions, imports) = super::parse_markdown(src, "g.md");
            for range in definitions
                .iter()
                .map(|d| d.range)
                .chain(imports.iter().map(|i| i.range))
            {
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
    fn link_names_keep_every_dot_but_the_extension() {
        let result = parse("# G\n\nSee [v2](docs/reference.v2.md).\n").unwrap();
        assert_eq!(result.imports[0].name.as_deref(), Some("reference.v2"));
    }

    #[test]
    fn custom_parse_respects_the_per_file_cpu_budget() {
        let doc: String = (0..2000)
            .map(|i| format!("# h{i}\n\ntext [l{i}](./f{i}.md)\n\n"))
            .collect();
        let result = MarkdownDsl::spec().parse_full_collect(
            doc.as_bytes(),
            "big.md",
            crate::v2::config::Language::Markdown,
            &Tracer::new(false),
            crate::v2::dsl::engine::PhaseTimeouts {
                parse: Some(std::time::Duration::from_nanos(1)),
                ..Default::default()
            },
        );
        match result {
            Err(crate::v2::dsl::engine::ParseFullError::Aborted { phase, .. }) => {
                assert_eq!(phase, crate::v2::error::AbortPhase::Parse);
            }
            Err(other) => panic!("expected a parse-budget abort, got {other}"),
            Ok(_) => panic!("expected a parse-budget abort, got a successful parse"),
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
