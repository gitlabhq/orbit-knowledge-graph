use std::path::Path;

use comrak::nodes::{AstNode, LineColumn, NodeValue, Sourcepos};
use comrak::{Arena, Options, parse_document};

use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
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

fn parse_markdown(
    source: &str,
    file_path: &str,
) -> (Vec<CanonicalDefinition>, Vec<CanonicalImport>) {
    let arena = Arena::new();
    let mut options = Options::default();
    // Without this, `title: x` under the opening `---` of GitLab-style YAML
    // front matter parses as a setext heading and becomes a bogus Section.
    options.extension.front_matter_delimiter = Some("---".to_string());
    let root = parse_document(&arena, source, &options);
    let lines = LineIndex::new(source);

    (
        section_definitions(root, source, file_path, &lines),
        link_imports(root, source, &lines),
    )
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

    let headings: Vec<(u8, String, Sourcepos)> = root
        .descendants()
        .filter_map(|node| match &node.data.borrow().value {
            NodeValue::Heading(heading) => {
                let name = inline_text(node);
                (!name.is_empty()).then(|| (heading.level, name, node.data.borrow().sourcepos))
            }
            _ => None,
        })
        .collect();

    let mut stack: Vec<(u8, String)> = Vec::new();
    let mut definitions = Vec::with_capacity(headings.len());
    for (i, (level, name, sourcepos)) in headings.iter().enumerate() {
        while stack.last().is_some_and(|(l, _)| l >= level) {
            stack.pop();
        }
        let (end_position, end_byte) = headings[i + 1..]
            .iter()
            .find(|(l, _, _)| l <= level)
            .map(|(_, _, next)| (lines.position(next.start), lines.byte(next.start)))
            .unwrap_or_else(|| lines.eof(source));
        let parts: Vec<&str> = std::iter::once(stem)
            .chain(stack.iter().map(|(_, n)| n.as_str()))
            .chain(std::iter::once(name.as_str()))
            .collect();
        definitions.push(CanonicalDefinition {
            definition_type: "Section",
            kind: DefKind::Module,
            name: name.clone(),
            fqn: Fqn::from_parts(&parts, "#"),
            range: Range::new(
                lines.position(sourcepos.start),
                end_position,
                (lines.byte(sourcepos.start), end_byte),
            ),
            is_top_level: stack.is_empty(),
            metadata: None,
        });
        stack.push((*level, name.clone()));
    }
    definitions
}

fn link_imports<'a>(
    root: &'a AstNode<'a>,
    source: &str,
    lines: &LineIndex,
) -> Vec<CanonicalImport> {
    root.descendants()
        .filter_map(|node| {
            let data = node.data.borrow();
            let url = match &data.value {
                NodeValue::Link(link) | NodeValue::Image(link) => &link.url,
                _ => return None,
            };
            let target = repo_link_target(url)?;
            let name = target
                .rsplit('/')
                .next()
                .map(|f| f.rsplit_once('.').map_or(f, |(base, _)| base))
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            Some(CanonicalImport {
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
            })
        })
        .collect()
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
        && !target.starts_with('/')
        && !target.contains(':')
        && !target.contains(char::is_whitespace))
    .then(|| target.to_string())
}

/// Converts comrak's 1-based, byte-counted [`Sourcepos`] coordinates into the
/// 0-based, exclusive-end [`Range`] convention shared with tree-sitter. All
/// byte conversions clamp to the source length and the exclusive end snaps to
/// a char boundary, so an off sourcepos from comrak degrades to a truncated
/// range, never an out-of-bounds or mid-char offset.
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

    fn position(&self, lc: LineColumn) -> Position {
        Position::new(lc.line.saturating_sub(1), lc.column.saturating_sub(1))
    }

    fn range(&self, sourcepos: Sourcepos, source: &str) -> Range {
        let end_start = self.byte(sourcepos.end);
        let end_exclusive = source
            .get(end_start..)
            .and_then(|rest| rest.chars().next())
            .map_or(self.len, |c| end_start + c.len_utf8());
        Range::new(
            self.position(sourcepos.start),
            Position::new(sourcepos.end.line.saturating_sub(1), sourcepos.end.column),
            (self.byte(sourcepos.start), end_exclusive),
        )
    }

    fn eof(&self, source: &str) -> (Position, usize) {
        let last_line = self.starts.len() - 1;
        (
            Position::new(last_line, source.len() - self.starts[last_line]),
            source.len(),
        )
    }
}

pub struct MarkdownRules;

impl HasRules for MarkdownRules {
    fn rules() -> ResolutionRules {
        let spec = MarkdownDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "markdown",
            scopes,
            spec,
            vec![],
            vec![],
            ReceiverMode::None,
            "#",
            &[],
            None,
        )
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
            "# Guide\n\nSee [security](../design/security.md#auth) and [the API](https://example.com/api?v=2),\nplus [setup](./setup.md) but not [mail](mailto:x@y.z) or [abs](/etc/passwd).\n",
        )
        .unwrap();
        let links: Vec<(&str, &str)> = result
            .imports
            .iter()
            .map(|i| (i.import_type, i.path.as_str()))
            .collect();
        assert_eq!(
            links,
            vec![("Link", "../design/security.md"), ("Link", "./setup.md")]
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
