use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::types::{CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::extract::descendant;
use treesitter_visit::predicate::has_child;
use treesitter_visit::tree_sitter::{ParseGuard, StrDoc};
use treesitter_visit::{Node, Root, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

#[derive(Default)]
pub struct MarkdownDsl;

impl DslLanguage for MarkdownDsl {
    fn name() -> &'static str {
        "markdown"
    }

    fn language() -> Language {
        Language::Markdown
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("section", "Section")
                .def_kind(DefKind::Module)
                .when(has_child(&["atx_heading", "setext_heading"]))
                .name_from(descendant("inline")),
        ]
    }

    fn file_scope() -> bool {
        true
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(markdown_extract_links),
            lexical_file_links: true,
            ..LanguageHooks::default()
        }
    }
}

const INLINE_LINK_PARSE_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);

fn markdown_extract_links(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "inline" {
        return false;
    }
    let text = node.text();
    let guard = ParseGuard::default().with_budget(INLINE_LINK_PARSE_BUDGET);
    let Ok(ast) = Root::try_new(&*text, SupportLang::MarkdownInline, &guard) else {
        return false;
    };
    let base_byte = node.range().start;
    let base_line = node.start_pos().line();
    let base_column = node.start_pos().column(node);
    let mut pushed = false;
    for dest in ast.root().dfs() {
        if dest.kind().as_ref() != "link_destination" {
            continue;
        }
        let Some(target) = repo_link_target(&dest.text()) else {
            continue;
        };
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
            range: file_range(&dest, base_byte, base_line, base_column),
            is_type_only: false,
            wildcard: false,
        });
        pushed = true;
    }
    pushed
}

fn file_range(
    dest: &N<'_>,
    base_byte: usize,
    base_line: usize,
    base_column: usize,
) -> crate::v2::types::Range {
    let bytes = dest.range();
    let start = dest.start_pos();
    let end = dest.end_pos();
    let column = |line: usize, column: usize| {
        if line == 0 {
            base_column + column
        } else {
            column
        }
    };
    crate::v2::types::Range::new(
        crate::v2::types::Position::new(
            base_line + start.line(),
            column(start.line(), start.column(dest)),
        ),
        crate::v2::types::Position::new(
            base_line + end.line(),
            column(end.line(), end.column(dest)),
        ),
        (base_byte + bytes.start, base_byte + bytes.end),
    )
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
        assert!(
            prereqs.fqn.to_string().contains("Setup"),
            "expected FQN scoped under Setup, got {}",
            prereqs.fqn
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
    fn repeated_links_to_one_target_get_distinct_ranges() {
        let src = "# Guide\n\nIntro [setup](./setup.md) and later [again](./setup.md).\n";
        let result = parse(src).unwrap();
        assert_eq!(result.imports.len(), 2);
        let offsets: Vec<(usize, usize)> =
            result.imports.iter().map(|i| i.range.byte_offset).collect();
        assert_ne!(offsets[0], offsets[1]);
        for (start, end) in offsets {
            assert_eq!(&src[start..end], "./setup.md");
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
