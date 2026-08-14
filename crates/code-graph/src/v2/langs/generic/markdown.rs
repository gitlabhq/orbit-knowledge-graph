use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::types::DefKind;
use treesitter_visit::extract::descendant;
use treesitter_visit::predicate::has_child;

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
