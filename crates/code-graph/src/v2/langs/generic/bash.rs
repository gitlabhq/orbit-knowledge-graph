use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::DefKind;
use treesitter_visit::extract::field;
use treesitter_visit::predicate::field_text;
use treesitter_visit::syntax_tree as rw;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::linker::{HasRules, ResolveSettings};

#[derive(Default)]
pub struct BashDsl;

const QUOTE_CHARS: &[char] = &['"', '\''];

fn is_source_cmd() -> treesitter_visit::predicate::Pred {
    field_text("name", "source").or(field_text("name", "."))
}

impl DslLanguage for BashDsl {
    fn name() -> &'static str {
        "bash"
    }

    fn language() -> Language {
        Language::Bash
    }

    fn rewrite(tree: &mut SyntaxTree) {
        tree.apply_rewrites(&[
            rw::insert(
                "command",
                field("argument").trim_matches(QUOTE_CHARS),
                "__import_path",
            )
            .when(is_source_cmd()),
            rw::rename("command", "__source_import").when(is_source_cmd()),
        ]);
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .name_from(field("name")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            import("__source_import")
                .label("Source")
                .path_from(treesitter_visit::extract::child_of_kind("__import_path"))
                .side_effect(),
        ]
    }

    fn file_scope() -> bool {
        true
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig::default()
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct BashRules;

impl HasRules for BashRules {
    fn rules() -> ResolutionRules {
        let spec = BashDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "bash",
            scopes,
            spec,
            vec![],
            vec![],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_settings(ResolveSettings::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        BashDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.sh",
                Language::Bash,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.sh", format!("{e:?}")))
    }

    #[test]
    fn both_function_syntaxes() {
        let result = parse("greet() {\n  echo hi\n}\nfunction helper {\n  echo yo\n}\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"greet"), "should find greet");
        assert!(names.contains(&"helper"), "should find helper");
        assert!(
            result
                .definitions
                .iter()
                .all(|d| d.kind == DefKind::Function),
            "all defs should be functions"
        );
    }

    #[test]
    fn source_and_dot_imports() {
        let result = parse("source ./lib.sh\n. ../util.sh\n").unwrap();
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"./lib.sh"), "should find sourced lib.sh");
        assert!(paths.contains(&"../util.sh"), "should find dotted util.sh");
        assert!(
            result.imports.iter().all(|i| i.import_type == "Source"),
            "all imports should be Source"
        );
    }

    #[test]
    fn quoted_source_path_is_stripped() {
        let result = parse("source \"./config.sh\"\n").unwrap();
        assert_eq!(result.imports.len(), 1);
        assert_eq!(result.imports[0].path, "./config.sh");
    }

    #[test]
    fn ordinary_command_is_not_an_import() {
        let result = parse("echo hello\nls -la\n").unwrap();
        assert!(
            result.imports.is_empty(),
            "non-source commands must not import"
        );
    }

    #[test]
    fn env_prefixed_source_resolves_the_path_not_the_assignment() {
        let result = parse("FOO=1 source ./env.sh\n").unwrap();
        assert_eq!(result.imports.len(), 1);
        assert_eq!(result.imports[0].path, "./env.sh");
    }

    #[test]
    fn bare_source_without_argument_emits_no_import() {
        let result = parse("source\n").unwrap();
        assert!(result.imports.is_empty());
    }
}
