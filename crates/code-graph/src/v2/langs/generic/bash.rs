use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::extract::field;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::linker::{HasRules, ResolveSettings};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

#[derive(Default)]
pub struct BashDsl;

impl DslLanguage for BashDsl {
    fn name() -> &'static str {
        "bash"
    }

    fn language() -> Language {
        Language::Bash
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(bash_extract_imports),
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .name_from(field("name")),
        ]
    }

    fn file_scope() -> bool {
        true
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig::default()
    }
}

fn bash_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "command" {
        return false;
    }
    let Some(command) = node.field("name") else {
        return false;
    };
    if command.text().as_ref() != "source" && command.text().as_ref() != "." {
        return false;
    }
    let path = node
        .field("argument")
        .map(|arg| strip_quotes(arg.text().as_ref()).to_string());
    if let Some(path) = path.filter(|p| !p.is_empty()) {
        imports.push(CanonicalImport {
            import_type: "Source",
            binding_kind: ImportBindingKind::SideEffect,
            mode: ImportMode::Runtime,
            path,
            name: None,
            alias: None,
            scope_fqn: None,
            range: crate::v2::types::Range::empty(),
            is_type_only: false,
            wildcard: false,
        });
    }
    true
}

fn strip_quotes(s: &str) -> &str {
    s.trim_matches(|c| c == '"' || c == '\'')
}

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
