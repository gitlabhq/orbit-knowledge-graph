use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::utils::node_to_range;
use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::dsl::utils::canonical_range;
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, Fqn, ImportBindingKind, ImportMode, Range,
};

const MAX_STAGES_PER_FILE: usize = 1_000;
const MAX_VARIABLES_PER_FILE: usize = 10_000;
const MAX_NAME_BYTES: usize = 512;

type AstNode<'r> = Node<'r, StrDoc<SupportLang>>;

#[derive(Default)]
pub struct DockerfileDsl;

impl DslLanguage for DockerfileDsl {
    fn name() -> &'static str {
        "dockerfile"
    }

    fn language() -> Language {
        Language::Dockerfile
    }

    fn parser() -> LanguageParser {
        LanguageParser::Custom(parse_dockerfile)
    }
}

fn parse_dockerfile(
    source: &str,
    file_path: &str,
    budget: Option<Duration>,
) -> Result<CustomParse, String> {
    let ast = Language::Dockerfile.parse_ast(source, budget)?;
    let stem = Path::new(file_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(file_path);
    let mut parser = DockerfileParser::new(stem);
    for (index, instruction) in ast.root().children().filter(|n| n.is_named()).enumerate() {
        if parser.visit(index, &instruction).is_break() {
            break;
        }
    }
    Ok(parser.finish())
}

struct DockerfileParser<'a> {
    stem: &'a str,
    definitions: Vec<CanonicalDefinition>,
    imports: Vec<CanonicalImport>,
    stage_count: usize,
    variable_count: usize,
    stage: Option<Stage>,
}

struct Stage {
    name: String,
    end: Range,
    definition: usize,
}

impl<'a> DockerfileParser<'a> {
    fn new(stem: &'a str) -> Self {
        Self {
            stem,
            definitions: Vec::new(),
            imports: Vec::new(),
            stage_count: 0,
            variable_count: 0,
            stage: None,
        }
    }

    fn visit(&mut self, index: usize, instruction: &AstNode<'_>) -> ControlFlow<()> {
        match instruction.kind().as_ref() {
            "from_instruction" => return self.visit_from(index, instruction),
            "arg_instruction" => self.visit_variables(instruction, "arg_pair"),
            "env_instruction" => self.visit_variables(instruction, "env_pair"),
            _ => {}
        }
        if instruction.kind() != "comment"
            && let Some(stage) = &mut self.stage
        {
            stage.end = canonical_range(&node_to_range(instruction));
        }
        ControlFlow::Continue(())
    }

    fn visit_from(&mut self, index: usize, instruction: &AstNode<'_>) -> ControlFlow<()> {
        if self.stage_count >= MAX_STAGES_PER_FILE {
            return ControlFlow::Break(());
        }
        self.close_stage();
        let Some(spec) = instruction.child_of_kind("image_spec") else {
            return ControlFlow::Continue(());
        };
        let image = spec
            .field("name")
            .map(|n| truncate_name(&n.text()))
            .unwrap_or_default();
        let name = match instruction.field("as") {
            Some(alias) => truncate_name(&alias.text()),
            // The index counts comments and parser directives, as in the incremental engine.
            None => format!("stage:{image}#{index}"),
        };
        let range = canonical_range(&node_to_range(instruction));
        let fqn = Fqn::from_segments(&[self.stem, &name], ".");
        self.imports.push(CanonicalImport {
            import_type: "AliasedImport",
            binding_kind: ImportBindingKind::Named,
            mode: ImportMode::Declarative,
            path: image.clone(),
            name: Some(image.clone()),
            alias: Some(format!("image:{image}")),
            scope_fqn: Some(fqn.clone()),
            range: canonical_range(&node_to_range(&spec)),
            is_type_only: false,
            wildcard: false,
        });
        self.stage = Some(Stage {
            name: name.clone(),
            end: range,
            definition: self.definitions.len(),
        });
        self.definitions.push(CanonicalDefinition {
            definition_type: "Class",
            kind: DefKind::Class,
            name,
            fqn,
            range,
            is_top_level: true,
            metadata: None,
        });
        self.stage_count += 1;
        ControlFlow::Continue(())
    }

    fn visit_variables(&mut self, instruction: &AstNode<'_>, pair_kind: &str) {
        for pair in instruction.children().filter(|n| n.kind() == pair_kind) {
            if self.variable_count >= MAX_VARIABLES_PER_FILE {
                return;
            }
            let Some(name_node) = pair.field("name") else {
                continue;
            };
            let name = truncate_name(&name_node.text());
            let fqn = match &self.stage {
                Some(stage) => Fqn::from_segments(&[self.stem, &stage.name, &name], "."),
                None => Fqn::from_segments(&[self.stem, &name], "."),
            };
            self.definitions.push(CanonicalDefinition {
                definition_type: "Variable",
                kind: DefKind::Other,
                name,
                fqn,
                range: canonical_range(&node_to_range(&pair)),
                is_top_level: self.stage.is_none(),
                metadata: None,
            });
            self.variable_count += 1;
        }
    }

    fn close_stage(&mut self) {
        let Some(stage) = self.stage.take() else {
            return;
        };
        let definition = &mut self.definitions[stage.definition];
        definition.range = Range::new(
            definition.range.start,
            stage.end.end,
            (definition.range.byte_offset.0, stage.end.byte_offset.1),
        );
    }

    fn finish(mut self) -> CustomParse {
        self.close_stage();
        CustomParse {
            definitions: self.definitions,
            imports: self.imports,
            ..Default::default()
        }
    }
}

fn truncate_name(name: &str) -> String {
    if name.len() <= MAX_NAME_BYTES {
        return name.to_string();
    }
    let mut end = MAX_NAME_BYTES;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::dsl::engine::{ParseFullError, ParseFullResult, PhaseTimeouts};
    use crate::v2::error::AbortPhase;
    use crate::v2::trace::Tracer;

    fn parse(code: &str) -> ParseFullResult {
        DockerfileDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Dockerfile",
                Language::Dockerfile,
                &Tracer::new(false),
                Default::default(),
            )
            .ok()
            .unwrap()
    }

    fn names(result: &ParseFullResult, definition_type: &str) -> Vec<String> {
        result
            .definitions
            .iter()
            .filter(|d| d.definition_type == definition_type)
            .map(|d| d.fqn.to_string())
            .collect()
    }

    #[test]
    fn stages_and_variables_become_definitions_under_their_stage() {
        let result = parse(
            "ARG GO=1.22\nFROM golang:${GO} AS builder\nENV CGO=0 OS=linux\n\nFROM alpine AS runtime\nARG USER=app\n",
        );
        assert_eq!(
            names(&result, "Class"),
            ["Dockerfile.builder", "Dockerfile.runtime"]
        );
        assert_eq!(
            names(&result, "Variable"),
            [
                "Dockerfile.GO",
                "Dockerfile.builder.CGO",
                "Dockerfile.builder.OS",
                "Dockerfile.runtime.USER"
            ]
        );
        let builder = &result.definitions[1];
        assert_eq!(builder.fqn.parent().unwrap().to_string(), "Dockerfile");
        assert_eq!(
            result.definitions[2].fqn.parent().unwrap().to_string(),
            "Dockerfile.builder"
        );
        assert!(result.definitions[0].is_top_level && !result.definitions[2].is_top_level);
    }

    #[test]
    fn a_stage_spans_from_its_from_to_its_last_instruction() {
        let source = "FROM a AS one\nRUN x\nFROM b AS two\n";
        let result = parse(source);
        let (one, two) = (&result.definitions[0], &result.definitions[1]);
        assert_eq!(one.range.byte_offset.0, 0);
        assert!(one.range.byte_offset.1 <= two.range.byte_offset.0 + 1);
        assert_eq!(two.range.byte_offset.1, source.trim_end().len());
    }

    #[test]
    fn base_images_become_aliased_imports_in_their_stage() {
        let result = parse(
            "FROM --platform=$BP registry.gitlab.com/g/p/base:1.0@sha256:abc AS build\nFROM golang:1.22\n",
        );
        let imports: Vec<(&str, Option<&str>, String)> = result
            .imports
            .iter()
            .map(|i| {
                (
                    i.path.as_str(),
                    i.alias.as_deref(),
                    i.scope_fqn.as_ref().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            imports,
            [
                (
                    "registry.gitlab.com/g/p/base",
                    Some("image:registry.gitlab.com/g/p/base"),
                    "Dockerfile.build".to_string()
                ),
                (
                    "golang",
                    Some("image:golang"),
                    "Dockerfile.stage:golang#1".to_string()
                ),
            ]
        );
        assert!(
            result
                .imports
                .iter()
                .all(|i| i.import_type == "AliasedImport")
        );
    }

    #[test]
    fn unnamed_stage_index_counts_comments_like_the_incremental_engine() {
        let result = parse("# syntax=docker/dockerfile:1\n# base\nFROM node:20\n");
        assert_eq!(names(&result, "Class"), ["Dockerfile.stage:node#2"]);
    }

    #[test]
    fn empty_and_comment_only_files_yield_nothing() {
        for source in ["", "\n\n", "# only a comment\n# another\n"] {
            let result = parse(source);
            assert!(result.definitions.is_empty() && result.imports.is_empty());
        }
    }

    #[test]
    fn stages_past_the_cap_are_dropped_and_earlier_ones_are_kept() {
        let source: String = (0..MAX_STAGES_PER_FILE + 50)
            .map(|i| format!("FROM img AS s{i}\nENV V{i}=1\n"))
            .collect();
        let result = parse(&source);
        assert_eq!(names(&result, "Class").len(), MAX_STAGES_PER_FILE);
        assert_eq!(result.imports.len(), MAX_STAGES_PER_FILE);
        let last = format!(
            "Dockerfile.s{}.V{}",
            MAX_STAGES_PER_FILE - 1,
            MAX_STAGES_PER_FILE - 1
        );
        assert_eq!(names(&result, "Variable").last().unwrap(), &last);
    }

    #[test]
    fn variables_past_the_cap_are_dropped() {
        let pairs: String = (0..MAX_VARIABLES_PER_FILE + 50)
            .map(|i| format!(" V{i}=1"))
            .collect();
        let result = parse(&format!("FROM img AS s\nENV{pairs}\n"));
        assert_eq!(names(&result, "Variable").len(), MAX_VARIABLES_PER_FILE);
        assert_eq!(names(&result, "Class"), ["Dockerfile.s"]);
    }

    #[test]
    fn long_names_are_truncated_on_a_char_boundary() {
        let long = "é".repeat(MAX_NAME_BYTES);
        let truncated = truncate_name(&long);
        assert_eq!(truncated.len(), MAX_NAME_BYTES);
        assert!(truncated.chars().all(|c| c == 'é'));
        let odd = format!("a{long}");
        assert_eq!(truncate_name(&odd).len(), MAX_NAME_BYTES - 1);
    }

    #[test]
    fn long_stage_names_are_truncated() {
        let long = "a".repeat(MAX_NAME_BYTES + 100);
        let result = parse(&format!("FROM img AS {long}\n"));
        assert_eq!(result.definitions[0].name.len(), MAX_NAME_BYTES);
    }

    #[test]
    fn deeply_nested_expansions_do_not_overflow_a_small_stack() {
        let nested = "${A:-".repeat(100_000) + &"}".repeat(100_000);
        let source = format!("FROM img AS first\nENV A={nested}\nFROM img AS second\n");
        let result = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || parse(&source))
            .unwrap()
            .join()
            .unwrap();
        assert!(names(&result, "Class").contains(&"Dockerfile.first".to_string()));
    }

    #[test]
    fn trailing_comments_stay_out_of_the_stage_range() {
        let source = "FROM img AS s\nRUN x\n# about the next stage\nFROM img AS t\n";
        let result = parse(source);
        assert_eq!(
            result.definitions[0].range.byte_offset.1,
            "FROM img AS s\nRUN x".len()
        );
    }

    #[test]
    fn an_exhausted_parse_budget_aborts_instead_of_returning_an_empty_file() {
        let outcome = DockerfileDsl::spec().parse_full_collect(
            "FROM img AS s\n".repeat(1_000).as_bytes(),
            "Dockerfile",
            Language::Dockerfile,
            &Tracer::new(false),
            PhaseTimeouts {
                parse: Some(Duration::ZERO),
                ..Default::default()
            },
        );
        assert!(matches!(
            outcome,
            Err(ParseFullError::Aborted {
                phase: AbortPhase::Parse,
                ..
            })
        ));
    }
}
