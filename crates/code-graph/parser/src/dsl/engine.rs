use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::definitions::DefinitionInfo;
use crate::parser::ParseResult;
use crate::utils::node_to_range;

use super::types::{DslDefinitionInfo, DslDefinitionType, DslFqn, DslRawReference, LanguageSpec};

pub struct DslParseOutput {
    pub definitions: Vec<DslDefinitionInfo>,
    pub references: Vec<DslRawReference>,
}

struct ScopeMatch {
    name: String,
    label: &'static str,
    range: crate::utils::Range,
    creates_scope: bool,
}

pub struct DslAnalyzer<'spec> {
    spec: &'spec LanguageSpec,
}

impl<'spec> DslAnalyzer<'spec> {
    pub fn new(spec: &'spec LanguageSpec) -> Self {
        Self { spec }
    }

    pub fn analyze(
        &self,
        parse_result: &ParseResult<'_, Root<StrDoc<SupportLang>>>,
    ) -> crate::Result<DslParseOutput> {
        let root_node = parse_result.ast.root();

        let mut definitions: Vec<DslDefinitionInfo> = Vec::new();
        let mut references: Vec<DslRawReference> = Vec::new();
        let mut scope_stack: Vec<String> = Vec::new();

        self.walk_node(
            &root_node,
            &mut scope_stack,
            &mut definitions,
            &mut references,
        );

        Ok(DslParseOutput {
            definitions,
            references,
        })
    }

    fn walk_node(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        scope_stack: &mut Vec<String>,
        definitions: &mut Vec<DslDefinitionInfo>,
        references: &mut Vec<DslRawReference>,
    ) {
        if stacker::remaining_stack().unwrap_or(usize::MAX) < crate::MINIMUM_STACK_REMAINING {
            return;
        }

        let node_kind = node.kind();
        let mut pushed_scope = false;

        if let Some(m) = self.evaluate_scope(node, &node_kind) {
            if m.creates_scope {
                scope_stack.push(m.name.clone());
                pushed_scope = true;
            }

            let fqn = if m.creates_scope {
                DslFqn::new(scope_stack.clone())
            } else {
                let mut parts = scope_stack.clone();
                parts.push(m.name.clone());
                DslFqn::new(parts)
            };

            definitions.push(DefinitionInfo::new(
                DslDefinitionType { label: m.label },
                m.name,
                fqn,
                m.range,
            ));
        }

        if let Some((name, range)) = self.evaluate_reference(node, &node_kind) {
            references.push(DslRawReference { name, range });
        }

        for child in node.children() {
            self.walk_node(&child, scope_stack, definitions, references);
        }

        if pushed_scope {
            scope_stack.pop();
        }
    }

    fn evaluate_scope(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
    ) -> Option<ScopeMatch> {
        if !self.spec.is_scope_candidate(node_kind) {
            return None;
        }

        // Last matching rule wins.
        let rule = self
            .spec
            .scopes
            .iter()
            .rev()
            .find(|r| r.matches(node, node_kind))?;

        let name = rule.name.extract_name(node)?;
        let range = node_to_range(node);
        let label = rule.resolve_label(node);
        Some(ScopeMatch {
            name,
            label,
            range,
            creates_scope: rule.creates_scope,
        })
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
    ) -> Option<(String, crate::utils::Range)> {
        let rule = self.spec.refs.iter().find(|r| r.matches(node, node_kind))?;
        let name = rule.name.extract_name(node)?;
        Some((name, node_to_range(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::extractors::field;
    use crate::dsl::predicates::*;
    use crate::dsl::types::*;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    #[test]
    fn test_scope_matching_and_fqn() {
        let spec = LanguageSpec {
            name: "test",
            scopes: vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "Method").when(grandparent_is("class_definition")),
            ],
            refs: vec![],
        };
        let analyzer = DslAnalyzer::new(&spec);
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "class A:\n    def b(self): pass\ndef c(): pass";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = analyzer.analyze(&result).unwrap();

        assert_eq!(output.definitions.len(), 3);

        let b = output.definitions.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type.label, "Method");
        assert_eq!(dsl_fqn_to_string(&b.fqn), "A.b");

        let c = output.definitions.iter().find(|d| d.name == "c").unwrap();
        assert_eq!(c.definition_type.label, "Function");
        assert_eq!(dsl_fqn_to_string(&c.fqn), "c");
    }

    #[test]
    fn test_reference_extraction() {
        let spec = LanguageSpec {
            name: "test",
            scopes: vec![scope("function_definition", "Function")],
            // Python uses "call" not "call_expression"
            refs: vec![reference("call").name_from(field("function"))],
        };
        let analyzer = DslAnalyzer::new(&spec);
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "def foo(): pass\nfoo()";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = analyzer.analyze(&result).unwrap();

        assert_eq!(output.references.len(), 1);
        assert_eq!(output.references[0].name, "foo");
    }

    #[test]
    fn test_no_scope_definition() {
        // Only inner is .no_scope() — outer still pushes scope normally.
        let spec = LanguageSpec {
            name: "test",
            scopes: vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                // Methods inside classes don't push scope
                scope("function_definition", "FlatMethod")
                    .when(grandparent_is("class_definition"))
                    .no_scope(),
            ],
            refs: vec![],
        };
        let analyzer = DslAnalyzer::new(&spec);
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "class A:\n    def method(self): pass";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = analyzer.analyze(&result).unwrap();

        let method = output
            .definitions
            .iter()
            .find(|d| d.name == "method")
            .unwrap();
        // Method gets A.method FQN but doesn't push scope
        assert_eq!(dsl_fqn_to_string(&method.fqn), "A.method");
        assert_eq!(method.definition_type.label, "FlatMethod");
    }
}
