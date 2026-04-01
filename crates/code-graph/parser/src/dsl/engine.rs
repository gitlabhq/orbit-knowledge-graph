use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::definitions::DefinitionInfo;
use crate::parser::ParseResult;
use crate::utils::node_to_range;

use super::extractors::Extract;
use super::types::{DslDefinitionInfo, DslDefinitionType, DslFqn, DslRawReference, LanguageSpec};

pub struct DslParseOutput {
    pub definitions: Vec<DslDefinitionInfo>,
    pub references: Vec<DslRawReference>,
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

        if let Some((name, label, range, creates_scope)) = self.evaluate_scope(node, &node_kind) {
            if creates_scope {
                scope_stack.push(name.clone());
                pushed_scope = true;
            }

            let fqn = if creates_scope {
                DslFqn::new(scope_stack.clone())
            } else {
                let mut parts = scope_stack.clone();
                parts.push(name.clone());
                DslFqn::new(parts)
            };

            definitions.push(DefinitionInfo::new(
                DslDefinitionType { label },
                name,
                fqn,
                range,
            ));
        }

        if let Some((name, range)) = self.evaluate_reference(node, &node_kind) {
            references.push(DslRawReference {
                name,
                range,
                scope_fqn: if scope_stack.is_empty() {
                    None
                } else {
                    Some(DslFqn::new(scope_stack.clone()))
                },
            });
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
    ) -> Option<(String, String, crate::utils::Range, bool)> {
        if !self.spec.is_scope_candidate(node_kind) {
            return None;
        }

        // Last matching rule wins.
        let matched = self
            .spec
            .scopes
            .iter()
            .rev()
            .find(|r| r.matches(node, node_kind));

        if let Some(rule) = matched {
            let name = rule.name.extract_name(node)?;
            let range = rule.range.extract_range(node);
            let label = rule.resolve_label(node).to_string();
            return Some((name, label, range, rule.creates_scope));
        }

        // Auto-scope only when no rules are defined at all.
        if !self.spec.scopes.is_empty() {
            return None;
        }
        let name = Extract::Default.extract_name(node)?;
        let range = node_to_range(node);
        Some((name, node_kind.to_string(), range, true))
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
    use crate::dsl::predicates::*;
    use crate::dsl::types::*;
    use crate::parser::GenericParser;
    use crate::parser::LanguageParser;

    fn python_spec() -> LanguageSpec {
        LanguageSpec {
            name: "python",
            scope_corpus: &["class_definition", "function_definition"],
            scopes: vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "Method").when(grandparent_is("class_definition")),
            ],
            refs: vec![],
        }
    }

    #[test]
    fn test_python_definitions_via_dsl() {
        let spec = python_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(crate::parser::SupportedLanguage::Python);
        let code = r#"
class Calculator:
    def add(self, a, b):
        return a + b

    def subtract(self, a, b):
        return a - b

def standalone():
    pass
"#;
        let result = parser.parse(code, Some("test.py")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        assert_eq!(analysis.definitions.len(), 4);

        let names: Vec<&str> = analysis
            .definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        assert!(names.contains(&"Calculator"));
        assert!(names.contains(&"add"));
        assert!(names.contains(&"subtract"));
        assert!(names.contains(&"standalone"));

        let fqns: Vec<String> = analysis
            .definitions
            .iter()
            .map(|d| dsl_fqn_to_string(&d.fqn))
            .collect();
        assert!(fqns.contains(&"Calculator".to_string()));
        assert!(fqns.contains(&"Calculator.add".to_string()));
        assert!(fqns.contains(&"Calculator.subtract".to_string()));
        assert!(fqns.contains(&"standalone".to_string()));

        let calc = analysis
            .definitions
            .iter()
            .find(|d| d.name == "Calculator")
            .unwrap();
        assert_eq!(calc.definition_type.label, "Class");

        let add = analysis
            .definitions
            .iter()
            .find(|d| d.name == "add")
            .unwrap();
        assert_eq!(add.definition_type.label, "Method");

        let standalone = analysis
            .definitions
            .iter()
            .find(|d| d.name == "standalone")
            .unwrap();
        assert_eq!(standalone.definition_type.label, "Function");
    }

    #[test]
    fn test_last_rule_wins() {
        let spec = python_spec();
        let analyzer = DslAnalyzer::new(&spec);

        let parser = GenericParser::new(crate::parser::SupportedLanguage::Python);
        let code = "class A:\n    def b(self): pass\ndef c(): pass";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let analysis = analyzer.analyze(&result).unwrap();

        let b = analysis.definitions.iter().find(|d| d.name == "b").unwrap();
        assert_eq!(b.definition_type.label, "Method");

        let c = analysis.definitions.iter().find(|d| d.name == "c").unwrap();
        assert_eq!(c.definition_type.label, "Function");
    }
}
