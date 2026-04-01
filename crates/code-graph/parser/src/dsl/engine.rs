use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::definitions::DefinitionInfo;
use crate::parser::ParseResult;
use crate::utils::node_to_range;

use super::extractors::{DefaultNameExtractor, NameExtractor};
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

        if let Some((name, label, range)) = self.evaluate_scope(node, &node_kind) {
            scope_stack.push(name.clone());
            pushed_scope = true;

            definitions.push(DefinitionInfo::new(
                DslDefinitionType { label },
                name,
                DslFqn::new(scope_stack.clone()),
                range,
            ));
        }

        if let Some((name, range)) = self.evaluate_reference(node) {
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

    /// Returns `(name, label, range)` if the node creates a scope.
    fn evaluate_scope(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
    ) -> Option<(String, String, crate::utils::Range)> {
        if !self.spec.is_scope_candidate(node_kind) {
            return None;
        }

        // Last matching rule wins.
        let matched_rule = self
            .spec
            .scope_rules
            .iter()
            .rev()
            .find(|r| r.predicate.test(node));

        if let Some(rule) = matched_rule {
            let name = rule.get_name_extractor().extract_name(node)?;
            let range = rule.get_range_extractor().extract_range(node);
            let label = rule.label.unwrap_or(node_kind).to_string();
            return Some((name, label, range));
        }

        // Node kind is in the corpus but no explicit rule matched — auto-scope.
        let name = DefaultNameExtractor.extract_name(node)?;
        let range = node_to_range(node);
        Some((name, node_kind.to_string(), range))
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
    ) -> Option<(String, crate::utils::Range)> {
        for rule in &self.spec.reference_rules {
            if rule.predicate.test(node) {
                let name = rule.get_name_extractor().extract_name(node)?;
                return Some((name, node_to_range(node)));
            }
        }
        None
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
            scope_rules: vec![
                ScopeRule::new(Box::new(kind_eq("class_definition"))).with_label("Class"),
                // "Function" is the general rule; "Method" is more specific
                // and comes later so it overrides for class-contained functions.
                ScopeRule::new(Box::new(kind_eq("function_definition"))).with_label("Function"),
                ScopeRule::new(Box::new(
                    kind_eq("function_definition").and(grandparent_kind("class_definition")),
                ))
                .with_label("Method"),
            ],
            reference_rules: vec![],
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

        // Check FQNs
        let fqns: Vec<String> = analysis
            .definitions
            .iter()
            .map(|d| dsl_fqn_to_string(&d.fqn))
            .collect();
        assert!(fqns.contains(&"Calculator".to_string()));
        assert!(fqns.contains(&"Calculator.add".to_string()));
        assert!(fqns.contains(&"Calculator.subtract".to_string()));
        assert!(fqns.contains(&"standalone".to_string()));

        // Check labels
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
        // Both rules match function_definition, the second (Method) should win
        // when inside a class, but the first (Function) should win otherwise.
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
