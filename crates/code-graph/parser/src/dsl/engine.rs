use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::definitions::DefinitionInfo;
use crate::parser::ParseResult;
use crate::utils::node_to_range;

use super::types::{
    DslDefinitionInfo, DslDefinitionType, DslFqn, DslImport, DslRawReference, LanguageSpec, Rule,
};

pub struct DslParseOutput {
    pub definitions: Vec<DslDefinitionInfo>,
    pub references: Vec<DslRawReference>,
    pub imports: Vec<DslImport>,
}

struct ScopeMatch {
    name: String,
    label: &'static str,
    range: crate::utils::Range,
    creates_scope: bool,
}

impl LanguageSpec {
    pub fn analyze(
        &self,
        parse_result: &ParseResult<'_, Root<StrDoc<SupportLang>>>,
    ) -> crate::Result<DslParseOutput> {
        let root_node = parse_result.ast.root();

        let mut definitions: Vec<DslDefinitionInfo> = Vec::new();
        let mut references: Vec<DslRawReference> = Vec::new();
        let mut imports: Vec<DslImport> = Vec::new();
        let mut scope_stack: Vec<String> = Vec::new();

        self.walk_node(
            &root_node,
            &mut scope_stack,
            &mut definitions,
            &mut references,
            &mut imports,
        );

        Ok(DslParseOutput {
            definitions,
            references,
            imports,
        })
    }

    fn walk_node(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        scope_stack: &mut Vec<String>,
        definitions: &mut Vec<DslDefinitionInfo>,
        references: &mut Vec<DslRawReference>,
        imports: &mut Vec<DslImport>,
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

        if let Some(imp) = self.evaluate_import(node, &node_kind) {
            imports.push(imp);
        }

        for child in node.children() {
            self.walk_node(&child, scope_stack, definitions, references, imports);
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
        if !self.is_scope_candidate(node_kind) {
            return None;
        }

        let rule = self
            .scopes
            .iter()
            .rev()
            .find(|r| r.matches(node, node_kind))?;

        let name = rule.extract_name(node)?;
        Some(ScopeMatch {
            name,
            label: rule.resolve_label(node),
            range: node_to_range(node),
            creates_scope: rule.creates_scope,
        })
    }

    fn evaluate_reference(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
    ) -> Option<(String, crate::utils::Range)> {
        let rule = self.refs.iter().find(|r| r.matches(node, node_kind))?;
        let name = rule.extract_name(node)?;
        Some((name, node_to_range(node)))
    }

    fn evaluate_import(
        &self,
        node: &Node<StrDoc<SupportLang>>,
        node_kind: &str,
    ) -> Option<DslImport> {
        let rule = self.imports.iter().find(|r| r.matches(node, node_kind))?;
        let path = rule.extract_name(node)?;
        Some(DslImport {
            path,
            name: rule.extract_symbol(node),
            alias: rule.extract_alias(node),
            range: node_to_range(node),
        })
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
        let spec = LanguageSpec::new(
            "test",
            vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "Method").when(grandparent_is("class_definition")),
            ],
            vec![],
            vec![],
        );
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "class A:\n    def b(self): pass\ndef c(): pass";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = spec.analyze(&result).unwrap();

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
        let spec = LanguageSpec::new(
            "test",
            vec![scope("function_definition", "Function")],
            vec![reference("call").name_from(field("function"))],
            vec![],
        );
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "def foo(): pass\nfoo()";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = spec.analyze(&result).unwrap();

        assert_eq!(output.references.len(), 1);
        assert_eq!(output.references[0].name, "foo");
    }

    #[test]
    fn test_no_scope_definition() {
        let spec = LanguageSpec::new(
            "test",
            vec![
                scope("class_definition", "Class"),
                scope("function_definition", "Function"),
                scope("function_definition", "FlatMethod")
                    .when(grandparent_is("class_definition"))
                    .no_scope(),
            ],
            vec![],
            vec![],
        );
        let parser = GenericParser::new(SupportedLanguage::Python);
        let code = "class A:\n    def method(self): pass";
        let result = parser.parse(code, Some("test.py")).unwrap();
        let output = spec.analyze(&result).unwrap();

        let method = output
            .definitions
            .iter()
            .find(|d| d.name == "method")
            .unwrap();
        assert_eq!(dsl_fqn_to_string(&method.fqn), "A.method");
        assert_eq!(method.definition_type.label, "FlatMethod");
    }
}
