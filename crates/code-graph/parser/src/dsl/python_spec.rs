use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::extractors::{NameExtractor, extract_from_field};
use super::predicates::*;
use super::types::{LanguageSpec, ReferenceRule, ScopeRule};

/// Python language specification for the DSL engine.
///
/// Replicates the definition extraction from `python/fqn.rs::build_fqn_index`
/// using declarative rules instead of hand-written DFS logic.
///
/// Handles: classes, decorated classes, functions, async functions,
/// decorated functions, methods (instance, async, decorated, decorated async),
/// and lambda assignments.
pub fn python_language_spec() -> LanguageSpec {
    LanguageSpec {
        name: "python",
        scope_corpus: &["class_definition", "function_definition", "assignment"],
        scope_rules: vec![
            // --- Classes ---
            // Plain class (general rule, overridden by decorated variant)
            ScopeRule::new(Box::new(kind_eq("class_definition"))).with_label("Class"),
            // Decorated class: parent is `decorated_definition`
            ScopeRule::new(Box::new(
                kind_eq("class_definition").and(parent_kind("decorated_definition")),
            ))
            .with_label("DecoratedClass"),
            // --- Functions/Methods ---
            // General function (least specific, overridden by more specific rules below)
            ScopeRule::new(Box::new(kind_eq("function_definition"))).with_label("Function"),
            // Async function (no decorator, not in class)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild)
                    .and(grandparent_kind("class_definition").not()),
            ))
            .with_label("AsyncFunction"),
            // Decorated function (not in class)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(parent_kind("decorated_definition"))
                    .and(IsNotInClassScope),
            ))
            .with_label("DecoratedFunction"),
            // Decorated async function (not in class)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild)
                    .and(parent_kind("decorated_definition"))
                    .and(IsNotInClassScope),
            ))
            .with_label("DecoratedAsyncFunction"),
            // Method (in class, no decorator, not async)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild.not())
                    .and(parent_kind("decorated_definition").not())
                    .and(IsInClassScope),
            ))
            .with_label("Method"),
            // Async method (in class, no decorator)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild)
                    .and(parent_kind("decorated_definition").not())
                    .and(IsInClassScope),
            ))
            .with_label("AsyncMethod"),
            // Decorated method (in class, has decorator, not async)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild.not())
                    .and(parent_kind("decorated_definition"))
                    .and(IsInClassScope),
            ))
            .with_label("DecoratedMethod"),
            // Decorated async method (in class, has decorator, is async)
            ScopeRule::new(Box::new(
                kind_eq("function_definition")
                    .and(HasAsyncChild)
                    .and(parent_kind("decorated_definition"))
                    .and(IsInClassScope),
            ))
            .with_label("DecoratedAsyncMethod"),
            // --- Lambdas ---
            // `x = lambda ...` — creates a definition but NOT a scope
            ScopeRule::new(Box::new(kind_eq("assignment").and(IsLambdaAssignment)))
                .with_label("Lambda")
                .with_name_extractor(Box::new(LambdaAssignmentNameExtractor))
                .no_scope(),
        ],
        reference_rules: vec![
            // call_expression: foo(), obj.method(), etc.
            ReferenceRule::new(Box::new(kind_eq("call_expression")), "Call")
                .with_name_extractor(Box::new(extract_from_field("function"))),
        ],
    }
}

/// Checks if a `function_definition` has an `async` keyword child.
struct HasAsyncChild;

impl Predicate for HasAsyncChild {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        node.children().any(|c| c.kind() == "async")
    }
}

/// True when the function_definition's grandparent (skipping block) is a class.
/// In Python's tree-sitter: class_definition > body: block > function_definition
/// With decorators: class_definition > body: block > decorated_definition > function_definition
struct IsInClassScope;

impl Predicate for IsInClassScope {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        // Walk up to find the nearest class or function ancestor (skipping block, decorated_definition)
        let mut current = node.parent();
        while let Some(ancestor) = current {
            let kind = ancestor.kind();
            if kind == "class_definition" {
                return true;
            }
            // If we hit another function first, we're nested inside a function not a class
            if kind == "function_definition" {
                return false;
            }
            current = ancestor.parent();
        }
        false
    }
}

struct IsNotInClassScope;

impl Predicate for IsNotInClassScope {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        !IsInClassScope.test(node)
    }
}

/// Checks if an assignment's RHS is a lambda (possibly parenthesized).
struct IsLambdaAssignment;

impl Predicate for IsLambdaAssignment {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        if let Some(right) = node.field("right") {
            is_lambda_rhs(&right)
        } else {
            false
        }
    }
}

fn is_lambda_rhs(node: &Node<StrDoc<SupportLang>>) -> bool {
    let kind = node.kind();
    if kind == "lambda" {
        return true;
    }
    if kind == "call" {
        return false;
    }
    if kind == "parenthesized_expression"
        && let Some(inner) = node.child(0)
    {
        return is_lambda_rhs(&inner);
    }
    false
}

/// Extracts the name from a lambda assignment's LHS.
/// Handles both simple identifiers (`x = lambda ...`) and
/// attribute access (`self.attr = lambda ...`).
struct LambdaAssignmentNameExtractor;

impl NameExtractor for LambdaAssignmentNameExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        let left = node.field("left")?;
        let kind = left.kind();
        if kind == "attribute" {
            Some(extract_attribute_path(&left))
        } else {
            Some(left.text().to_string())
        }
    }
}

fn extract_attribute_path(node: &Node<StrDoc<SupportLang>>) -> String {
    if node.kind() == "attribute" {
        let mut parts = Vec::new();
        if let Some(object) = node.field("object") {
            parts.push(extract_attribute_path(&object));
        }
        if let Some(attribute) = node.field("attribute") {
            parts.push(attribute.text().to_string());
        }
        parts.join(".")
    } else {
        node.text().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::engine::DslAnalyzer;
    use crate::dsl::types::dsl_fqn_to_string;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    fn analyze(code: &str) -> crate::dsl::engine::DslParseOutput {
        let spec = python_language_spec();
        let analyzer = DslAnalyzer::new(&spec);
        let parser = GenericParser::new(SupportedLanguage::Python);
        let result = parser.parse(code, Some("test.py")).unwrap();
        analyzer.analyze(&result).unwrap()
    }

    fn assert_def(output: &crate::dsl::engine::DslParseOutput, name: &str, label: &str, fqn: &str) {
        let def = output
            .definitions
            .iter()
            .find(|d| d.name == name)
            .unwrap_or_else(|| panic!("Definition '{name}' not found"));
        assert_eq!(
            def.definition_type.label, label,
            "Label mismatch for '{name}': expected '{label}', got '{}'",
            def.definition_type.label
        );
        assert_eq!(
            dsl_fqn_to_string(&def.fqn),
            fqn,
            "FQN mismatch for '{name}'"
        );
    }

    #[test]
    fn test_simple_function() {
        let output = analyze("def foo(x): pass");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "foo", "Function", "foo");
    }

    #[test]
    fn test_async_function() {
        let output = analyze("async def bar(): pass");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "bar", "AsyncFunction", "bar");
    }

    #[test]
    fn test_decorated_function() {
        let output = analyze("@deco\ndef baz(): pass");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "baz", "DecoratedFunction", "baz");
    }

    #[test]
    fn test_decorated_async_function() {
        let output = analyze("@deco\nasync def qux(): pass");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "qux", "DecoratedAsyncFunction", "qux");
    }

    #[test]
    fn test_class_and_methods() {
        let code = r#"
class MyClass:
    def method(self):
        pass

    async def async_method(self):
        pass

    @deco
    def decorated_method(self):
        pass

    @deco
    async def decorated_async_method(self):
        pass
"#;
        let output = analyze(code);
        assert_eq!(output.definitions.len(), 5);
        assert_def(&output, "MyClass", "Class", "MyClass");
        assert_def(&output, "method", "Method", "MyClass.method");
        assert_def(
            &output,
            "async_method",
            "AsyncMethod",
            "MyClass.async_method",
        );
        assert_def(
            &output,
            "decorated_method",
            "DecoratedMethod",
            "MyClass.decorated_method",
        );
        assert_def(
            &output,
            "decorated_async_method",
            "DecoratedAsyncMethod",
            "MyClass.decorated_async_method",
        );
    }

    #[test]
    fn test_decorated_class() {
        let output = analyze("@dataclass\nclass Foo:\n    x: int");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "Foo", "DecoratedClass", "Foo");
    }

    #[test]
    fn test_nested_functions() {
        let code = r#"
def outer():
    def inner():
        pass
"#;
        let output = analyze(code);
        assert_eq!(output.definitions.len(), 2);
        assert_def(&output, "outer", "Function", "outer");
        assert_def(&output, "inner", "Function", "outer.inner");
    }

    #[test]
    fn test_nested_class() {
        let code = r#"
class Outer:
    class Inner:
        pass
"#;
        let output = analyze(code);
        assert_eq!(output.definitions.len(), 2);
        assert_def(&output, "Outer", "Class", "Outer");
        assert_def(&output, "Inner", "Class", "Outer.Inner");
    }

    #[test]
    fn test_lambda_assignment() {
        let output = analyze("my_fn = lambda x: x * 2");
        assert_eq!(output.definitions.len(), 1);
        assert_def(&output, "my_fn", "Lambda", "my_fn");
    }

    #[test]
    fn test_lambda_in_method() {
        let code = r#"
class MyClass:
    def method(self):
        self.attr_lambda = lambda x: x * 2
"#;
        let output = analyze(code);
        assert_eq!(output.definitions.len(), 3);
        assert_def(&output, "MyClass", "Class", "MyClass");
        assert_def(&output, "method", "Method", "MyClass.method");
        assert_def(
            &output,
            "self.attr_lambda",
            "Lambda",
            "MyClass.method.self.attr_lambda",
        );
    }

    #[test]
    fn test_method_inside_nested_function_is_not_method() {
        let code = r#"
class MyClass:
    def method(self):
        def inner():
            pass
"#;
        let output = analyze(code);
        assert_def(&output, "inner", "Function", "MyClass.method.inner");
    }

    #[test]
    fn test_full_parity_with_existing_analyzer() {
        // Replicates the exact test from python/definitions.rs::test_method_definitions
        let code = r#"
class MyClass:
    def method(self):
        self.attr_lambda = lambda x: x * 2

    async def async_method(self):
        pass

    def nested_method(self):
        def inner_function():
            pass

    @classmethod
    def decorated_method(cls):
        pass

    @classmethod
    async def decorated_async_method(cls):
        pass

    lambda_method = lambda self: self.class_var * 2
"#;
        let output = analyze(code);
        assert_eq!(
            output.definitions.len(),
            9,
            "Expected 9 definitions, got {}:\n{}",
            output.definitions.len(),
            output
                .definitions
                .iter()
                .map(|d| format!(
                    "  {} [{}] -> {}",
                    d.name,
                    d.definition_type.label,
                    dsl_fqn_to_string(&d.fqn)
                ))
                .collect::<Vec<_>>()
                .join("\n")
        );

        assert_def(&output, "MyClass", "Class", "MyClass");
        assert_def(&output, "method", "Method", "MyClass.method");
        assert_def(
            &output,
            "self.attr_lambda",
            "Lambda",
            "MyClass.method.self.attr_lambda",
        );
        assert_def(
            &output,
            "async_method",
            "AsyncMethod",
            "MyClass.async_method",
        );
        assert_def(&output, "nested_method", "Method", "MyClass.nested_method");
        assert_def(
            &output,
            "inner_function",
            "Function",
            "MyClass.nested_method.inner_function",
        );
        assert_def(
            &output,
            "decorated_method",
            "DecoratedMethod",
            "MyClass.decorated_method",
        );
        assert_def(
            &output,
            "decorated_async_method",
            "DecoratedAsyncMethod",
            "MyClass.decorated_async_method",
        );
        assert_def(&output, "lambda_method", "Lambda", "MyClass.lambda_method");
    }
}
