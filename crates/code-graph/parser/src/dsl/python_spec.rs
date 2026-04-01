use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use super::extractors::field;
use super::predicates::*;
use super::types::{LanguageSpec, reference, scope, scope_fn};

pub fn python_language_spec() -> LanguageSpec {
    LanguageSpec::new(
        "python",
        vec![
            scope("class_definition", "Class"),
            scope("class_definition", "DecoratedClass").when(parent_is("decorated_definition")),
            scope_fn("function_definition", classify_function),
            scope("assignment", "Lambda")
                .when(field_descends(
                    "right",
                    &["parenthesized_expression"],
                    &["lambda"],
                    &["call"],
                ))
                .name_from(field("left"))
                .no_scope(),
        ],
        vec![reference("call_expression").name_from(field("function"))],
    )
}

fn classify_function(node: &Node<StrDoc<SupportLang>>) -> &'static str {
    let is_async = has_child(&["async"]).test(node);
    let has_decorator = parent_is("decorated_definition").test(node);
    let is_method = nearest_ancestor(
        &["class_definition", "function_definition"],
        &["class_definition"],
    )
    .test(node);

    match (is_method, is_async, has_decorator) {
        (true, true, true) => "DecoratedAsyncMethod",
        (true, true, false) => "AsyncMethod",
        (true, false, true) => "DecoratedMethod",
        (true, false, false) => "Method",
        (false, true, true) => "DecoratedAsyncFunction",
        (false, true, false) => "AsyncFunction",
        (false, false, true) => "DecoratedFunction",
        (false, false, false) => "Function",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::engine::DslParseOutput;
    use crate::dsl::types::dsl_fqn_to_string;
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};

    fn analyze(code: &str) -> DslParseOutput {
        let spec = python_language_spec();
        let parser = GenericParser::new(SupportedLanguage::Python);
        let result = parser.parse(code, Some("test.py")).unwrap();
        spec.analyze(&result).unwrap()
    }

    fn assert_def(output: &DslParseOutput, name: &str, label: &str, fqn: &str) {
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
        let code = "def outer():\n    def inner():\n        pass";
        let output = analyze(code);
        assert_eq!(output.definitions.len(), 2);
        assert_def(&output, "outer", "Function", "outer");
        assert_def(&output, "inner", "Function", "outer.inner");
    }

    #[test]
    fn test_nested_class() {
        let code = "class Outer:\n    class Inner:\n        pass";
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
        let code = "class MyClass:\n    def method(self):\n        def inner():\n            pass";
        let output = analyze(code);
        assert_def(&output, "inner", "Function", "MyClass.method.inner");
    }

    #[test]
    fn test_full_parity_with_existing_analyzer() {
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
