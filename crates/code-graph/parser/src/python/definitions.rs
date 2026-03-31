use crate::python::types::PythonDefinitionInfo;

/// Type alias for Python definitions - these are extracted during FQN traversal
pub type PythonDefinitions = Vec<PythonDefinitionInfo>;

#[cfg(test)]
mod definition_tests {
    use crate::parser::{GenericParser, LanguageParser, SupportedLanguage};
    use crate::python::fqn::build_fqn_index;
    use crate::python::fqn::python_fqn_to_string;
    use crate::python::types::PythonDefinitionType;

    fn test_definition_extraction(
        code: &str,
        expected_definitions: Vec<(&str, PythonDefinitionType, &str)>, // (name, type, expected_fqn)
        description: &str,
    ) {
        println!("\n=== Testing: {description} ===");
        println!("Code snippet:\n{code}");

        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let parse_result = parser.parse(code, Some("test.py")).unwrap();
        let (_node_fqn_map, definitions) = build_fqn_index(&parse_result.ast);

        println!("Found {} definitions:", definitions.len());
        for def in &definitions {
            let fqn_str = python_fqn_to_string(&def.fqn);
            println!("  {:?}: {} -> {}", def.definition_type, def.name, fqn_str);
        }

        assert_eq!(
            definitions.len(),
            expected_definitions.len(),
            "Expected {} definitions, found {}",
            expected_definitions.len(),
            definitions.len()
        );

        for (expected_name, expected_type, expected_fqn) in expected_definitions {
            let matching_def = definitions
                .iter()
                .find(|d| d.name == expected_name && d.definition_type == expected_type)
                .unwrap_or_else(|| {
                    panic!("Could not find definition: {expected_name} of type {expected_type:?}")
                });

            let actual_fqn = &matching_def.fqn;
            let actual_fqn_str = python_fqn_to_string(actual_fqn);
            assert_eq!(
                actual_fqn_str, expected_fqn,
                "FQN mismatch for {expected_name}: expected '{expected_fqn}', got '{actual_fqn_str}'"
            );
        }
        println!("✅ All assertions passed for: {description}\n");
    }

    #[test]
    fn test_simple_function_definition() {
        let code = r#"
def simple_function(x: int, y: str = "default") -> bool:
    return len(y) > x
        "#;
        let expected_definitions = vec![(
            "simple_function",
            PythonDefinitionType::Function,
            "simple_function",
        )];
        test_definition_extraction(code, expected_definitions, "Simple function definition");
    }

    #[test]
    fn test_generator_function_definition() {
        let code = r#"
def generator_function():
    yield 1
    yield 2
        "#;
        let expected_definitions = vec![(
            "generator_function",
            PythonDefinitionType::Function,
            "generator_function",
        )];
        test_definition_extraction(code, expected_definitions, "Generator definition");
    }

    #[test]
    fn test_decorated_function_definition() {
        let code = r#"
@staticmethod
@property
def decorated_function():
    pass
        "#;
        let expected_definitions = vec![(
            "decorated_function",
            PythonDefinitionType::DecoratedFunction,
            "decorated_function",
        )];
        test_definition_extraction(code, expected_definitions, "Decorated function definition");
    }

    #[test]
    fn test_async_function_definition() {
        let code = r#"
async def async_function():
    pass
        "#;
        let expected_definitions = vec![(
            "async_function",
            PythonDefinitionType::AsyncFunction,
            "async_function",
        )];
        test_definition_extraction(
            code,
            expected_definitions,
            "Asynchronous function definition",
        );
    }

    #[test]
    fn test_decorated_async_function_definition() {
        let code = r#"
@decorator
async def decorated_async_function():
    pass
        "#;
        let expected_definitions = vec![(
            "decorated_async_function",
            PythonDefinitionType::DecoratedAsyncFunction,
            "decorated_async_function",
        )];
        test_definition_extraction(
            code,
            expected_definitions,
            "Decorated asynchronous function definition",
        );
    }

    #[test]
    fn test_lambda_function_definition() {
        let code = r#"
module_lambda = lambda x: x * 2
        "#;
        let expected_definitions = vec![(
            "module_lambda",
            PythonDefinitionType::Lambda,
            "module_lambda",
        )];
        test_definition_extraction(code, expected_definitions, "Module-level lambda definition");
    }

    #[test]
    fn test_nested_function_definitions() {
        let code = r#"
def outer_function():
    def inner_function():
        pass
    
    inner_lambda = lambda x: x
    return inner_function
        "#;
        let expected_definitions = vec![
            (
                "outer_function",
                PythonDefinitionType::Function,
                "outer_function",
            ),
            (
                "inner_function",
                PythonDefinitionType::Function,
                "outer_function.inner_function",
            ),
            (
                "inner_lambda",
                PythonDefinitionType::Lambda,
                "outer_function.inner_lambda",
            ),
        ];
        test_definition_extraction(code, expected_definitions, "Nested function definitions");
    }

    #[test]
    fn test_simple_class_definition() {
        let code = r#"
class SimpleClass:
    class_var = 42
        "#;
        let expected_definitions =
            vec![("SimpleClass", PythonDefinitionType::Class, "SimpleClass")];
        test_definition_extraction(code, expected_definitions, "Simple class definition");
    }

    #[test]
    fn test_decorated_class_definition() {
        let code = r#"
from dataclasses import dataclass
@dataclass
class DecoratedClass:
    field: int
        "#;
        let expected_definitions = vec![(
            "DecoratedClass",
            PythonDefinitionType::DecoratedClass,
            "DecoratedClass",
        )];
        test_definition_extraction(code, expected_definitions, "Decorated class definition");
    }

    #[test]
    fn test_method_definitions() {
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
        let expected_definitions = vec![
            ("MyClass", PythonDefinitionType::Class, "MyClass"),
            ("method", PythonDefinitionType::Method, "MyClass.method"),
            (
                "self.attr_lambda",
                PythonDefinitionType::Lambda,
                "MyClass.method.self#attr_lambda",
            ),
            (
                "async_method",
                PythonDefinitionType::AsyncMethod,
                "MyClass.async_method",
            ),
            (
                "nested_method",
                PythonDefinitionType::Method,
                "MyClass.nested_method",
            ),
            (
                "inner_function",
                PythonDefinitionType::Function,
                "MyClass.nested_method.inner_function",
            ),
            (
                "decorated_method",
                PythonDefinitionType::DecoratedMethod,
                "MyClass.decorated_method",
            ),
            (
                "decorated_async_method",
                PythonDefinitionType::DecoratedAsyncMethod,
                "MyClass.decorated_async_method",
            ),
            (
                "lambda_method",
                PythonDefinitionType::Lambda,
                "MyClass.lambda_method",
            ),
        ];
        test_definition_extraction(code, expected_definitions, "Class method definitions");
    }

    #[test]
    fn test_nested_class_definitions() {
        let code = r#"
class NestedClass:
    class InnerClass:
        class_var = 42
        "#;
        let expected_definitions = vec![
            ("NestedClass", PythonDefinitionType::Class, "NestedClass"),
            (
                "InnerClass",
                PythonDefinitionType::Class,
                "NestedClass.InnerClass",
            ),
        ];
        test_definition_extraction(code, expected_definitions, "Class method definitions");
    }
}
