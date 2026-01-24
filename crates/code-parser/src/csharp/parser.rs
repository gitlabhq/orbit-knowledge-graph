use std::sync::Arc;

use crate::csharp::types::{
    CSharpDefinitionInfo, CSharpDefinitionType, CSharpDefinitions, CSharpFqn, CSharpFqnPart,
    CSharpFqnPartType, CSharpImportType, CSharpImports,
};
use crate::imports::{ImportIdentifier, ImportedSymbolInfo};
use crate::utils::node_to_range;
use smallvec::{SmallVec, smallvec};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

type ScopeStack = SmallVec<[CSharpFqnPart; 16]>;

/// Helper function to add children to stack in reverse order
fn push_children_reverse<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    stack: &mut Vec<Option<Node<'a, StrDoc<SupportLang>>>>,
) {
    let children: Vec<_> = node.children().collect();
    stack.reserve(children.len());
    for child in children.into_iter().rev() {
        stack.push(Some(child));
    }
}

/// Process a using directive node and create an ImportedSymbolInfo
fn process_using_directive(
    node: &Node<StrDoc<SupportLang>>,
    current_scope: &ScopeStack,
) -> Option<ImportedSymbolInfo<CSharpImportType, CSharpFqn>> {
    let node_text_cow = node.text();
    let node_text = node_text_cow.as_ref();

    // Determine if it's global and/or static
    let is_global = node_text.starts_with("global ");
    let is_static = node_text.contains(" static ");

    // Parse the using directive based on its structure
    let children: Vec<_> = node.children().collect();

    // Find identifier and qualified_name nodes
    let identifier_node = children.iter().find(|c| c.kind() == "identifier");
    let qualified_name_node = children.iter().find(|c| c.kind() == "qualified_name");

    let (import_path, import_identifier) =
        if let (Some(identifier), Some(qualified_name)) = (identifier_node, qualified_name_node) {
            // Alias case: using Console = System.Console;
            let alias_name = identifier.text().into_owned();
            let qualified_name_text = qualified_name.text().into_owned();

            let import_id = ImportIdentifier {
                name: qualified_name_text.clone(),
                alias: Some(alias_name),
            };
            (qualified_name_text, Some(import_id))
        } else if let Some(qualified_name) = qualified_name_node {
            // Regular case with qualified name: using System.Console;
            let qualified_name_text = qualified_name.text().into_owned();
            let import_id = ImportIdentifier {
                name: qualified_name_text.clone(),
                alias: None,
            };
            (qualified_name_text, Some(import_id))
        } else if let Some(identifier) = identifier_node {
            // Simple case: using System;
            let identifier_text = identifier.text().into_owned();
            let import_id = ImportIdentifier {
                name: identifier_text.clone(),
                alias: None,
            };
            (identifier_text, Some(import_id))
        } else {
            return None;
        };

    // Determine import type based on global and static flags
    let import_type = match (
        is_global,
        is_static,
        import_identifier.as_ref().and_then(|id| id.alias.as_ref()),
    ) {
        (true, true, _) => CSharpImportType::GlobalStatic,
        (true, false, Some(_)) => CSharpImportType::GlobalAlias,
        (true, false, None) => CSharpImportType::Global,
        (false, true, _) => CSharpImportType::Static,
        (false, false, Some(_)) => CSharpImportType::Alias,
        (false, false, None) => CSharpImportType::Default,
    };

    Some(ImportedSymbolInfo::new(
        import_type,
        import_path,
        import_identifier,
        node_to_range(node),
        if current_scope.is_empty() {
            None
        } else {
            Some(Arc::new(current_scope.clone()))
        },
    ))
}

// get the range of node identifier and build the fqn part
fn node_to_fqn_part(node: &Node<StrDoc<SupportLang>>, kind: &str) -> Option<CSharpFqnPart> {
    match kind {
        "namespace_declaration" => process_simple_node(node, CSharpFqnPartType::Namespace),
        "class_declaration" => process_simple_node(node, CSharpFqnPartType::Class),
        "interface_declaration" => process_simple_node(node, CSharpFqnPartType::Interface),
        "enum_declaration" => process_simple_node(node, CSharpFqnPartType::Enum),
        "struct_declaration" => process_simple_node(node, CSharpFqnPartType::Struct),
        "record_declaration" => process_simple_node(node, CSharpFqnPartType::Record),
        "constructor_declaration" => process_simple_node(node, CSharpFqnPartType::Constructor),
        "destructor_declaration" => process_simple_node(node, CSharpFqnPartType::Finalizer),
        "delegate_declaration" => process_simple_node(node, CSharpFqnPartType::Delegate),
        "property_declaration" => process_simple_node(node, CSharpFqnPartType::Property),
        "method_declaration" => process_method_node(node),
        "variable_declarator" => process_variable_declarator_node(node),
        "operator_declaration" => process_operator_declaration_node(node),
        "indexer_declaration" => process_indexer_declaration_node(node),
        "event_field_declaration" => process_class_member_node(node, CSharpFqnPartType::Event),
        "field_declaration" => process_class_member_node(node, CSharpFqnPartType::Field),
        _ => None,
    }
}

fn process_simple_node(
    node: &Node<StrDoc<SupportLang>>,
    node_type: CSharpFqnPartType,
) -> Option<CSharpFqnPart> {
    let identifier_node = node.field("name")?;
    let name = identifier_node.text().into_owned();
    Some(CSharpFqnPart::new(node_type, name, node_to_range(node)))
}

fn process_operator_declaration_node(node: &Node<StrDoc<SupportLang>>) -> Option<CSharpFqnPart> {
    let mut children = node.children();
    // The "operator" keyword has to exist.
    children.find(|c| c.kind() == "operator")?;

    // The operation name always follows the "operator" keyword.
    let identifier_node = children.next()?;

    let name = format!("operator{}", identifier_node.text());

    Some(CSharpFqnPart::new(
        CSharpFqnPartType::Operator,
        name,
        node_to_range(node),
    ))
}

fn process_indexer_declaration_node(node: &Node<StrDoc<SupportLang>>) -> Option<CSharpFqnPart> {
    Some(CSharpFqnPart::new(
        CSharpFqnPartType::Indexer,
        "indexer".to_string(),
        node_to_range(node),
    ))
}

fn process_class_member_node(
    node: &Node<StrDoc<SupportLang>>,
    node_type: CSharpFqnPartType,
) -> Option<CSharpFqnPart> {
    let variable_declaration_node = node
        .children()
        .find(|c| c.kind() == "variable_declaration")?;
    let variable_declarator_node = variable_declaration_node
        .children()
        .find(|c| c.kind() == "variable_declarator")?;
    let identifier_node = variable_declarator_node.field("name")?;
    let name = identifier_node.text().into_owned();

    Some(CSharpFqnPart::new(node_type, name, node_to_range(node)))
}

fn process_variable_declarator_node(node: &Node<StrDoc<SupportLang>>) -> Option<CSharpFqnPart> {
    let part_type = if node
        .children()
        .any(|child| child.kind() == "lambda_expression")
    {
        Some(CSharpFqnPartType::Lambda)
    } else if node
        .children()
        .any(|child| child.kind() == "anonymous_object_creation_expression")
    {
        Some(CSharpFqnPartType::AnonymousType)
    } else {
        None
    };

    part_type.and_then(|part_type| {
        let identifier_node = node.field("name")?;
        let name = identifier_node.text().into_owned();

        Some(CSharpFqnPart::new(part_type, name, node_to_range(node)))
    })
}

fn process_method_node(node: &Node<StrDoc<SupportLang>>) -> Option<CSharpFqnPart> {
    // Check if method has static modifier
    let has_static_modifier = node
        .children()
        .any(|child| child.kind() == "modifier" && child.text().as_ref() == "static");

    let identifier_node = node.field("name")?;

    let name = identifier_node.text().into_owned();

    let node_type = if has_static_modifier {
        let is_extension_method = node
            .field("parameters")
            .and_then(|param_list| param_list.children().find(|c| c.kind() == "parameter"))
            .is_some_and(|first_param| {
                first_param
                    .children()
                    .any(|child| child.kind() == "modifier" && child.text().as_ref() == "this")
            });

        if is_extension_method {
            CSharpFqnPartType::ExtensionMethod
        } else {
            CSharpFqnPartType::StaticMethod
        }
    } else {
        CSharpFqnPartType::InstanceMethod
    };

    Some(CSharpFqnPart::new(node_type, name, node_to_range(node)))
}

fn process_node(node: &Node<StrDoc<SupportLang>>) -> Option<CSharpFqnPart> {
    let node_kind = node.kind();
    let kind_str = node_kind.as_ref();
    node_to_fqn_part(node, kind_str)
}

pub fn parse_ast(ast: &Root<StrDoc<SupportLang>>) -> (CSharpDefinitions, CSharpImports) {
    // a collection of fqn definitions
    let mut fqn_definitions = Vec::with_capacity(128);

    // a collection of imported symbols
    let mut imports = Vec::with_capacity(32);

    // a stack of fqn parts for the current scope
    let mut current_scope: ScopeStack = smallvec![];

    // a stack of nodes to process and their known fqn parts
    let mut stack: Vec<Option<Node<StrDoc<SupportLang>>>> = Vec::with_capacity(128);

    // handle file scoped namespace declaration
    if let Some(file_namespace_node) = ast
        .root()
        .children()
        .find(|c| c.kind() == "file_scoped_namespace_declaration")
        && let Some(namespace_name) = file_namespace_node.field("name")
    {
        current_scope.push(CSharpFqnPart::new(
            CSharpFqnPartType::Namespace,
            namespace_name.text().to_string(),
            node_to_range(&namespace_name),
        ));
    }

    stack.push(Some(ast.root()));

    while let Some(node_option) = stack.pop() {
        if let Some(node) = node_option {
            let node_kind = node.kind();

            // Check if this is a using directive
            if node_kind == "using_directive" {
                if let Some(import) = process_using_directive(&node, &current_scope) {
                    imports.push(import);
                }
                // using directives don't create scope, so just add children
                push_children_reverse(&node, &mut stack);
            } else if let Some(fqn_part) = process_node(&node) {
                current_scope.push(fqn_part);

                // add a None to the stack to indicate the end of the scope
                stack.push(None);

                // add all children to the stack in reverse order
                push_children_reverse(&node, &mut stack);

                // Create definition if this is a definition type
                if let Some(last_fqn_part) = current_scope.last()
                    && let Some(definition_type) =
                        CSharpDefinitionType::from_fqn_part_type(&last_fqn_part.node_type)
                {
                    fqn_definitions.push(CSharpDefinitionInfo::new(
                        definition_type,
                        last_fqn_part.node_name().to_string(),
                        Arc::new(current_scope.clone()),
                        last_fqn_part.range(),
                    ));
                }
            } else {
                // this node kind does not create a scope just add children to the stack in reverse order
                push_children_reverse(&node, &mut stack);
            }
        } else {
            // None indicates the end of a scope
            current_scope.pop();
        }
    }
    (fqn_definitions, imports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LanguageParser, SupportedLanguage,
        parser::GenericParser,
        utils::{Position, Range},
    };

    fn assert_fqn_range(
        code: &str,
        name: &str,
        expected_type: CSharpFqnPartType,
        expected_range: Range,
    ) {
        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (definitions, _imports) = parse_ast(&ast);

        println!("--- Testing for {name} ---");
        for def in &definitions {
            println!(
                "Found definition: {:?}, range: {:?}",
                def.fqn.iter().map(|p| &p.node_name).collect::<Vec<_>>(),
                def.range,
            );
        }

        let found_definition = definitions.iter().find(|def| {
            if let Some(part) = def.fqn.last() {
                part.node_name == name && part.node_type == expected_type
            } else {
                false
            }
        });

        match found_definition {
            Some(def) => {
                assert_eq!(
                    def.range, expected_range,
                    "Range mismatch for definition of '{name}': {def:?}"
                );
            }
            None => {
                panic!("Definition with name '{name}' and type {expected_type:?} not found.")
            }
        }
    }

    #[test]
    fn test_namespace_fqn() {
        // Note: Namespaces are not included in definitions since they don't have a CSharpDefinitionType
        // This test verifies that we can still parse and handle namespaces correctly in the FQN structure
        let code = "namespace My.Test.App { class TestClass { } }";
        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (definitions, _imports) = parse_ast(&ast);

        // Find the class definition and verify its FQN includes the namespace
        let class_def = definitions.iter().find(|d| d.name == "TestClass");
        assert!(class_def.is_some(), "Should find TestClass definition");

        let class_def = class_def.unwrap();
        let fqn_parts: Vec<_> = class_def.fqn.iter().map(|p| p.node_name.as_str()).collect();
        assert_eq!(fqn_parts, vec!["My.Test.App", "TestClass"]);

        // Verify the first part is indeed a namespace
        assert_eq!(
            class_def.fqn.first().unwrap().node_type,
            CSharpFqnPartType::Namespace
        );
    }

    #[test]
    fn test_class_fqn() {
        let code = "class MyClass { }";
        assert_fqn_range(
            code,
            "MyClass",
            CSharpFqnPartType::Class,
            Range::new(Position::new(0, 0), Position::new(0, 17), (0, 17)),
        );
    }

    #[test]
    fn test_interface_fqn() {
        let code = "interface IMyInterface { }";
        assert_fqn_range(
            code,
            "IMyInterface",
            CSharpFqnPartType::Interface,
            Range::new(Position::new(0, 0), Position::new(0, 26), (0, 26)),
        );
    }

    #[test]
    fn test_enum_fqn() {
        let code = "enum MyEnum { One, Two }";
        assert_fqn_range(
            code,
            "MyEnum",
            CSharpFqnPartType::Enum,
            Range::new(Position::new(0, 0), Position::new(0, 24), (0, 24)),
        );
    }

    #[test]
    fn test_struct_fqn() {
        let code = "struct MyStruct { }";
        assert_fqn_range(
            code,
            "MyStruct",
            CSharpFqnPartType::Struct,
            Range::new(Position::new(0, 0), Position::new(0, 19), (0, 19)),
        );
    }

    #[test]
    fn test_record_fqn() {
        let code = "public record Person(string Name);";
        assert_fqn_range(
            code,
            "Person",
            CSharpFqnPartType::Record,
            Range::new(Position::new(0, 0), Position::new(0, 34), (0, 34)),
        );
    }

    #[test]
    fn test_constructor_fqn() {
        let code = "class MyClass { public MyClass() {} }";
        assert_fqn_range(
            code,
            "MyClass",
            CSharpFqnPartType::Constructor,
            Range::new(Position::new(0, 16), Position::new(0, 35), (16, 35)),
        );
    }

    #[test]
    fn test_finalizer_fqn() {
        let code = "class MyClass { ~MyClass() {} }";
        assert_fqn_range(
            code,
            "MyClass",
            CSharpFqnPartType::Finalizer,
            Range::new(Position::new(0, 16), Position::new(0, 29), (16, 29)),
        );
    }

    #[test]
    fn test_delegate_fqn() {
        let code = "public delegate void MyDelegate(int arg);";
        assert_fqn_range(
            code,
            "MyDelegate",
            CSharpFqnPartType::Delegate,
            Range::new(Position::new(0, 0), Position::new(0, 41), (0, 41)),
        );
    }

    #[test]
    fn test_property_fqn() {
        let code = "class C { public int MyProperty { get; set; } }";
        assert_fqn_range(
            code,
            "MyProperty",
            CSharpFqnPartType::Property,
            Range::new(Position::new(0, 10), Position::new(0, 45), (10, 45)),
        );
    }

    #[test]
    fn test_instancemethod_fqn() {
        let code = "class C { void MyMethod() {} }";
        assert_fqn_range(
            code,
            "MyMethod",
            CSharpFqnPartType::InstanceMethod,
            Range::new(Position::new(0, 10), Position::new(0, 28), (10, 28)),
        );
    }

    #[test]
    fn test_staticmethod_fqn() {
        let code = "class C { static void MyStaticMethod() {} }";
        assert_fqn_range(
            code,
            "MyStaticMethod",
            CSharpFqnPartType::StaticMethod,
            Range::new(Position::new(0, 10), Position::new(0, 41), (10, 41)),
        );
    }

    #[test]
    fn test_lambda_fqn() {
        let code = "class C { Action a = () => {}; }";
        assert_fqn_range(
            code,
            "a",
            CSharpFqnPartType::Lambda,
            Range::new(Position::new(0, 17), Position::new(0, 29), (17, 29)),
        );
    }

    #[test]
    fn test_operator_fqn() {
        let code = "class C { public static C operator+(C a, C b) => new C(); }";
        assert_fqn_range(
            code,
            "operator+",
            CSharpFqnPartType::Operator,
            Range::new(Position::new(0, 10), Position::new(0, 57), (10, 57)),
        );
    }

    #[test]
    fn test_indexer_fqn() {
        let code = "class C { public int this[int index] { get; set; } }";
        assert_fqn_range(
            code,
            "indexer",
            CSharpFqnPartType::Indexer,
            Range::new(Position::new(0, 10), Position::new(0, 50), (10, 50)),
        );
    }

    #[test]
    fn test_extensionmethod_fqn() {
        let code = "static class C { public static void MyExt(this int i) {} }";
        assert_fqn_range(
            code,
            "MyExt",
            CSharpFqnPartType::ExtensionMethod,
            Range::new(Position::new(0, 17), Position::new(0, 56), (17, 56)),
        );
    }

    #[test]
    fn test_event_fqn() {
        let code = "class C { public event System.Action MyEvent; }";
        assert_fqn_range(
            code,
            "MyEvent",
            CSharpFqnPartType::Event,
            Range::new(Position::new(0, 10), Position::new(0, 45), (10, 45)),
        );
    }

    #[test]
    fn test_field_fqn() {
        let code = "class C { public int myField; }";
        assert_fqn_range(
            code,
            "myField",
            CSharpFqnPartType::Field,
            Range::new(Position::new(0, 10), Position::new(0, 29), (10, 29)),
        );
    }

    #[test]
    fn test_anonymous_type_fqn() {
        let code = "class C { void M() { var an = new { a = 1 }; } }";
        assert_fqn_range(
            code,
            "an",
            CSharpFqnPartType::AnonymousType,
            Range::new(Position::new(0, 25), Position::new(0, 43), (25, 43)),
        );
    }

    #[test]
    fn test_nested_fqn() {
        let code = r#"
namespace N {
    class C {
        void M() { }
    }
}
        "#;
        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (definitions, _imports) = parse_ast(&ast);

        let method_entry = definitions
            .iter()
            .find(|def| def.name == "M")
            .expect("Could not find method M");

        let fqn_parts: Vec<_> = method_entry
            .fqn
            .iter()
            .map(|p| p.node_name.as_str())
            .collect();
        assert_eq!(fqn_parts, vec!["N", "C", "M"]);
    }

    #[test]
    fn test_file_scoped_namespace_is_root_fqn() {
        let code = r#"
namespace MyFileScopedNamespace;

class MyClass {
    void MyMethod() {}
}
        "#;
        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (definitions, _imports) = parse_ast(&ast);

        assert!(
            !definitions.is_empty(),
            "Definitions should not be empty for the given code"
        );

        for def in definitions.iter() {
            assert!(!def.fqn.is_empty(), "FQN should not be empty");
            let first_part = def.fqn.first().unwrap();
            assert_eq!(first_part.node_name, "MyFileScopedNamespace");
            assert_eq!(first_part.node_type, CSharpFqnPartType::Namespace);
        }

        let method_entry = definitions
            .iter()
            .find(|def| def.name == "MyMethod")
            .expect("Could not find method MyMethod");

        let fqn_parts: Vec<_> = method_entry
            .fqn
            .iter()
            .map(|p| p.node_name.as_str())
            .collect();
        assert_eq!(
            fqn_parts,
            vec!["MyFileScopedNamespace", "MyClass", "MyMethod"]
        );
    }

    #[test]
    fn test_using_directives_parsing() {
        let code = r#"
using System;
using Console = System.Console;
"#;
        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (_definitions, imports) = parse_ast(&ast);

        assert_eq!(imports.len(), 2, "Should find 2 using directives");

        // Test default import
        let default_import = imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Default && i.import_path == "System");
        assert!(default_import.is_some(), "Should find default using System");

        // Test alias import
        let alias_import = imports.iter().find(|i| {
            i.import_type == CSharpImportType::Alias && i.import_path == "System.Console"
        });
        assert!(
            alias_import.is_some(),
            "Should find alias using Console = System.Console"
        );
        if let Some(import) = alias_import {
            assert_eq!(
                import.identifier.as_ref().unwrap().alias.as_ref().unwrap(),
                "Console"
            );
        }
    }

    #[test]
    fn test_comprehensive_using_directives() {
        let fixture_path = "src/csharp/fixtures/ComprehensiveCSharp.cs";
        let csharp_code = std::fs::read_to_string(fixture_path)
            .expect("Should be able to read ComprehensiveCSharp.cs fixture");

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(&csharp_code, Some(fixture_path)).unwrap().ast;
        let (_definitions, imports) = parse_ast(&ast);

        assert!(
            !imports.is_empty(),
            "Should find imports in comprehensive fixture"
        );

        // Test that we find the basic using System
        let system_import = imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Default && i.import_path == "System");
        assert!(system_import.is_some(), "Should find 'using System;'");

        // Test that we find the static import
        let static_import = imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Static);
        assert!(
            static_import.is_some(),
            "Should find at least one static import"
        );

        // Test that we find the alias import
        let alias_import = imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Alias);
        assert!(
            alias_import.is_some(),
            "Should find at least one alias import"
        );

        // Test that we find the global import
        let global_import = imports
            .iter()
            .find(|i| i.import_type == CSharpImportType::Global);
        assert!(
            global_import.is_some(),
            "Should find at least one global import"
        );
    }

    #[test]
    fn test_parse_ast_returns_definitions_directly() {
        let code = r#"
namespace TestNamespace {
    public class TestClass {
        public void TestMethod() { }
        public int TestProperty { get; set; }
    }
    
    public interface ITestInterface {
        void InterfaceMethod();
    }
}
        "#;

        let parser = GenericParser::default_for_language(SupportedLanguage::CSharp);
        let ast = parser.parse(code, None).unwrap().ast;
        let (definitions, _imports) = parse_ast(&ast);

        // Verify we get definitions directly
        assert!(!definitions.is_empty(), "Should have definitions");

        // Check that we have the expected definitions
        let class_def = definitions.iter().find(|d| d.name == "TestClass");
        assert!(class_def.is_some(), "Should find TestClass definition");
        let class_def = class_def.unwrap();
        assert_eq!(class_def.definition_type, CSharpDefinitionType::Class);

        let method_def = definitions.iter().find(|d| d.name == "TestMethod");
        assert!(method_def.is_some(), "Should find TestMethod definition");
        let method_def = method_def.unwrap();
        assert_eq!(
            method_def.definition_type,
            CSharpDefinitionType::InstanceMethod
        );

        let property_def = definitions.iter().find(|d| d.name == "TestProperty");
        assert!(
            property_def.is_some(),
            "Should find TestProperty definition"
        );
        let property_def = property_def.unwrap();
        assert_eq!(property_def.definition_type, CSharpDefinitionType::Property);

        let interface_def = definitions.iter().find(|d| d.name == "ITestInterface");
        assert!(
            interface_def.is_some(),
            "Should find ITestInterface definition"
        );
        let interface_def = interface_def.unwrap();
        assert_eq!(
            interface_def.definition_type,
            CSharpDefinitionType::Interface
        );

        let interface_method_def = definitions.iter().find(|d| d.name == "InterfaceMethod");
        assert!(
            interface_method_def.is_some(),
            "Should find InterfaceMethod definition"
        );
        let interface_method_def = interface_method_def.unwrap();
        assert_eq!(
            interface_method_def.definition_type,
            CSharpDefinitionType::InstanceMethod
        );

        // Verify FQNs are correct
        let method_fqn: Vec<_> = method_def
            .fqn
            .iter()
            .map(|p| p.node_name.as_str())
            .collect();
        assert_eq!(method_fqn, vec!["TestNamespace", "TestClass", "TestMethod"]);
    }
}
