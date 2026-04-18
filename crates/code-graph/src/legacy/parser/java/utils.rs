use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

pub(in crate::legacy::parser::java) mod node_types {
    pub const CLASS: &str = "class_declaration";
    pub const INTERFACE: &str = "interface_declaration";
    pub const ENUM: &str = "enum_declaration";
    pub const ENUM_CONSTANT: &str = "enum_constant";
    pub const RECORD: &str = "record_declaration";
    pub const ANNOTATION: &str = "annotation";
    pub const MARKER_ANNOTATION: &str = "marker_annotation";
    pub const ANNOTATION_DECLARATION: &str = "annotation_type_declaration";
    pub const ANNOTATION_ELEMENT_DECLARATION: &str = "annotation_type_element_declaration";
    pub const METHOD: &str = "method_declaration";
    pub const PACKAGE: &str = "package_declaration";
    pub const SCOPED_IDENTIFIER: &str = "scoped_identifier";
    pub const FIELD_DECLARATION: &str = "field_declaration";
    pub const LOCAL_VARIABLE_DECLARATION: &str = "local_variable_declaration";
    pub const VARIABLE_DECLARATOR: &str = "variable_declarator";
    pub const IMPORT_DECLARATION: &str = "import_declaration";
    pub const IDENTIFIER: &str = "identifier";
    pub const CONSTRUCTOR_DECLARATION: &str = "constructor_declaration";
    pub const FORMAL_PARAMETERS: &str = "formal_parameters";
    pub const FORMAL_PARAMETER: &str = "formal_parameter";
    pub const EXTENDS_INTERFACES: &str = "extends_interfaces";
    pub const TYPE_LIST: &str = "type_list";
    pub const ARRAY_TYPE: &str = "array_type";
    pub const GENERIC_TYPE: &str = "generic_type";
    pub const PATTERN: &str = "pattern";
    pub const TYPE_PATTERN: &str = "type_pattern";
    pub const FOR_STATEMENT: &str = "for_statement"; // Covers the binding in the for loop.
    pub const ENHANCED_FOR_STATEMENT: &str = "enhanced_for_statement"; // Covers the binding in the enhanced for loop.
    pub const TRY_WITH_RESOURCES_STATEMENT: &str = "try_with_resources_statement"; // Covers the binding in the try with resources statement.
    pub const BLOCK: &str = "BLOCK"; // Covers, normal try, finally, if, if else, else, switch expressions, do while, while.
    pub const CATCH_CLAUSE: &str = "catch_clause"; // Covers the exception binding in the catch clause.
    pub const RESOURCE: &str = "resource"; // Covers the resource binding in the try with resources statement.
    pub const IF_STATEMENT: &str = "if_statement"; // Covers the binding in the if statement. (ex: instanceof)
    pub const SWITCH_RULE: &str = "switch_rule"; // Covers the binding in the switch rule.
    // EXPRESSIONS
    pub const OBJECT_CREATION_EXPRESSION: &str = "object_creation_expression";
    pub const ARRAY_CREATION_EXPRESSION: &str = "array_creation_expression";
    pub const PARENTHESIZED_EXPRESSION: &str = "parenthesized_expression";
    pub const BINARY_EXPRESSION: &str = "binary_expression";
    pub const UNARY_EXPRESSION: &str = "unary_expression";
    pub const UPDATE_EXPRESSION: &str = "update_expression";
    pub const INSTANCE_OF_EXPRESSION: &str = "instanceof_expression";
    pub const METHOD_INVOCATION: &str = "method_invocation";
    pub const FIELD_ACCESS: &str = "field_access";
    pub const ARRAY_ACCESS: &str = "array_access";
    pub const THIS: &str = "this";
    pub const SUPER: &str = "super";
    pub const RETURN_STATEMENT: &str = "return_statement";
    pub const TERNARY_EXPRESSION: &str = "ternary_expression";
    pub const METHOD_REFERENCE: &str = "method_reference";
    pub const EXPRESSION_STATEMENT: &str = "expression_statement";
    pub const LAMBDA_EXPRESSION: &str = "lambda_expression";
    pub const ARGUMENT_LIST: &str = "argument_list";
    pub const ELEMENT_VALUE_PAIR: &str = "element_value_pair";
    pub const ANNOTATION_ARGUMENT_LIST: &str = "annotation_argument_list";
    pub const THROW_STATEMENT: &str = "throw_statement";
    pub const ASSIGNMENT_EXPRESSION: &str = "assignment_expression";
}

// Helper function to get a child node by kind
pub(in crate::legacy::parser::java) fn get_child_by_kind<'a>(
    node: &Node<'a, StrDoc<SupportLang>>,
    kind_name: &str,
) -> Option<Node<'a, StrDoc<SupportLang>>> {
    node.children()
        .find(|child| child.kind().as_ref() == kind_name)
}
