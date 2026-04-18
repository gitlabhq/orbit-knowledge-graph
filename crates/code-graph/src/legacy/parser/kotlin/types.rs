use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::legacy::parser::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::legacy::parser::fqn::FQNPart;
use crate::legacy::parser::imports::{ImportTypeInfo, ImportedSymbolInfo};
use crate::legacy::parser::references::{ReferenceInfo, TargetResolution};
use crate::utils::Range;

// ast types

pub(in crate::legacy::parser::kotlin) type AstNode<'a> = Node<'a, StrDoc<SupportLang>>;
pub(in crate::legacy::parser::kotlin) type AstRootNode = Root<StrDoc<SupportLang>>;

pub(in crate::legacy::parser::kotlin) mod node_types {
    pub const CLASS: &str = "class_declaration";
    pub const OBJECT: &str = "object_declaration";
    pub const FUNCTION: &str = "function_declaration";
    pub const PROPERTY: &str = "property_declaration";
    pub const COMPANION_OBJECT: &str = "companion_object";
    pub const PACKAGE: &str = "package_header";
    pub const ENUM_ENTRY: &str = "enum_entry";
    pub const CLASS_PARAMETER: &str = "class_parameter";
    pub const PRIMARY_CONSTRUCTOR: &str = "primary_constructor";
    pub const SECONDARY_CONSTRUCTOR: &str = "secondary_constructor";
    pub const CONSTRUCTOR_INVOCATION: &str = "constructor_invocation";
    pub const VARIABLE_DECLARATION: &str = "variable_declaration";
    pub const SIMPLE_IDENTIFIER: &str = "simple_identifier";
    pub const TYPE_IDENTIFIER: &str = "type_identifier";
    pub const TYPE_ARGUMENTS: &str = "type_arguments";
    pub const TYPE_PROJECTION: &str = "type_projection";
    pub const FUNCTION_TYPE: &str = "function_type";
    pub const IDENTIFIER: &str = "identifier";
    pub const LAMBDA_LITERAL: &str = "lambda_literal";
    pub const CALLABLE_REFERENCE: &str = "callable_reference";
    pub const ANONYMOUS_FUNCTION: &str = "anonymous_function";
    pub const MODIFIERS: &str = "modifiers";
    pub const CLASS_MODIFIER: &str = "class_modifier";
    pub const ENUM_CLASS_BODY: &str = "enum_class_body";
    pub const IMPORT_HEADER: &str = "import_header";
    pub const IMPORT_ALIAS: &str = "import_alias";
    pub const GETTER: &str = "getter";
    pub const SETTER: &str = "setter";
    pub const DELEGATION_SPECIFIER: &str = "delegation_specifier";
    pub const FUNCTION_VALUE_PARAMETERS: &str = "function_value_parameters";
    pub const PARAMETER: &str = "parameter";
    pub const CALL_EXPRESSION: &str = "call_expression";
    pub const CALL_SUFFIX: &str = "call_suffix";
    pub const NAVIGATION_EXPRESSION: &str = "navigation_expression";
    pub const PARENTHESIZED_EXPRESSION: &str = "parenthesized_expression";
    pub const INDEXING_EXPRESSION: &str = "indexing_expression";
    pub const INFIX_EXPRESSION: &str = "infix_expression";
    pub const NAVIGATION_SUFFIX: &str = "navigation_suffix";
    pub const FUNCTION_BODY: &str = "function_body";
    pub const STATEMENTS: &str = "statements";
    pub const USER_TYPE: &str = "user_type";
    pub const NULLABLE_TYPE: &str = "nullable_type";
    pub const WHEN_EXPRESSION: &str = "when_expression";
    pub const WHEN_SUBJECT: &str = "when_subject";
    pub const WHEN_ENTRY: &str = "when_entry";
    pub const FOR_STATEMENT: &str = "for_statement";
    pub const WHILE_STATEMENT: &str = "while_statement";
    pub const DO_WHILE_STATEMENT: &str = "do_while_statement";
    pub const IF_EXPRESSION: &str = "if_expression";
    pub const TRY_EXPRESSION: &str = "try_expression";
    pub const CATCH_BLOCK: &str = "catch_block";
    pub const CONTROL_STRUCTURE_BODY: &str = "control_structure_body";
    pub const INTERPOLATED_IDENTIFIER: &str = "interpolated_identifier";
    pub const INTERPOLATED_EXPRESSION: &str = "interpolated_expression";
    pub const DISJUNCTION_EXPRESSION: &str = "disjunction_expression";
    pub const CONJUNCTION_EXPRESSION: &str = "conjunction_expression";
    pub const ADDITIVE_EXPRESSION: &str = "additive_expression";
    pub const MULTIPLICATIVE_EXPRESSION: &str = "multiplicative_expression";
    pub const ASSIGNMENT: &str = "assignment";
    pub const EQUALITY_EXPRESSION: &str = "equality_expression";
    pub const COMPARISON_EXPRESSION: &str = "comparison_expression";
    pub const RANGE_EXPRESSION: &str = "range_expression";
    pub const ELVIS_EXPRESSION: &str = "elvis_expression";
    pub const AS_EXPRESSION: &str = "as_expression";
    pub const PREFIX_EXPRESSION: &str = "prefix_expression";
    pub const POSTFIX_EXPRESSION: &str = "postfix_expression";
    pub const INDEXING_SUFFIX: &str = "indexing_suffix";
    pub const VALUE_ARGUMENT: &str = "value_argument";
    pub const CHECK_EXPRESSION: &str = "check_expression";
    pub const SOURCE_FILE: &str = "source_file";
    pub const STRING_LITERAL: &str = "string_literal";
    pub const JUMP_EXPRESSION: &str = "jump_expression";
    pub const WHEN_CONDITION: &str = "when_condition";
    pub const THIS_EXPRESSION: &str = "this_expression";
    pub const SUPER_EXPRESSION: &str = "super_expression";
    pub const ANNOTATION: &str = "annotation";
}

// FQN types

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum KotlinFqnPartType {
    Package,
    Class,
    DataClass,
    ValueClass,
    Interface,
    Enum,
    EnumEntry,
    AnnotationClass,
    Function,
    CompanionObject,
    Constructor,
    Object,
    Lambda,
    Property,
    Parameter,
    LocalVariable,
}

/// Kotlin-specific FQN part with metadata
pub type KotlinFqnPart = FQNPart<KotlinFqnPartType>;

/// Kotlin-specific FQN with rich metadata
pub type KotlinFqn = Arc<SmallVec<[KotlinFqnPart; 8]>>;

/// Maps node ranges to their corresponding AST nodes and FQN parts
pub type KotlinNodeFqnMap<'a> = FxHashMap<Range, (Node<'a, StrDoc<SupportLang>>, KotlinFqn)>;

// Definition types

/// Represents a Kotlin definition found in the code
/// This is now a type alias using the generic DefinitionInfo with Kotlin-specific types

#[derive(Debug, Clone)]
pub enum KotlinDefinitionMetadata {
    Class {
        super_class: Option<String>,
        super_interfaces: Vec<String>,
    },
    Function {
        receiver: Option<String>,
        return_type: Option<String>,
        init: Option<KotlinExpressionInfo>,
    },
    Parameter {
        parameter_type: String,
        range: Range,
    },
    Field {
        receiver: Option<String>,
        field_type: Option<String>,
        init: Option<KotlinExpressionInfo>,
        range: Range,
    },
}

pub type KotlinDefinitionInfo =
    DefinitionInfo<KotlinDefinitionType, KotlinFqn, KotlinDefinitionMetadata>;
pub type KotlinDefinitions = Vec<KotlinDefinitionInfo>;

/// Types of definitions that can be found in Kotlin code
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum KotlinDefinitionType {
    Package,
    Class,
    DataClass,
    ValueClass,
    Interface,
    Enum,
    EnumEntry,
    AnnotationClass,
    Function,
    Property,
    CompanionObject,
    Constructor,
    Object,
    Lambda,
    Parameter,
    LocalVariable,
}

impl KotlinDefinitionType {
    pub fn from_fqn_part_type(fqn_part_type: &KotlinFqnPartType) -> Option<Self> {
        match fqn_part_type {
            KotlinFqnPartType::Class => Some(Self::Class),
            KotlinFqnPartType::DataClass => Some(Self::DataClass),
            KotlinFqnPartType::ValueClass => Some(Self::ValueClass),
            KotlinFqnPartType::Interface => Some(Self::Interface),
            KotlinFqnPartType::Enum => Some(Self::Enum),
            KotlinFqnPartType::Object => Some(Self::Object),
            KotlinFqnPartType::Function => Some(Self::Function),
            KotlinFqnPartType::Constructor => Some(Self::Constructor),
            KotlinFqnPartType::Property => Some(Self::Property),
            KotlinFqnPartType::Lambda => Some(Self::Lambda),
            KotlinFqnPartType::EnumEntry => Some(Self::EnumEntry),
            KotlinFqnPartType::CompanionObject => Some(Self::CompanionObject),
            KotlinFqnPartType::AnnotationClass => Some(Self::AnnotationClass),
            KotlinFqnPartType::Parameter => Some(Self::Parameter),
            KotlinFqnPartType::LocalVariable => Some(Self::LocalVariable),
            KotlinFqnPartType::Package => Some(Self::Package),
        }
    }
}

impl DefinitionTypeInfo for KotlinDefinitionType {
    /// Convert KotlinDefinitionType to its string representation
    fn as_str(&self) -> &str {
        match self {
            KotlinDefinitionType::Class => "Class",
            KotlinDefinitionType::DataClass => "DataClass",
            KotlinDefinitionType::ValueClass => "ValueClass",
            KotlinDefinitionType::Interface => "Interface",
            KotlinDefinitionType::Enum => "Enum",
            KotlinDefinitionType::EnumEntry => "EnumEntry",
            KotlinDefinitionType::AnnotationClass => "AnnotationClass",
            KotlinDefinitionType::Function => "Function",
            KotlinDefinitionType::Property => "Property",
            KotlinDefinitionType::CompanionObject => "CompanionObject",
            KotlinDefinitionType::Constructor => "Constructor",
            KotlinDefinitionType::Object => "Object",
            KotlinDefinitionType::Lambda => "Lambda",
            KotlinDefinitionType::Parameter => "Parameter",
            KotlinDefinitionType::LocalVariable => "LocalVariable",
            KotlinDefinitionType::Package => "Package",
        }
    }
}

// Import types

pub type KotlinImportedSymbolInfo = ImportedSymbolInfo<KotlinImportType, KotlinFqn>;
pub type KotlinImports = Vec<KotlinImportedSymbolInfo>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum KotlinImportType {
    Import,
    WildcardImport,
    AliasedImport,
}

impl ImportTypeInfo for KotlinImportType {
    fn as_str(&self) -> &str {
        match self {
            KotlinImportType::Import => "Import",
            KotlinImportType::WildcardImport => "WildcardImport",
            KotlinImportType::AliasedImport => "AliasedImport",
        }
    }
}

// Reference types

pub type KotlinExpressions = Vec<KotlinExpression>;

#[derive(Debug, Clone)]
pub struct KotlinExpressionInfo {
    pub range: Range,
    pub generics: Vec<String>,
    pub expression: KotlinExpression,
}

#[derive(Debug, Clone)]
pub enum KotlinExpression {
    // Simple expressions
    Identifier {
        name: String,
    },
    Call {
        name: String,
    },
    Index {
        target: Box<KotlinExpressionInfo>,
    },
    FieldAccess {
        target: Box<KotlinExpressionInfo>,
        member: String,
    },
    MemberFunctionCall {
        target: Box<KotlinExpressionInfo>,
        member: String,
    },
    MethodReference {
        target: Option<Box<KotlinExpressionInfo>>,
        member: String,
    },
    Annotation {
        name: String,
    },
    This {
        label: Option<String>,
    },
    Super,
    // Complex expressions
    Elvis {
        left: Box<KotlinExpressionInfo>,
        right: Option<Box<KotlinExpressionInfo>>,
    },
    When {
        entries: Vec<KotlinExpressionInfo>,
    },
    If {
        bodies: Vec<KotlinExpressionInfo>,
    },
    Try {
        body: Option<Box<KotlinExpressionInfo>>,
        catch_clauses: Vec<KotlinExpressionInfo>,
    },
    Lambda {
        expression: Box<KotlinExpressionInfo>,
    },
    // Operator expressions
    Parenthesized {
        expression: Box<KotlinExpressionInfo>,
    },
    Unary {
        operator: String,
        target: Box<KotlinExpressionInfo>,
    },
    Binary {
        left: Box<KotlinExpressionInfo>,
        operator: String,
        right: Box<KotlinExpressionInfo>,
    },
    // Number, String, Boolean, Unit, etc.
    Literal,
}

impl KotlinExpression {
    pub fn variant_name(&self) -> &'static str {
        match self {
            KotlinExpression::Identifier { .. } => "Identifier",
            KotlinExpression::Call { .. } => "Call",
            KotlinExpression::Index { .. } => "Index",
            KotlinExpression::FieldAccess { .. } => "FieldAccess",
            KotlinExpression::MemberFunctionCall { .. } => "MemberFunctionCall",
            KotlinExpression::MethodReference { .. } => "MethodReference",
            KotlinExpression::Annotation { .. } => "Annotation",
            KotlinExpression::This { .. } => "This",
            KotlinExpression::Super => "Super",
            KotlinExpression::Elvis { .. } => "Elvis",
            KotlinExpression::When { .. } => "When",
            KotlinExpression::If { .. } => "If",
            KotlinExpression::Try { .. } => "Try",
            KotlinExpression::Lambda { .. } => "Lambda",
            KotlinExpression::Parenthesized { .. } => "Parenthesized",
            KotlinExpression::Unary { .. } => "Unary",
            KotlinExpression::Binary { .. } => "Binary",
            KotlinExpression::Literal => "Literal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum KotlinReferenceType {
    Call,
    FieldAccess,
}

pub type KotlinTargetResolution =
    TargetResolution<KotlinDefinitionInfo, KotlinImportedSymbolInfo, KotlinExpressionInfo>;
pub type KotlinReferences = Vec<KotlinReferenceInfo>;
pub type KotlinReferenceInfo =
    ReferenceInfo<KotlinTargetResolution, KotlinReferenceType, KotlinExpressionInfo, KotlinFqn>;
