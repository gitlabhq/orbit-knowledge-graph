use std::sync::Arc;

use smallvec::SmallVec;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, Root, SupportLang};

use crate::legacy::parser::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::legacy::parser::fqn::FQNPart;
use crate::legacy::parser::imports::{ImportTypeInfo, ImportedSymbolInfo};
use crate::legacy::parser::references::{ReferenceInfo, TargetResolution};

// ast types

pub(in crate::legacy::parser::java) type AstNode<'a> = Node<'a, StrDoc<SupportLang>>;
pub(in crate::legacy::parser::java) type AstRootNode = Root<StrDoc<SupportLang>>;

// FQN types

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JavaFqnPartType {
    Class,
    Interface,
    Enum,
    EnumConstant,
    Record,
    Annotation,
    AnnotationDeclaration,
    Constructor,
    Method,
    Lambda,
    Package,
    Field,
    Parameter,
    LocalVariable,
}

/// Java-specific metadata for FQN parts (currently empty, placeholder for future use)
#[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
pub struct JavaFqnMetadata;

/// Java-specific FQN part with metadata
pub type JavaFqnPart = FQNPart<JavaFqnPartType, JavaFqnMetadata>;

/// Java-specific FQN with rich metadata
pub type JavaFqn = Arc<SmallVec<[JavaFqnPart; 8]>>;

// Definition types

#[derive(Debug, Clone)]
pub enum JavaDefinitionMetadata {
    Class {
        super_types: Vec<JavaType>,
    },
    Method {
        return_type: JavaType,
    },
    Parameter {
        parameter_type: JavaType,
    },
    Field {
        field_type: JavaType,
    },
    LocalVariable {
        variable_type: Option<JavaType>,
        init: Option<JavaExpression>,
    },
}

/// Represents a Java definition found in the code
/// This is now a type alias using the generic DefinitionInfo with Java-specific types
pub type JavaDefinitionInfo = DefinitionInfo<JavaDefinitionType, JavaFqn, JavaDefinitionMetadata>;
pub type JavaDefinitions = Vec<JavaDefinitionInfo>;

/// Types of definitions that can be found in Java code
/// Limited to what is actually supported in the Java FQN implementation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JavaDefinitionType {
    Class,
    Interface,
    Enum,
    EnumConstant,
    Record,
    Annotation,
    AnnotationDeclaration,
    Constructor,
    Method,
    Lambda,
    // Exported but not indexed
    Package,
    Parameter,
    Field,
    LocalVariable,
}

impl JavaDefinitionType {
    pub fn from_fqn_part_type(fqn_part_type: &JavaFqnPartType) -> Option<Self> {
        match fqn_part_type {
            JavaFqnPartType::Class => Some(Self::Class),
            JavaFqnPartType::Interface => Some(Self::Interface),
            JavaFqnPartType::Enum => Some(Self::Enum),
            JavaFqnPartType::Record => Some(Self::Record),
            JavaFqnPartType::Annotation => Some(Self::Annotation),
            JavaFqnPartType::Method => Some(Self::Method),
            JavaFqnPartType::Lambda => Some(Self::Lambda),
            JavaFqnPartType::EnumConstant => Some(Self::EnumConstant),
            JavaFqnPartType::AnnotationDeclaration => Some(Self::AnnotationDeclaration),
            JavaFqnPartType::Constructor => Some(Self::Constructor),
            JavaFqnPartType::Field => Some(Self::Field),
            JavaFqnPartType::Parameter => Some(Self::Parameter),
            JavaFqnPartType::LocalVariable => Some(Self::LocalVariable),
            JavaFqnPartType::Package => Some(Self::Package),
        }
    }
}

impl DefinitionTypeInfo for JavaDefinitionType {
    /// Convert JavaDefinitionType to its string representation
    fn as_str(&self) -> &str {
        match self {
            JavaDefinitionType::Class => "Class",
            JavaDefinitionType::Interface => "Interface",
            JavaDefinitionType::Enum => "Enum",
            JavaDefinitionType::EnumConstant => "EnumConstant",
            JavaDefinitionType::Record => "Record",
            JavaDefinitionType::Annotation => "Annotation",
            JavaDefinitionType::AnnotationDeclaration => "AnnotationDeclaration",
            JavaDefinitionType::Constructor => "Constructor",
            JavaDefinitionType::Method => "Method",
            JavaDefinitionType::Lambda => "Lambda",
            JavaDefinitionType::Parameter => "Parameter",
            JavaDefinitionType::Field => "Field",
            JavaDefinitionType::LocalVariable => "LocalVariable",
            JavaDefinitionType::Package => "Package",
        }
    }
}

// Import types

pub type JavaImportedSymbolInfo = ImportedSymbolInfo<JavaImportType, JavaFqn>;
pub type JavaImports = Vec<JavaImportedSymbolInfo>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JavaImportType {
    Import,
    WildcardImport,
    StaticImport,
}

impl ImportTypeInfo for JavaImportType {
    fn as_str(&self) -> &str {
        match self {
            JavaImportType::Import => "Import",
            JavaImportType::WildcardImport => "WildcardImport",
            JavaImportType::StaticImport => "StaticImport",
        }
    }
}

// Reference types

#[derive(Debug, Clone)]
pub struct JavaType {
    pub name: String,
}

pub type JavaExpressions = Vec<JavaExpression>;

#[derive(Debug, Clone)]
pub enum JavaExpression {
    ArrayAccess {
        target: Box<JavaExpression>,
    },
    FieldAccess {
        target: Box<JavaExpression>,
        member: String,
    },
    MemberMethodCall {
        target: Box<JavaExpression>,
        member: String,
    },
    Identifier {
        name: String,
    },
    MethodCall {
        name: String,
    },
    MethodReference {
        target: Box<JavaExpression>,
        member: String,
    },
    Index {
        target: Box<JavaExpression>,
    },
    ObjectCreation {
        target: Box<JavaType>,
    },
    ArrayCreation {
        target: Box<JavaType>,
    },
    ArrayItem {
        target: Box<JavaExpression>,
    },
    Annotation {
        name: String,
    },
    This,
    Super,
    Literal,
}

impl JavaExpression {
    pub fn variant_name(&self) -> &'static str {
        match self {
            JavaExpression::ArrayAccess { .. } => "ArrayAccess",
            JavaExpression::FieldAccess { .. } => "FieldAccess",
            JavaExpression::MemberMethodCall { .. } => "MemberMethodCall",
            JavaExpression::Identifier { .. } => "Identifier",
            JavaExpression::MethodCall { .. } => "MethodCall",
            JavaExpression::MethodReference { .. } => "MethodReference",
            JavaExpression::Index { .. } => "Index",
            JavaExpression::ObjectCreation { .. } => "ObjectCreation",
            JavaExpression::ArrayCreation { .. } => "ArrayCreation",
            JavaExpression::ArrayItem { .. } => "ArrayItem",
            JavaExpression::Annotation { .. } => "Annotation",
            JavaExpression::This => "This",
            JavaExpression::Super => "Super",
            JavaExpression::Literal => "Literal",
        }
    }
}

#[derive(Debug, Clone)]
pub enum JavaReferenceType {
    Call,
}

pub type JavaTargetResolution =
    TargetResolution<JavaDefinitionInfo, JavaImportedSymbolInfo, JavaExpression>;
pub type JavaReferences = Vec<JavaReferenceInfo>;
pub type JavaReferenceInfo =
    ReferenceInfo<JavaTargetResolution, JavaReferenceType, JavaExpression, JavaFqn>;
