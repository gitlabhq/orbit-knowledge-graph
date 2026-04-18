use std::sync::Arc;

use smallvec::SmallVec;

use crate::legacy::parser::{
    definitions::{DefinitionInfo, DefinitionTypeInfo},
    fqn::FQNPart,
    imports::{ImportTypeInfo, ImportedSymbolInfo},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CSharpFqnPartType {
    Namespace,
    Class,
    InstanceMethod,
    StaticMethod,
    ExtensionMethod,
    Property,
    Field,
    Constructor,
    Finalizer,
    Delegate,
    Interface,
    Enum,
    Struct,
    Record,
    Lambda,
    Operator,
    Indexer,
    Event,
    AnonymousType,
}

pub type CSharpFqnPart = FQNPart<CSharpFqnPartType>;

pub type CSharpFqn = Arc<SmallVec<[CSharpFqnPart; 16]>>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CSharpImportType {
    Default,      // using System;
    Global,       // global using System;
    Static,       // using static System.Console;
    Alias,        // using Console = System.Console;
    GlobalStatic, // global using static System.Console;
    GlobalAlias,  // global using Console = System.Console;
}

impl ImportTypeInfo for CSharpImportType {
    fn as_str(&self) -> &str {
        match self {
            CSharpImportType::Default => "Default",
            CSharpImportType::Global => "Global",
            CSharpImportType::Static => "Static",
            CSharpImportType::Alias => "Alias",
            CSharpImportType::GlobalStatic => "GlobalStatic",
            CSharpImportType::GlobalAlias => "GlobalAlias",
        }
    }
}

pub type CSharpImportedSymbolInfo = ImportedSymbolInfo<CSharpImportType, CSharpFqn>;
pub type CSharpImports = Vec<CSharpImportedSymbolInfo>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CSharpDefinitionType {
    Class,
    InstanceMethod,
    StaticMethod,
    ExtensionMethod,
    Property,
    Constructor,
    Finalizer,
    Interface,
    Enum,
    Struct,
    Record,
    Delegate,
    Lambda,
    Operator,
    Field,
    AnonymousType,
    Indexer,
    Event,
}

impl CSharpDefinitionType {
    pub fn from_fqn_part_type(part_type: &CSharpFqnPartType) -> Option<Self> {
        match part_type {
            CSharpFqnPartType::Class => Some(Self::Class),
            CSharpFqnPartType::InstanceMethod => Some(Self::InstanceMethod),
            CSharpFqnPartType::StaticMethod => Some(Self::StaticMethod),
            CSharpFqnPartType::ExtensionMethod => Some(Self::ExtensionMethod),
            CSharpFqnPartType::Property => Some(Self::Property),
            CSharpFqnPartType::Constructor => Some(Self::Constructor),
            CSharpFqnPartType::Finalizer => Some(Self::Finalizer),
            CSharpFqnPartType::Interface => Some(Self::Interface),
            CSharpFqnPartType::Enum => Some(Self::Enum),
            CSharpFqnPartType::Struct => Some(Self::Struct),
            CSharpFqnPartType::Record => Some(Self::Record),
            CSharpFqnPartType::Delegate => Some(Self::Delegate),
            CSharpFqnPartType::Lambda => Some(Self::Lambda),
            CSharpFqnPartType::Operator => Some(Self::Operator),
            CSharpFqnPartType::Field => Some(Self::Field),
            CSharpFqnPartType::AnonymousType => Some(Self::AnonymousType),
            CSharpFqnPartType::Indexer => Some(Self::Indexer),
            CSharpFqnPartType::Event => Some(Self::Event),
            CSharpFqnPartType::Namespace => None,
        }
    }
}

impl DefinitionTypeInfo for CSharpDefinitionType {
    fn as_str(&self) -> &str {
        match self {
            CSharpDefinitionType::Class => "Class",
            CSharpDefinitionType::InstanceMethod => "InstanceMethod",
            CSharpDefinitionType::StaticMethod => "StaticMethod",
            CSharpDefinitionType::ExtensionMethod => "ExtensionMethod",
            CSharpDefinitionType::Property => "Property",
            CSharpDefinitionType::Constructor => "Constructor",
            CSharpDefinitionType::Finalizer => "Finalizer",
            CSharpDefinitionType::Interface => "Interface",
            CSharpDefinitionType::Enum => "Enum",
            CSharpDefinitionType::Struct => "Struct",
            CSharpDefinitionType::Record => "Record",
            CSharpDefinitionType::Delegate => "Delegate",
            CSharpDefinitionType::Lambda => "Lambda",
            CSharpDefinitionType::Operator => "Operator",
            CSharpDefinitionType::Field => "Field",
            CSharpDefinitionType::AnonymousType => "AnonymousType",
            CSharpDefinitionType::Indexer => "Indexer",
            CSharpDefinitionType::Event => "Event",
        }
    }
}

pub type CSharpDefinitionInfo = DefinitionInfo<CSharpDefinitionType, CSharpFqn>;

pub type CSharpDefinitions = Vec<CSharpDefinitionInfo>;
