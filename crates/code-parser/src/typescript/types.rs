use std::sync::Arc;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::definitions::DefinitionInfo;
use crate::fqn::FQNPart;
use crate::imports::ImportTypeInfo;
use crate::imports::ImportedSymbolInfo;
use crate::utils::Range;
use swc_common::{SourceMap, sync::Lrc};
use swc_ecma_ast::Module;

// typescript/parser.rs
pub struct TypeScriptSwcAst {
    pub module: Module,
    pub source_map: Lrc<SourceMap>,
}

impl TypeScriptSwcAst {
    pub fn new(module: Module, source_map: Lrc<SourceMap>) -> Self {
        Self { module, source_map }
    }
}

// typescript/fqn.rs

pub type TypeScriptFqnPart = FQNPart<TypeScriptDefinitionType>;
pub type TypeScriptFqn = Arc<SmallVec<[TypeScriptFqnPart; 8]>>;

pub type TypeScriptNodeFqnMap<'a> =
    FxHashMap<Range, (Node<'a, StrDoc<SupportLang>>, TypeScriptFqn)>;

// typescript/definitions.rs
pub type TypeScriptDefinitionInfo = DefinitionInfo<TypeScriptDefinitionType, TypeScriptFqn>;

/// Types of definitions that can be found in TS/JS code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum TypeScriptDefinitionType {
    Class,
    NamedClassExpression,
    Method,
    Function,
    NamedFunctionExpression,
    NamedArrowFunction,
    NamedGeneratorFunctionExpression,
    NamedGeneratorFunctionDeclaration,
    NamedCallExpression,
    Interface,
    Namespace,
    Type,
    Enum,
}

impl TypeScriptDefinitionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TypeScriptDefinitionType::Class => "Class",
            TypeScriptDefinitionType::NamedClassExpression => "NamedClassExpression",
            TypeScriptDefinitionType::Method => "Method",
            TypeScriptDefinitionType::Function => "Function",
            TypeScriptDefinitionType::NamedArrowFunction => "NamedArrowFunction",
            TypeScriptDefinitionType::NamedFunctionExpression => "NamedFunctionExpression",
            TypeScriptDefinitionType::NamedGeneratorFunctionExpression => {
                "NamedGeneratorFunctionExpression"
            }
            TypeScriptDefinitionType::NamedGeneratorFunctionDeclaration => {
                "NamedGeneratorFunctionDeclaration"
            }
            TypeScriptDefinitionType::NamedCallExpression => "NamedCallExpression",
            TypeScriptDefinitionType::Interface => "Interface",
            TypeScriptDefinitionType::Namespace => "Namespace",
            TypeScriptDefinitionType::Type => "Type",
            TypeScriptDefinitionType::Enum => "Enum",
        }
    }

    pub fn from_node_kind(node_kind: &str) -> Option<TypeScriptDefinitionType> {
        match node_kind {
            "class" => Some(TypeScriptDefinitionType::NamedClassExpression),
            "class_declaration" => Some(TypeScriptDefinitionType::Class),
            "method_definition" => Some(TypeScriptDefinitionType::Method),
            "function_declaration" => Some(TypeScriptDefinitionType::Function),
            "arrow_function" => Some(TypeScriptDefinitionType::NamedArrowFunction),
            "function_expression" => Some(TypeScriptDefinitionType::NamedFunctionExpression),
            "generator_function" => {
                Some(TypeScriptDefinitionType::NamedGeneratorFunctionExpression)
            }
            "generator_function_declaration" => {
                Some(TypeScriptDefinitionType::NamedGeneratorFunctionDeclaration)
            }
            "call_expression" => Some(TypeScriptDefinitionType::NamedCallExpression),
            "interface_declaration" => Some(TypeScriptDefinitionType::Interface),
            "type_alias_declaration" => Some(TypeScriptDefinitionType::Type),
            "internal_module" => Some(TypeScriptDefinitionType::Namespace),
            "enum_declaration" => Some(TypeScriptDefinitionType::Enum),
            _ => None,
        }
    }

    pub fn to_node_kind(&self) -> &str {
        match self {
            TypeScriptDefinitionType::Class => "class",
            TypeScriptDefinitionType::NamedClassExpression => "class",
            TypeScriptDefinitionType::Method => "method_definition",
            TypeScriptDefinitionType::Function => "function_declaration",
            TypeScriptDefinitionType::NamedArrowFunction => "arrow_function",
            TypeScriptDefinitionType::NamedFunctionExpression => "function_expression",
            TypeScriptDefinitionType::NamedGeneratorFunctionExpression => "generator_function",
            TypeScriptDefinitionType::NamedGeneratorFunctionDeclaration => {
                "generator_function_declaration"
            }
            TypeScriptDefinitionType::NamedCallExpression => "call_expression",
            TypeScriptDefinitionType::Interface => "interface_declaration",
            TypeScriptDefinitionType::Namespace => "internal_module",
            TypeScriptDefinitionType::Type => "type_alias_declaration",
            TypeScriptDefinitionType::Enum => "enum_declaration",
        }
    }
}

// Import types

/// Represents a TypeScript/JS import found in the code
pub type TypeScriptImportedSymbolInfo = ImportedSymbolInfo<TypeScriptImportType, TypeScriptFqn>;

/// Types of imported symbols that can be found in TypeScript/JavaScript code
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TypeScriptImportType {
    DefaultImport,               // import React from 'react'
    NamedImport,                 // import { useState } from 'react'
    AliasedImport,               // import { Component as ReactComponent } from 'react'
    SvaRequireOrImport, // const VAR_NAME = opt<await> either(require('SOURCE'), import('SOURCE'))
    DestructuredImportOrRequire, // const { readFile } = opt<await> either(require('fs'), import('fs'))
    AliasedImportOrRequire, // const { readFile: fsRead } = opt<await> either(require('fs'), import('fs'))
    SideEffectImport,       // import 'reflect-metadata'
    SideEffectImportOrRequire, // import('reflect-metadata') or require('reflect-metadata')
    ImportAndRequire,       // import express = require('express')
    NamespaceImport,        // import * as React from 'react'
    // TODO: Capture type-information too
    TypeOnlyImport, // import type { FC } from 'react'
}

impl ImportTypeInfo for TypeScriptImportType {
    fn as_str(&self) -> &str {
        match self {
            TypeScriptImportType::DefaultImport => "DefaultImport",
            TypeScriptImportType::NamedImport => "NamedImport",
            TypeScriptImportType::AliasedImport => "AliasedImport",
            TypeScriptImportType::ImportAndRequire => "ImportAndRequire",
            TypeScriptImportType::NamespaceImport => "NamespaceImport",
            TypeScriptImportType::SideEffectImport => "SideEffectImport",
            TypeScriptImportType::TypeOnlyImport => "TypeOnlyImport",
            TypeScriptImportType::SvaRequireOrImport => "SvaRequireOrImport",
            TypeScriptImportType::SideEffectImportOrRequire => "SideEffectImportOrRequire",
            TypeScriptImportType::DestructuredImportOrRequire => "DestructuredImportOrRequire",
            TypeScriptImportType::AliasedImportOrRequire => "AliasedImportOrRequire",
        }
    }
}

/// Type-safe constants for capture variable names used in the TypeScript import rules
pub mod ts_import_meta_vars {
    // Default imports: import React from 'react'
    pub const DEFAULT_IMPORT_NAME: &str = "DEFAULT_IMPORT_NAME";
    pub const DEFAULT_IMPORT_SOURCE: &str = "DEFAULT_IMPORT_SOURCE";

    // Named imports: import { useState } from 'react'
    pub const NAMED_IMPORT_NAME: &str = "NAMED_IMPORT_NAME";
    pub const NAMED_IMPORT_SOURCE: &str = "NAMED_IMPORT_SOURCE";

    // Aliased imports: import { Component as ReactComponent } from 'react'
    pub const ALIASED_IMPORT_ORIGINAL: &str = "ALIASED_IMPORT_ORIGINAL";
    pub const ALIASED_IMPORT_ALIAS: &str = "ALIASED_IMPORT_ALIAS";
    pub const ALIASED_IMPORT_SOURCE: &str = "ALIASED_IMPORT_SOURCE";

    // Require Or Import imports: const VAR_NAME = opt<await> either(require('SOURCE'), import('SOURCE'))
    pub const SVA_REQUIRE_OR_IMPORT_NAME: &str = "SVA_REQUIRE_OR_IMPORT_NAME";
    pub const SVA_REQUIRE_OR_IMPORT_SOURCE: &str = "SVA_REQUIRE_OR_IMPORT_SOURCE";

    // Destructured require: const { readFile } = opt<await> either(require('fs'), import('fs'))
    pub const DESTRUCTURED_REQUIRE_OR_IMPORT_NAME: &str = "DESTRUCTURED_REQUIRE_OR_IMPORT_NAME";
    pub const DESTRUCTURED_REQUIRE_OR_IMPORT_SOURCE: &str = "DESTRUCTURED_REQUIRE_OR_IMPORT_SOURCE";

    // Aliased require: const { readFile: fsRead } = opt<await> either(require('fs'), import('fs'))
    pub const ALIASED_REQUIRE_OR_IMPORT_NAME: &str = "ALIASED_REQUIRE_OR_IMPORT_NAME";
    pub const ALIASED_REQUIRE_OR_IMPORT_ALIAS: &str = "ALIASED_REQUIRE_OR_IMPORT_ALIAS";
    pub const ALIASED_REQUIRE_OR_IMPORT_SOURCE: &str = "ALIASED_REQUIRE_OR_IMPORT_SOURCE";

    // Namespace imports: import * as React from 'react'
    pub const NAMESPACE_IMPORT_NAME: &str = "NAMESPACE_IMPORT_NAME";
    pub const NAMESPACE_IMPORT_SOURCE: &str = "NAMESPACE_IMPORT_SOURCE";

    // Import-require: import express = require('express')
    pub const IMPORT_AND_REQUIRE_NAME: &str = "IMPORT_AND_REQUIRE_NAME";
    pub const IMPORT_AND_REQUIRE_SOURCE: &str = "IMPORT_AND_REQUIRE_SOURCE";

    // Side effect imports: import 'reflect-metadata'
    pub const SIDE_EFFECT_IMPORT_SOURCE: &str = "SIDE_EFFECT_IMPORT_SOURCE";

    // Side effect requires: opt<await> either(require('SOURCE'), import('SOURCE'))
    pub const SIDE_EFFECT_IMPORT_OR_REQUIRE_SOURCE: &str = "SIDE_EFFECT_IMPORT_OR_REQUIRE_SOURCE";

    // Type-only imports: import type { FC } from 'react'
    pub const TYPE_ONLY_IMPORT_NAME: &str = "TYPE_ONLY_IMPORT_NAME";
    pub const TYPE_ONLY_IMPORT_SOURCE: &str = "TYPE_ONLY_IMPORT_SOURCE";
}

/// TypeScript node kinds for import-related constructs
pub mod ts_import_node_types {
    pub const IMPORT_STATEMENT: &str = "import_statement";
    pub const IMPORT_CLAUSE: &str = "import_clause";
    pub const IMPORT_SPECIFIER: &str = "import_specifier";
    pub const NAMESPACE_IMPORT: &str = "namespace_import";
    pub const NAMED_IMPORTS: &str = "named_imports";
    pub const IMPORT_REQUIRE_CLAUSE: &str = "import_require_clause";
    pub const VARIABLE_DECLARATOR: &str = "variable_declarator";
    pub const CALL_EXPRESSION: &str = "call_expression";
    pub const OBJECT_PATTERN: &str = "object_pattern";
    pub const PAIR_PATTERN: &str = "pair_pattern";
    pub const SHORTHAND_PROPERTY_IDENTIFIER_PATTERN: &str = "shorthand_property_identifier_pattern";
    pub const AWAIT_EXPRESSION: &str = "await_expression";
    pub const IDENTIFIER: &str = "identifier";
    pub const STRING: &str = "string";
    pub const ASSIGNMENT_EXPRESSION: &str = "assignment_expression";
}

pub type TypeScriptScopeStack = SmallVec<[TypeScriptFqnPart; 8]>;
pub type TypeScriptImportedSymbolInfoVec = SmallVec<[TypeScriptImportedSymbolInfo; 8]>;
