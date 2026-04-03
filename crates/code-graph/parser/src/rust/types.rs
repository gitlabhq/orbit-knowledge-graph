//! Shared types for the Rust parser
//!
//! This module defines the type hierarchy for Rust language constructs that can appear
//! in Fully Qualified Names (FQNs).
//!
//! ## Type Hierarchy
//!
//! - `RustFqnPartType`: Represents **any symbol** that can appear in an FQN path, including
//!   both definitions and contextual elements like modules, traits, impls, and type aliases.
//!
//! - `RustDefinitionType`: Represents **callable/definable** constructs that create new scopes
//!   or can be invoked. These are the primary targets for code analysis and indexing.

use crate::definitions::DefinitionTypeInfo;
use crate::fqn::FQNPart;
use crate::imports::ImportTypeInfo;
use crate::utils::Range;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// Macro to define the core Rust definition types that can be called or invoked.
/// These represent the fundamental constructs that create new scopes or can be executed.
macro_rules! define_rust_definition_types {
    ($(($variant:ident, $str_repr:literal, $doc:literal)),* $(,)?) => {
        /// Represents a **callable/definable** Rust construct.
        ///
        /// These are the primary targets for code analysis because they:
        /// - Create new scopes (modules, structs, enums, traits)
        /// - Can be invoked/called (functions, methods, associated functions)
        /// - Represent reusable code units (macros, closures)
        ///
        /// This is a **subset** of `RustFqnPartType` - every definition type
        /// can appear in an FQN, but not every FQN part is a definition.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum RustDefinitionType {
            $(
                #[doc = $doc]
                $variant,
            )*
        }

        impl DefinitionTypeInfo for RustDefinitionType {
            fn as_str(&self) -> &str {
                match self {
                    $(RustDefinitionType::$variant => $str_repr,)*
                }
            }
        }
    };
}

/// Macro to define all possible FQN part types, including both definitions and contextual elements.
macro_rules! define_rust_fqn_part_types {
    (
        // Definition types that map directly from RustDefinitionType
        definitions: [$(($def_variant:ident, $def_str:literal, $def_doc:literal)),* $(,)?],
        // Additional contextual types that can appear in FQNs but aren't definitions
        contextual: [$(($ctx_variant:ident, $ctx_str:literal, $ctx_doc:literal)),* $(,)?]
    ) => {
        /// Represents **any symbol** that can be part of a Rust FQN path.
        ///
        /// This includes:
        /// - **Definition types**: Callable/definable constructs (functions, structs, etc.)
        /// - **Contextual types**: Supporting elements that provide context in FQNs
        ///   - `TypeAlias`: Type aliases that create new names for existing types
        ///   - `Constant`: Named constants that aren't callable
        ///   - `Static`: Static variables
        ///   - `Union`: Union types
        ///   - `Unknown`: Fallback for unrecognized constructs
        ///
        /// This is a **superset** of `RustDefinitionType` - it includes all definition
        /// types plus additional contextual elements needed for complete FQN representation.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum RustFqnPartType {
            // Definition types (these map 1:1 with RustDefinitionType)
            $(
                #[doc = $def_doc]
                $def_variant,
            )*
            // Contextual types (these provide additional FQN context)
            $(
                #[doc = $ctx_doc]
                $ctx_variant,
            )*
        }

        impl RustFqnPartType {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(RustFqnPartType::$def_variant => $def_str,)*
                    $(RustFqnPartType::$ctx_variant => $ctx_str,)*
                }
            }

            /// Check if this FQN part type represents a callable/definable construct.
            pub fn is_definition(&self) -> bool {
                matches!(self, $(RustFqnPartType::$def_variant)|*)
            }
        }

        // Automatic conversion from definition types to FQN part types
        impl From<RustDefinitionType> for RustFqnPartType {
            fn from(def_type: RustDefinitionType) -> Self {
                match def_type {
                    $(RustDefinitionType::$def_variant => RustFqnPartType::$def_variant,)*
                }
            }
        }

        // Fallible conversion from FQN part types to definition types
        impl TryFrom<RustFqnPartType> for RustDefinitionType {
            type Error = ();

            fn try_from(part_type: RustFqnPartType) -> Result<Self, Self::Error> {
                match part_type {
                    $(RustFqnPartType::$def_variant => Ok(RustDefinitionType::$def_variant),)*
                    _ => Err(()), // Contextual types cannot be converted to definition types
                }
            }
        }
    };
}

/// Rust AST node type constants from tree-sitter-rust
pub mod node_types {
    pub const MODULE: &str = "mod_item";
    pub const STRUCT: &str = "struct_item";
    pub const ENUM: &str = "enum_item";
    pub const TRAIT: &str = "trait_item";
    pub const IMPL: &str = "impl_item";
    pub const FUNCTION: &str = "function_item";
    pub const METHOD: &str = "function_item"; // Methods are also function_item in tree-sitter-rust
    pub const ASSOCIATED_FUNCTION: &str = "function_item"; // Associated functions are also function_item
    pub const FUNCTION_SIGNATURE: &str = "function_signature_item"; // Trait method signatures
    pub const MACRO_DEFINITION: &str = "macro_definition";
    pub const MACRO_INVOCATION: &str = "macro_invocation";
    pub const CLOSURE: &str = "closure_expression";
    pub const CONST: &str = "const_item";
    pub const STATIC: &str = "static_item";
    pub const TYPE_ALIAS: &str = "type_item";
    pub const UNION: &str = "union_item";
    pub const VARIANT: &str = "enum_variant";
    pub const FIELD: &str = "field_declaration";
    pub const IDENTIFIER: &str = "identifier";
    pub const TYPE_IDENTIFIER: &str = "type_identifier";
    pub const SCOPED_IDENTIFIER: &str = "scoped_identifier";
    pub const USE_DECLARATION: &str = "use_declaration";
    pub const USE_LIST: &str = "use_list";
    pub const USE_AS_CLAUSE: &str = "use_as_clause";
    pub const SCOPED_USE_LIST: &str = "scoped_use_list";
    pub const USE_WILDCARD: &str = "use_wildcard";
    pub const EXTERN_CRATE: &str = "extern_crate_declaration";
    pub const VISIBILITY_MODIFIER: &str = "visibility_modifier";
    pub const CRATE: &str = "crate";
    pub const SUPER: &str = "super";
    pub const SELF: &str = "self";
    pub const SELF_TYPE: &str = "Self";
}

// Define the core definition types with their string representations and documentation
define_rust_definition_types! {
    (Module, "Module", "A Rust module definition (`mod my_module`)"),
    (Struct, "Struct", "A Rust struct definition (`struct MyStruct { ... }`)"),
    (Enum, "Enum", "A Rust enum definition (`enum MyEnum { ... }`)"),
    (Trait, "Trait", "A Rust trait definition (`trait MyTrait { ... }`)"),
    (Impl, "Impl", "A Rust impl block (`impl MyStruct { ... }` or `impl Trait for Type { ... }`)"),
    (Function, "Function", "A Rust function definition (`fn my_function() { ... }`)"),
    (Method, "Method", "A Rust method definition within an impl block"),
    (AssociatedFunction, "AssociatedFunction", "A Rust associated function (like `Self::new()`)"),
    (Macro, "Macro", "A Rust macro definition (`macro_rules! my_macro { ... }`)"),
    (MacroCall, "MacroCall", "A Rust macro invocation (`my_macro!()`)"),
    (Closure, "Closure", "A Rust closure expression (`|x| x + 1`)"),
    (Variant, "Variant", "A Rust enum variant (`MyVariant` in `enum MyEnum { MyVariant }`)"),
    (Field, "Field", "A Rust struct field (`my_field: i32` in `struct MyStruct { my_field: i32 }`)"),
    (Constant, "Constant", "A Rust constant definition (`const MY_CONST: i32 = 42`)"),
    (Static, "Static", "A Rust static variable definition (`static MY_STATIC: i32 = 42`)"),
    (TypeAlias, "TypeAlias", "A Rust type alias definition (`type MyType = OtherType`)"),
    (Union, "Union", "A Rust union definition (`union MyUnion { ... }`)"),
}

// Define all FQN part types with comprehensive documentation
define_rust_fqn_part_types! {
    definitions: [
        (Module, "Module", "A Rust module (`mod my_module`) that can appear in FQN paths"),
        (Struct, "Struct", "A Rust struct (`struct MyStruct`) that can be part of an FQN path"),
        (Enum, "Enum", "A Rust enum (`enum MyEnum`) that can be part of an FQN path"),
        (Trait, "Trait", "A Rust trait (`trait MyTrait`) that can be part of an FQN path"),
        (Impl, "Impl", "A Rust impl block that can contain methods and associated functions in FQN paths"),
        (Function, "Function", "A Rust function (`fn my_function`) that can be part of an FQN path"),
        (Method, "Method", "A Rust method within an impl block that can be part of an FQN path"),
        (AssociatedFunction, "AssociatedFunction", "A Rust associated function (like `Self::new()`) that can be part of an FQN path"),
        (Macro, "Macro", "A Rust macro definition (`macro_rules! my_macro`) that can be part of an FQN path"),
        (MacroCall, "MacroCall", "A Rust macro invocation (`my_macro!()`) that can be part of an FQN path"),
        (Closure, "Closure", "A Rust closure expression (`|x| x + 1`) that can be part of an FQN path"),
        (Variant, "Variant", "A Rust enum variant that can be part of an FQN path (`MyEnum::MyVariant`)"),
        (Field, "Field", "A Rust struct field that can be part of an FQN path in certain contexts"),
        (Constant, "Constant", "A Rust constant (`const MY_CONST: i32 = 42`) that can be part of an FQN path"),
        (Static, "Static", "A Rust static variable (`static MY_STATIC: i32 = 42`) that can be part of an FQN path"),
        (TypeAlias, "TypeAlias", "A Rust type alias (`type MyType = OtherType`) that can be part of an FQN path"),
        (Union, "Union", "A Rust union (`union MyUnion`) that can be part of an FQN path"),
    ],
    contextual: [
        (Unknown, "Unknown", "A fallback type for unrecognized constructs that appear in FQN paths"),
    ]
}

/// Rust-specific FQN part with metadata
pub type RustFqnPart = FQNPart<RustFqnPartType>;

/// Rust-specific FQN with rich metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RustFqn {
    /// Note: we use a SmallVec here because FQN parts shouldn't be too deep
    /// SmallVec will automatically spill over to the heap if it exceeds 8 elements
    /// https://crates.io/crates/smallvec
    pub parts: Arc<SmallVec<[RustFqnPart; 8]>>,
}

impl RustFqn {
    pub fn new(parts: SmallVec<[RustFqnPart; 8]>) -> Self {
        Self {
            parts: Arc::new(parts),
        }
    }

    pub fn len(&self) -> usize {
        self.parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

/// Maps node ranges to their corresponding AST nodes and FQN parts
pub type RustNodeFqnMap<'a> = FxHashMap<
    Range,
    (
        Node<'a, StrDoc<SupportLang>>,
        Arc<SmallVec<[RustFqnPart; 8]>>,
    ),
>;

/// Represents a Rust definition found in the code
/// This is now a type alias using the generic DefinitionInfo with Rust-specific types
pub type RustDefinitionInfo = crate::definitions::DefinitionInfo<RustDefinitionType, RustFqn>;

/// Macro to define Rust import types covering all import mechanisms in Rust
macro_rules! define_rust_import_types {
    ($(($variant:ident, $str_repr:literal, $doc:literal)),* $(,)?) => {
        /// Comprehensive Rust import types covering all import mechanisms in Rust
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        pub enum RustImportType {
            $(
                #[doc = $doc]
                $variant,
            )*
        }

        impl ImportTypeInfo for RustImportType {
            fn as_str(&self) -> &str {
                match self {
                    $(RustImportType::$variant => $str_repr,)*
                }
            }
        }
    };
}

// Define the comprehensive Rust import types
define_rust_import_types! {
    (Use, "Use", "Basic use statement: `use std::collections::HashMap;`"),
    (GlobUse, "GlobUse", "Glob import: `use std::collections::*;`"),
    (AliasedUse, "AliasedUse", "Aliased import: `use std::collections::HashMap as Map;`"),
    (ExternCrate, "ExternCrate", "Extern crate declaration: `extern crate serde;`"),
    (AliasedExternCrate, "AliasedExternCrate", "Aliased extern crate: `extern crate serde as ser;`"),
    (ReExport, "ReExport", "Public re-export: `pub use std::collections::HashMap;`"),
    (ReExportAliased, "ReExportAliased", "Public re-export with alias: `pub use std::io::Error as PublicError;`"),
    (ReExportGlob, "ReExportGlob", "Public glob re-export: `pub use std::collections::*;`"),
    (UseGroup, "UseGroup", "Use group: `use std::io::{Error, Result};`"),
    (PubUseGroup, "PubUseGroup", "Public use group: `pub use crate::internal::{Config, Database as DB};`"),
    (NestedUseGroup, "NestedUseGroup", "Nested use group: `use std::{collections::{HashMap}, sync::{Arc}};`"),
    (TopLevelUseGroup, "TopLevelUseGroup", "Top-level use group: `use std::{collections::{...}, sync::{...}};`"),
    (ModDeclaration, "ModDeclaration", "Module declaration: `mod my_module;` or `mod my_module { ... }`"),
}
