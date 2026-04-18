//! Shared types for the Ruby parser
//!
//! This module defines the type hierarchy for Ruby language constructs that can appear
//! in Fully Qualified Names (FQNs).
//!
//! ## Type Hierarchy
//!
//! - `RubyFqnPartType`: Represents **any symbol** that can appear in an FQN path, including
//!   both definitions and contextual elements like constants, receivers, and blocks.
//!
//! - `RubyDefinitionType`: Represents **callable/definable** constructs that create new scopes
//!   or can be invoked. These are the primary targets for code analysis and indexing.

use crate::legacy::parser::definitions::DefinitionTypeInfo;
use crate::legacy::parser::fqn::FQNPart;
use crate::utils::Range;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

/// Macro to define the core Ruby definition types that can be called or invoked.
/// These represent the fundamental constructs that create new scopes or can be executed.
macro_rules! define_ruby_definition_types {
    ($(($variant:ident, $str_repr:literal, $doc:literal)),* $(,)?) => {
        /// Represents a **callable/definable** Ruby construct.
        ///
        /// These are the primary targets for code analysis because they:
        /// - Create new scopes (classes, modules)
        /// - Can be invoked/called (methods, lambdas, procs)
        /// - Represent reusable code units
        ///
        /// This is a **subset** of `RubyFqnPartType` - every definition type
        /// can appear in an FQN, but not every FQN part is a definition.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
        pub enum RubyDefinitionType {
            $(
                #[doc = $doc]
                $variant,
            )*
        }

        impl DefinitionTypeInfo for RubyDefinitionType {
            fn as_str(&self) -> &str {
                match self {
                    $(RubyDefinitionType::$variant => $str_repr,)*
                }
            }
        }
    };
}

/// Macro to define all possible FQN part types, including both definitions and contextual elements.
macro_rules! define_ruby_fqn_part_types {
    (
        // Definition types that map directly from RubyDefinitionType
        definitions: [$(($def_variant:ident, $def_str:literal)),* $(,)?],
        // Additional contextual types that can appear in FQNs but aren't definitions
        contextual: [$(($ctx_variant:ident, $ctx_str:literal)),* $(,)?]
    ) => {
        /// Represents **any symbol** that can be part of a Ruby FQN path.
        ///
        /// This includes:
        /// - **Definition types**: Callable/definable constructs (classes, methods, etc.)
        /// - **Contextual types**: Supporting elements that provide context in FQNs
        ///   - `Constant`: Named constants that aren't callable
        ///   - `Receiver`: Objects that receive method calls (e.g., `user` in `user.method`)
        ///   - `Block`: Anonymous code blocks
        ///   - `Unknown`: Fallback for unrecognized constructs
        ///
        /// This is a **superset** of `RubyDefinitionType` - it includes all definition
        /// types plus additional contextual elements needed for complete FQN representation.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum RubyFqnPartType {
            // Definition types (these map 1:1 with RubyDefinitionType)
            $(
                #[doc = concat!("A Ruby ", $def_str, " (definition type)")]
                $def_variant,
            )*
            // Contextual types (these provide additional FQN context)
            $(
                #[doc = concat!("A Ruby ", $ctx_str, " (contextual type)")]
                $ctx_variant,
            )*
        }

        impl RubyFqnPartType {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(RubyFqnPartType::$def_variant => $def_str,)*
                    $(RubyFqnPartType::$ctx_variant => $ctx_str,)*
                }
            }

            /// Check if this FQN part type represents a callable/definable construct.
            pub fn is_definition(&self) -> bool {
                matches!(self, $(RubyFqnPartType::$def_variant)|*)
            }
        }

        // Automatic conversion from definition types to FQN part types
        impl From<RubyDefinitionType> for RubyFqnPartType {
            fn from(def_type: RubyDefinitionType) -> Self {
                match def_type {
                    $(RubyDefinitionType::$def_variant => RubyFqnPartType::$def_variant,)*
                }
            }
        }

        // Fallible conversion from FQN part types to definition types
        impl TryFrom<RubyFqnPartType> for RubyDefinitionType {
            type Error = ();

            fn try_from(part_type: RubyFqnPartType) -> Result<Self, Self::Error> {
                if part_type.is_definition() {
                    match part_type {
                        $(RubyFqnPartType::$def_variant => Ok(RubyDefinitionType::$def_variant),)*
                        _ => unreachable!("is_definition() returned true but no matching definition type"),
                    }
                } else {
                    Err(()) // Contextual types cannot be converted to definition types
                }
            }
        }
    };
}
/// Ruby AST node type constants from tree-sitter-ruby
pub mod node_types {
    pub const CLASS: &str = "class";
    pub const MODULE: &str = "module";
    pub const METHOD: &str = "method";
    pub const SINGLETON_METHOD: &str = "singleton_method";
    pub const ASSIGNMENT: &str = "assignment";
    pub const CALL: &str = "call";
    pub const DO_BLOCK: &str = "do_block";
    pub const BLOCK: &str = "block";
    pub const CONSTANT: &str = "constant";
    pub const IDENTIFIER: &str = "identifier";
    pub const INSTANCE_VARIABLE: &str = "instance_variable";
    pub const CLASS_VARIABLE: &str = "class_variable";
    pub const SELF: &str = "self";
    pub const RECEIVER: &str = "receiver";
    pub const STRING: &str = "string";
    pub const SIMPLE_SYMBOL: &str = "simple_symbol";
    pub const ARGUMENTS: &str = "arguments";
    pub const ARGUMENT_LIST: &str = "argument_list";
    pub const HASH: &str = "hash";
    pub const ARRAY: &str = "array";
    pub const PAIR: &str = "pair";
    pub const BODY_STATEMENT: &str = "body_statement";
    pub const PROGRAM: &str = "program";
}

/// Ruby method name constants for import detection
pub mod method_names {
    pub const REQUIRE: &str = "require";
    pub const REQUIRE_RELATIVE: &str = "require_relative";
    pub const LOAD: &str = "load";
    pub const AUTOLOAD: &str = "autoload";
    pub const NEW: &str = "new";
    pub const LAMBDA: &str = "lambda";
}

/// Ruby constant name strings for import detection
pub mod constant_names {
    pub const KERNEL: &str = "Kernel";
    pub const PROC: &str = "Proc";
}

/// Common Ruby constants used for parsing and AST traversal
/// These use byte strings for zero-allocation comparisons during parsing
pub mod constants {
    /// Ruby method names for import detection (as byte strings)
    pub const REQUIRE: &[u8] = b"require";
    pub const REQUIRE_RELATIVE: &[u8] = b"require_relative";
    pub const LOAD: &[u8] = b"load";
    pub const AUTOLOAD: &[u8] = b"autoload";
    pub const NEW: &[u8] = b"new";
    pub const LAMBDA: &[u8] = b"lambda";
    pub const PROC: &[u8] = b"proc";

    /// Ruby constant names (as byte strings)
    pub const KERNEL: &[u8] = b"Kernel";
    pub const PROC_CONST: &[u8] = b"Proc";

    /// Common fallback names
    pub const UNKNOWN: &[u8] = b"unknown";
    pub const SELF_NAME: &[u8] = b"self";
    pub const BLOCK: &[u8] = b"block";

    /// Check if bytes match a known constant (zero-allocation comparison)
    pub fn matches_require(bytes: &[u8]) -> bool {
        bytes == REQUIRE
    }
    pub fn matches_require_relative(bytes: &[u8]) -> bool {
        bytes == REQUIRE_RELATIVE
    }
    pub fn matches_load(bytes: &[u8]) -> bool {
        bytes == LOAD
    }
    pub fn matches_autoload(bytes: &[u8]) -> bool {
        bytes == AUTOLOAD
    }
    pub fn matches_new(bytes: &[u8]) -> bool {
        bytes == NEW
    }
    pub fn matches_lambda(bytes: &[u8]) -> bool {
        bytes == LAMBDA
    }
    pub fn matches_proc(bytes: &[u8]) -> bool {
        bytes == PROC
    }
    pub fn matches_kernel(bytes: &[u8]) -> bool {
        bytes == KERNEL
    }
    pub fn matches_proc_const(bytes: &[u8]) -> bool {
        bytes == PROC_CONST
    }
}

// Define the core definition types with their string representations and documentation
define_ruby_definition_types! {
    (Class, "Class", "A Ruby class definition (`class MyClass`)"),
    (Module, "Module", "A Ruby module definition (`module MyModule`)"),
    (Method, "Method", "A Ruby method definition (`def my_method`)"),
    (SingletonMethod, "SingletonMethod", "A Ruby singleton method definition (`def self.my_method` or `def obj.my_method`)"),
    (Lambda, "Lambda", "A Ruby lambda expression assigned to a variable (`my_lambda = lambda { |x| x + 1 }`)"),
    (Proc, "Proc", "A Ruby proc expression assigned to a variable (`my_proc = Proc.new { |x| x + 1 }`)"),
}

// Define all FQN part types, including both definitions and contextual elements
define_ruby_fqn_part_types! {
    definitions: [
        (Class, "Class"),
        (Module, "Module"),
        (Method, "Method"),
        (SingletonMethod, "SingletonMethod"),
        (Lambda, "Lambda"),
        (Proc, "Proc"),
    ],
    contextual: [
        (Constant, "Constant"),     // Named constants (MY_CONSTANT = "value")
        (Receiver, "Receiver"),     // Method call receivers (user.method -> "user")
        (Block, "Block"),           // Anonymous blocks ({ |x| x + 1 })
        (Unknown, "Unknown"),       // Fallback for unrecognized constructs
    ]
}
/// Ruby-specific FQN part with metadata
pub type RubyFqnPart = FQNPart<RubyFqnPartType>;
/// Ruby-specific FQN with rich metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RubyFqn {
    /// Note: we use a SmallVec here because FQN parts shouldn't be too deep
    /// SmallVec will automatically spill over to the heap if it exceeds 8 elements
    /// https://crates.io/crates/smallvec
    pub parts: Arc<SmallVec<[RubyFqnPart; 8]>>,
}

impl RubyFqn {
    pub fn new(parts: SmallVec<[RubyFqnPart; 8]>) -> Self {
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
pub type RubyNodeFqnMap<'a> = FxHashMap<
    Range,
    (
        treesitter_visit::Node<
            'a,
            treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>,
        >,
        Arc<SmallVec<[RubyFqnPart; 8]>>,
    ),
>;

/// Ruby import types representing different ways to load code
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RubyImportType {
    /// `require 'gem'` - Load from $LOAD_PATH, cached
    Require,
    /// `require_relative '../path'` - Load relative to current file, cached
    RequireRelative,
    /// `load 'file.rb'` - Load every time, not cached
    Load,
    /// `autoload :Constant, 'path'` - Lazy load on first access
    Autoload,
}

impl crate::legacy::parser::imports::ImportTypeInfo for RubyImportType {
    fn as_str(&self) -> &str {
        match self {
            RubyImportType::Require => "Require",
            RubyImportType::RequireRelative => "RequireRelative",
            RubyImportType::Load => "Load",
            RubyImportType::Autoload => "Autoload",
        }
    }
}
