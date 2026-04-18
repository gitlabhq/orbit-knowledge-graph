use crate::legacy::parser::imports::ImportIdentifier;
use crate::legacy::parser::references::{ReferenceInfo, ReferenceTarget, TargetResolution};
use crate::legacy::parser::typescript::types::{
    TypeScriptDefinitionInfo, TypeScriptFqn, TypeScriptImportedSymbolInfo,
};
use crate::utils::HasRange;
use crate::utils::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeScriptReferenceType {
    FunctionCall,
    MethodCall,
    PropertyAccess,
    ConstructorCall,
    VariableReference,
}

// New comprehensive reference system based on generic structures

/// Types of symbols in TypeScript expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeScriptSymbolType {
    /// Simple identifier: `variable`
    Identifier,
    /// Property access: `obj.property`
    Property,
    /// Method call source: `obj for obj.method(args)`
    MethodCallSource,
    /// Method call: `obj.method(arg)`
    MethodCall,
    /// Array/object index: `obj[key]`
    Index,
    /// Function call: `func()`
    Call,
    /// Constructor call: `new Class()`
    ConstructorCall,
    /// Type reference: `MyType`
    Type,
    /// Namespace reference: `Namespace.member`
    Namespace,
    /// Argument: `func(arg)`
    Argument,
    /// Assignment target: `variable` in `variable = value`
    AssignmentTarget,
    /// Definition: `class MyClass`
    Definition,
    /// Import: `import { MyClass } from 'my-module'`
    Import,
    /// Reference: `MyClass`
    Reference,
    /// Arrow function callback: `(param) => {...}`
    ArrowFunctionCallback,
    /// Function expression callback: `function(param) {...}`
    FunctionExpressionCallback,
    /// Anonymous function callback: function without name
    AnonymousFunctionCallback,
    /// Unknown: `Unknown`
    Unknown,
}

/// Metadata for different types of references
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeScriptReferenceMetadata {
    /// Function or method call with argument information
    Call {
        /// Whether this is an async call (await expression)
        is_async: bool,

        // is super call
        is_super: bool,

        // is this call
        is_this: bool,

        /// Arguments passed to the call
        args: Vec<TypeScriptAnnotatedSymbol>,
    },
    /// Property access metadata
    Property {
        /// Whether this is optional chaining (?.)
        is_optional: bool,
        /// Whether this is computed access ([])
        is_computed: bool,
    },
    /// Index metadata
    Index {
        /// Whether this is computed access ([])
        is_computed: bool,
    },
    /// Constructor call metadata
    Constructor {
        /// Arguments passed to the constructor
        args: Vec<TypeScriptAnnotatedSymbol>,
    },
    /// Type reference metadata
    Type {
        /// Whether this is a type-only reference
        is_type_only: bool,
    },
    /// Assignment metadata
    Assignment {
        /// The assignment operator (=, +=, -=, etc.)
        operator: String,
        /// Whether this assignment is destructured
        is_destructured: bool,

        // Alias for the assignment target
        aliased_from: Option<String>,
    },
    /// Callback function metadata
    Callback {
        /// Parameter names in the callback function
        parameters: Vec<String>,
        /// Whether this callback is async
        is_async: bool,
        /// The function body text (if available)
        body: Option<String>,
    },
    /// No additional metadata
    None,
}

/// An annotated symbol representing part of a TypeScript expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeScriptAnnotatedSymbol {
    /// The symbol text (e.g., "method" in "obj.method()")
    pub symbol: String,
    /// The range of this symbol in the source code
    pub range: Range,
    /// What type of symbol this is
    pub symbol_type: TypeScriptSymbolType,
    /// The resolved target of this symbol (if any)
    pub target: Option<TypeScriptReferenceTarget>,
    /// Additional metadata about this symbol
    pub metadata: Option<TypeScriptReferenceMetadata>,
}

impl TypeScriptAnnotatedSymbol {
    pub fn new(
        symbol: String,
        range: Range,
        symbol_type: TypeScriptSymbolType,
        target: Option<TypeScriptReferenceTarget>,
        metadata: Option<TypeScriptReferenceMetadata>,
    ) -> Self {
        Self {
            symbol,
            range,
            symbol_type,
            target,
            metadata,
        }
    }
}

/// A TypeScript expression as a chain of connected symbols
/// This represents partial resolution - an expression that may need further resolution
/// in the indexer to determine the final target.
///
/// Examples:
/// - `obj.method()` -> [identifier("obj"), method("method")]
/// - `this.service.process()` -> [identifier("this"), property("service"), method("process")]
/// - `new MyClass().getInstance()` -> [constructor("MyClass"), method("getInstance")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeScriptExpression {
    pub range: Range,
    pub string: String,
    pub symbols: Vec<ExpressionSymbolInfo>,
    pub assigment_target_symbols: Vec<ExpressionSymbolInfo>,
}

impl TypeScriptExpression {
    pub fn new() -> Self {
        Self {
            range: Range::empty(),
            string: String::new(),
            symbols: vec![],
            assigment_target_symbols: vec![],
        }
    }

    pub fn valid(&self) -> bool {
        self.range.byte_offset.0 != 0 && self.range.byte_offset.1 != 0
    }
}

impl Default for TypeScriptExpression {
    fn default() -> Self {
        Self::new()
    }
}

impl HasRange for TypeScriptExpression {
    fn range(&self) -> Range {
        self.range
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct ExpressionModifiers {
    pub is_await: bool,
    pub is_super: bool,
    pub is_this: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExpressionSymbolInfo {
    pub range: Range,
    pub name: String,
    pub symbol_type: TypeScriptSymbolType,
    pub metadata: Option<TypeScriptReferenceMetadata>,
}

// Update the existing type aliases to use the new generic structures
pub type TypeScriptTargetResolution =
    TargetResolution<TypeScriptDefinitionInfo, TypeScriptImportedSymbolInfo, TypeScriptExpression>;
pub type TypeScriptReferenceTarget = ReferenceTarget<TypeScriptTargetResolution>;
pub type TypeScriptReferenceInfo = ReferenceInfo<
    TypeScriptTargetResolution,
    TypeScriptReferenceType,
    TypeScriptReferenceMetadata,
    TypeScriptFqn,
>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeScriptSymbol {
    Definition(TypeScriptDefinitionInfo),
    Import(TypeScriptImportedSymbolInfo),
    Reference(TypeScriptReferenceInfo),
    Unknown,
}

impl HasRange for TypeScriptSymbol {
    fn range(&self) -> Range {
        self.range()
    }
}

impl TypeScriptSymbol {
    pub fn name(&self) -> Option<String> {
        match self {
            TypeScriptSymbol::Definition(def) => Some(def.name.clone()),
            TypeScriptSymbol::Import(imp) => {
                if let Some(ImportIdentifier { name, alias }) = &imp.identifier {
                    match alias {
                        Some(alias) => Some(alias.clone()),
                        None => Some(name.clone()),
                    }
                } else {
                    None
                }
            }
            TypeScriptSymbol::Reference(r) => Some(r.name.clone()),
            TypeScriptSymbol::Unknown => None,
        }
    }

    pub fn symbol_type(&self) -> TypeScriptSymbolType {
        match self {
            TypeScriptSymbol::Definition(_) => TypeScriptSymbolType::Definition,
            TypeScriptSymbol::Import(_) => TypeScriptSymbolType::Import,
            TypeScriptSymbol::Reference(_) => TypeScriptSymbolType::Reference,
            TypeScriptSymbol::Unknown => TypeScriptSymbolType::Unknown,
        }
    }

    pub fn range(&self) -> Range {
        match self {
            TypeScriptSymbol::Definition(def) => def.range,
            TypeScriptSymbol::Import(imp) => imp.range,
            TypeScriptSymbol::Reference(r) => r.range,
            TypeScriptSymbol::Unknown => Range::empty(),
        }
    }

    pub fn definition(&self) -> Option<&TypeScriptDefinitionInfo> {
        match self {
            TypeScriptSymbol::Definition(def) => Some(def),
            _ => None,
        }
    }

    pub fn import(&self) -> Option<&TypeScriptImportedSymbolInfo> {
        match self {
            TypeScriptSymbol::Import(imp) => Some(imp),
            _ => None,
        }
    }
}
