use rustc_hash::FxHashMap as HashMap;
use std::hash::{Hash, Hasher};

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::definitions::{DefinitionInfo, DefinitionTypeInfo};
use crate::fqn::{FQNPart, Fqn};
use crate::imports::{ImportTypeInfo, ImportedSymbolInfo};
use crate::references::{ReferenceInfo, TargetResolution};
use crate::utils::Range;

/// Python-specific FQN part
pub type PythonFqnPart = FQNPart<PythonDefinitionType>;

/// Python-specific FQN
pub type PythonFqn = Fqn<PythonFqnPart>;

/// Maps node ranges to their corresponding AST nodes and FQN parts
pub type PythonNodeFqnMap<'a> = HashMap<Range, (Node<'a, StrDoc<SupportLang>>, PythonFqn)>;

// Definition types

/// Represents a Python definition found in the code
pub type PythonDefinitionInfo = DefinitionInfo<PythonDefinitionType, PythonFqn>;

/// Types of definitions that can be found in Python code
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum PythonDefinitionType {
    Class,
    DecoratedClass,
    Method,
    AsyncMethod,
    DecoratedMethod,
    DecoratedAsyncMethod,
    Function,
    AsyncFunction,
    DecoratedFunction,
    DecoratedAsyncFunction,
    Lambda,
}

impl DefinitionTypeInfo for PythonDefinitionType {
    /// Convert PythonDefinitionType to its string representation
    fn as_str(&self) -> &str {
        match self {
            PythonDefinitionType::Class => "Class",
            PythonDefinitionType::DecoratedClass => "DecoratedClass",
            PythonDefinitionType::Function => "Function",
            PythonDefinitionType::AsyncFunction => "AsyncFunction",
            PythonDefinitionType::DecoratedFunction => "DecoratedFunction",
            PythonDefinitionType::DecoratedAsyncFunction => "DecoratedAsyncFunction",
            PythonDefinitionType::Method => "Method",
            PythonDefinitionType::AsyncMethod => "AsyncMethod",
            PythonDefinitionType::DecoratedMethod => "DecoratedMethod",
            PythonDefinitionType::DecoratedAsyncMethod => "DecoratedAsyncMethod",
            PythonDefinitionType::Lambda => "Lambda",
        }
    }
}

// Import types

/// Represents a Python definition found in the code
pub type PythonImportedSymbolInfo = ImportedSymbolInfo<PythonImportType, PythonFqn>;

/// Types of imported symbols that can be found in Python code
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum PythonImportType {
    Import,                 // import module
    AliasedImport,          // import module as alias
    FromImport,             // from module import symbol
    AliasedFromImport,      // from module import symbol as alias
    WildcardImport,         // from module import *
    RelativeWildcardImport, // from . import *
    RelativeImport,         // from . import symbol
    AliasedRelativeImport,  // from . import symbol as alias
    FutureImport,           // from __future__ import symbol
    AliasedFutureImport,    // from __future__ import symbol as alias
}

impl ImportTypeInfo for PythonImportType {
    fn as_str(&self) -> &str {
        match self {
            PythonImportType::Import => "Import",
            PythonImportType::AliasedImport => "AliasedImport",
            PythonImportType::FromImport => "FromImport",
            PythonImportType::AliasedFromImport => "AliasedFromImport",
            PythonImportType::WildcardImport => "WildcardImport",
            PythonImportType::RelativeWildcardImport => "RelativeWildcardImport",
            PythonImportType::RelativeImport => "RelativeImport",
            PythonImportType::AliasedRelativeImport => "AliasedRelativeImport",
            PythonImportType::FutureImport => "FutureImport",
            PythonImportType::AliasedFutureImport => "AliasedFutureImport",
        }
    }
}

// References

/// Types of references (for now, we only care about function calls)
#[derive(Debug, Clone, PartialEq)]
pub enum PythonReferenceType {
    Call, // obj()
}

/// Symbol used in an expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Symbol {
    Identifier(String),   // x
    Connector(Connector), // x.y, x[], x()
    Receiver(),           // 'self'
}

/// How a symbol is connected to the next in an expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Connector {
    Attribute, // x.y
    Call,      // x()
    Index,     // x[0]
}

/// A chain of connected symbols (e.g. an expression)
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct SymbolChain {
    pub symbols: Vec<Symbol>,
}

impl SymbolChain {
    pub fn new(symbols: Vec<Symbol>) -> Self {
        Self { symbols }
    }

    pub fn is_identifier(&self) -> bool {
        self.symbols.len() == 1 && matches!(self.symbols[0], Symbol::Identifier(_))
    }

    pub fn is_single(&self) -> bool {
        self.symbols.len() == 1
    }

    pub fn as_str(&self) -> String {
        self.symbols
            .iter()
            .map(|s| match s {
                Symbol::Identifier(id) => id.clone(),
                Symbol::Connector(con) => match con {
                    Connector::Attribute => ".".to_string(),
                    Connector::Call => "()".to_string(),
                    Connector::Index => "[]".to_string(),
                },
                Symbol::Receiver() => ":self:".to_string(), // Wrapped in `:` to make it unique
            })
            .collect::<Vec<String>>()
            .join("")
    }
}

/// A partially resolved symbol chain (i.e. only a sub-chain is resolved)
#[derive(Debug, Clone)]
pub struct PartialResolution {
    pub symbol_chain: SymbolChain,
    pub index: usize, // Index of the tail of the resolved sub-chain
    pub target: Box<PythonTargetResolution>,
}

/// A resolved (or partially resolved) reference target
pub type PythonTargetResolution =
    TargetResolution<PythonDefinitionInfo, PythonImportedSymbolInfo, PartialResolution>;

/// Python-specific reference info
pub type PythonReferenceInfo =
    ReferenceInfo<PythonTargetResolution, PythonReferenceType, (), PythonFqn>;

// Resolution

/// Types of methods in a class
#[derive(Debug, Clone, PartialEq)]
pub enum MethodType {
    Instance, // No decorator
    Class,    // @classmethod
    Static,   // @staticmethod
    Property, // @property
}

/// Scope-creating constructs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeType {
    // Isolated (i.e. names defined in the scope aren't accessible by the parent)
    Module,
    Function,
    Class,
    Lambda,

    // Semi-isolated (i.e. some names leak into the parent scope, some don't)
    Comprehension, // Includes generators

    // Non-isolated (i.e. names defined in the scope are accessible by the parent)
    If,
    Elif,
    Else,
    Try,
    Except,
    For,
    While,
    Case,
    DefaultCase,
}

/// Types of conditional scope groups
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeGroupType {
    If,
    Try,
    Match,
    Loop,
    Comprehension,
}

/// Index type for referring to symbol tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SymbolTableId(pub usize);

/// Container for all the symbol tables in the scope hierarchy
#[derive(Debug, Clone)]
pub struct SymbolTableTree {
    tables: Vec<SymbolTable>,
    root: SymbolTableId,
    definition_table: HashMap<SymbolTableId, PythonDefinitionInfo>, // Look up definition by scope ID
                                                                    // TODO: Add lambdas to this table (do this when we drop ast-grep)
}

impl SymbolTableTree {
    pub fn new(location: Range, scope_type: ScopeType, fqn: Option<PythonFqn>) -> Self {
        let root_node = SymbolTable {
            symbols: HashMap::default(),
            parent: None,
            children: Vec::new(),
            conditionals: Vec::new(),
            location,
            scope_type,
            fqn,
            references: Vec::new(),
        };
        Self {
            tables: vec![root_node],
            root: SymbolTableId(0),
            definition_table: HashMap::default(),
        }
    }

    /// Add a new symbol table as a child of the given parent
    pub fn add_child(
        &mut self,
        parent_id: SymbolTableId,
        location: Range,
        scope_type: ScopeType,
        fqn: Option<PythonFqn>,
    ) -> SymbolTableId {
        let child_id = SymbolTableId(self.tables.len());

        let child_node = SymbolTable {
            symbols: HashMap::default(),
            parent: Some(parent_id),
            children: Vec::new(),
            conditionals: Vec::new(),
            location,
            scope_type,
            fqn,
            references: Vec::new(),
        };

        self.tables.push(child_node);

        // Add child to parent's children list
        if let Some(parent) = self.tables.get_mut(parent_id.0) {
            parent.children.push(child_id);
        }

        child_id
    }

    /// Get a reference to a symbol table node
    pub fn get(&self, id: SymbolTableId) -> Option<&SymbolTable> {
        self.tables.get(id.0)
    }

    /// Get a mutable reference to a symbol table node
    pub fn get_mut(&mut self, id: SymbolTableId) -> Option<&mut SymbolTable> {
        self.tables.get_mut(id.0)
    }

    /// Add a binding to a specific symbol table
    pub fn add_binding(&mut self, table_id: SymbolTableId, key: SymbolChain, binding: Binding) {
        if let Some(table) = self.tables.get_mut(table_id.0) {
            table.symbols.entry(key).or_default().push(binding);
        }
    }

    pub fn add_reference(
        &mut self,
        table_id: SymbolTableId,
        symbol_chain: SymbolChain,
        range: Range,
    ) {
        if let Some(table) = self.tables.get_mut(table_id.0) {
            table.add_reference(symbol_chain, range);
        }
    }

    pub fn add_conditional(&mut self, table_id: SymbolTableId, scope_group: ScopeGroup) {
        if let Some(table) = self.tables.get_mut(table_id.0) {
            table.add_scope_group(scope_group);
        }
    }

    pub fn add_definition(&mut self, table_id: SymbolTableId, definition: PythonDefinitionInfo) {
        self.definition_table.insert(table_id, definition);
    }

    pub fn get_definition_scope(&self, definition: &PythonDefinitionInfo) -> Option<SymbolTableId> {
        self.definition_table
            .iter()
            .find(|(_, def)| *def == definition)
            .map(|(scope_id, _)| *scope_id)
    }

    /// Get the root table ID
    pub fn root(&self) -> SymbolTableId {
        self.root
    }

    /// Iterate over all tables in the tree
    pub fn iter(&self) -> impl Iterator<Item = (SymbolTableId, &SymbolTable)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(i, node)| (SymbolTableId(i), node))
    }
}

/// Keeps track of the namespace in a scope
#[derive(Debug, Clone)]
pub struct SymbolTable {
    pub symbols: HashMap<SymbolChain, Vec<Binding>>,
    pub parent: Option<SymbolTableId>,
    pub children: Vec<SymbolTableId>,
    pub conditionals: Vec<ScopeGroup>,
    pub location: Range,
    pub scope_type: ScopeType,
    pub fqn: Option<PythonFqn>,
    pub references: Vec<(SymbolChain, Range)>,
}

impl PartialEq for SymbolTable {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location && self.scope_type == other.scope_type
    }
}

impl Eq for SymbolTable {}

impl Hash for SymbolTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.location.hash(state);
        self.scope_type.hash(state);
    }
}

impl SymbolTable {
    pub fn new(
        symbols: HashMap<SymbolChain, Vec<Binding>>,
        parent: Option<SymbolTableId>,
        children: Vec<SymbolTableId>,
        conditionals: Vec<ScopeGroup>,
        location: Range,
        scope_type: ScopeType,
        fqn: Option<PythonFqn>,
    ) -> Self {
        Self {
            symbols,
            parent,
            children,
            conditionals,
            location,
            scope_type,
            fqn,
            references: Vec::new(),
        }
    }

    pub fn add_binding(&mut self, key: SymbolChain, value: Binding) {
        self.symbols.entry(key).or_default().push(value);
    }

    pub fn add_reference(&mut self, symbol_chain: SymbolChain, range: Range) {
        self.references.push((symbol_chain, range));
    }

    pub fn add_scope_group(&mut self, scope_group: ScopeGroup) {
        self.conditionals.push(scope_group);
    }
}

/// Represents a binding of an expression to a target in the namespace
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Binding {
    /// Value that an expression is being bound to
    pub value: BindingValue,
    /// Location of the binding (i.e. where the binding takes effect)
    pub location: Range, // TODO: Change to a Position
}

impl Binding {
    pub fn new(value: BindingValue, range: Range) -> Self {
        Self {
            value,
            location: range,
        }
    }

    /// An expression we don't care about (e.g. a literal, like `x = "Hello world") `),
    /// or a deletion (`del x`)
    pub fn dead_end(range: Range) -> Self {
        Self {
            value: BindingValue::DeadEnd(),
            location: range,
        }
    }
}

/// Represents a binding value (e.g. y in `x = y`)
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum BindingValue {
    Definition(PythonDefinitionInfo),
    ImportedSymbol(PythonImportedSymbolInfo),
    SymbolChain(SymbolChain),
    DeadEnd(),
}

/// Represents a group of connected conditional scopes (e.g. if, elif, and else)
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ScopeGroup {
    pub location: Range,
    pub scope_ids: Vec<SymbolTableId>,
    pub group_type: ScopeGroupType,
}

impl ScopeGroup {
    pub fn new(location: Range, scope_ids: Vec<SymbolTableId>, group_type: ScopeGroupType) -> Self {
        Self {
            location,
            scope_ids,
            group_type,
        }
    }
}

/// Represents an assignment value (i.e. RHS of `var = ...`)
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedExpression {
    SymbolChain(SymbolChain),
    Lambda(),
    Ignored(),
} // TODO: This is unnecessary and lambdas should be parsed separately
