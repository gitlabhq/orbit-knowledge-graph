use code_graph_types::Range;
use rustc_hash::FxHashMap;
use std::fmt;

/// An element in an expression chain (e.g. `foo.bar()` → [Identifier("foo"), Attribute, Identifier("bar"), Call]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Symbol {
    Identifier(String),
    Connector(Connector),
    Receiver, // `self` / `cls`
}

/// How symbols are connected in an expression chain.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Connector {
    Attribute, // `.`
    Call,      // `()`
    Index,     // `[]`
}

/// A chain of symbols representing an expression (e.g. `obj.method().attr`).
///
/// Used as both a key for symbol table lookups and a value for alias bindings.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct SymbolChain {
    pub symbols: Vec<Symbol>,
}

impl SymbolChain {
    pub fn new(symbols: Vec<Symbol>) -> Self {
        Self { symbols }
    }

    pub fn single(name: impl Into<String>) -> Self {
        Self {
            symbols: vec![Symbol::Identifier(name.into())],
        }
    }

    pub fn is_identifier(&self) -> bool {
        self.symbols.len() == 1 && matches!(self.symbols[0], Symbol::Identifier(_))
    }

    pub fn is_single(&self) -> bool {
        self.symbols.len() == 1
    }

    pub fn first_identifier(&self) -> Option<&str> {
        match self.symbols.first()? {
            Symbol::Identifier(id) => Some(id),
            _ => None,
        }
    }

    pub fn starts_with_receiver(&self) -> bool {
        matches!(self.symbols.first(), Some(Symbol::Receiver))
    }
}

impl fmt::Display for SymbolChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for sym in &self.symbols {
            match sym {
                Symbol::Identifier(id) => write!(f, "{id}")?,
                Symbol::Connector(con) => match con {
                    Connector::Attribute => write!(f, ".")?,
                    Connector::Call => write!(f, "()")?,
                    Connector::Index => write!(f, "[]")?,
                },
                Symbol::Receiver => write!(f, ":self:")?,
            }
        }
        Ok(())
    }
}

// ── Binding types ───────────────────────────────────────────────

/// Index type for referring to symbol tables in the arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId(pub usize);

/// What a name is bound to in a scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BindingValue {
    /// Bound to a definition (carries the index into the file's definitions vec).
    Definition(DefinitionBinding),
    /// Bound to an import (carries the index into the file's imports vec).
    Import(ImportBinding),
    /// Alias to another expression chain (e.g. `x = y` → SymbolChain("y")).
    Alias(SymbolChain),
    /// Unresolvable value (parameter, literal, deletion, etc.).
    DeadEnd,
}

/// Reference to a definition within a file's CanonicalResult.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefinitionBinding {
    pub def_idx: usize,
}

/// Reference to an import within a file's CanonicalResult.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImportBinding {
    pub import_idx: usize,
}

/// A name-to-value mapping at a particular source location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Binding {
    pub value: BindingValue,
    pub location: Range,
}

impl Binding {
    pub fn new(value: BindingValue, location: Range) -> Self {
        Self { value, location }
    }

    pub fn dead_end(location: Range) -> Self {
        Self {
            value: BindingValue::DeadEnd,
            location,
        }
    }

    pub fn definition(def_idx: usize, location: Range) -> Self {
        Self {
            value: BindingValue::Definition(DefinitionBinding { def_idx }),
            location,
        }
    }

    pub fn import(import_idx: usize, location: Range) -> Self {
        Self {
            value: BindingValue::Import(ImportBinding { import_idx }),
            location,
        }
    }

    pub fn alias(chain: SymbolChain, location: Range) -> Self {
        Self {
            value: BindingValue::Alias(chain),
            location,
        }
    }
}

// ── Scope types ─────────────────────────────────────────────────

/// Classification of Python scope-creating constructs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeType {
    // Isolated: names defined here are not accessible from the parent
    Module,
    Function,
    Class,
    Lambda,

    // Semi-isolated: some names leak (comprehension iteration variable in Python 3)
    Comprehension,

    // Non-isolated: names defined here ARE accessible from the parent
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

impl ScopeType {
    /// Whether this scope type isolates its bindings from the parent.
    pub fn is_isolated(&self) -> bool {
        matches!(
            self,
            ScopeType::Module | ScopeType::Function | ScopeType::Class | ScopeType::Lambda
        )
    }

    pub fn is_class(&self) -> bool {
        matches!(self, ScopeType::Class)
    }

    /// Whether this scope has a "catch-all" branch (else, default case, bare except).
    pub fn is_catch_all(&self) -> bool {
        matches!(
            self,
            ScopeType::Else | ScopeType::DefaultCase | ScopeType::Except
        )
    }
}

/// Types of conditional scope groups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeGroupType {
    If,
    Try,
    Match,
    Loop,
    Comprehension,
}

/// A group of connected conditional scopes (e.g. if/elif/else chain).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeGroup {
    pub location: Range,
    pub scope_ids: Vec<ScopeId>,
    pub group_type: ScopeGroupType,
}

impl ScopeGroup {
    pub fn new(location: Range, scope_ids: Vec<ScopeId>, group_type: ScopeGroupType) -> Self {
        Self {
            location,
            scope_ids,
            group_type,
        }
    }

    /// Whether this group has a catch-all branch (else, bare except, default case).
    pub fn has_catch_all(&self, tree: &SymbolTableTree) -> bool {
        self.scope_ids.iter().any(|id| {
            tree.get(*id)
                .is_some_and(|scope| scope.scope_type.is_catch_all())
        })
    }
}

// ── Symbol table ────────────────────────────────────────────────

/// A scope node in the scope tree. Contains bindings, child scopes,
/// conditional groups, and recorded call-site references.
#[derive(Debug, Clone)]
pub struct SymbolTable {
    pub symbols: FxHashMap<SymbolChain, Vec<Binding>>,
    pub parent: Option<ScopeId>,
    pub children: Vec<ScopeId>,
    pub conditionals: Vec<ScopeGroup>,
    pub location: Range,
    pub scope_type: ScopeType,
    /// The FQN string of the scope-defining construct (class or function name).
    pub fqn: Option<String>,
    /// Call-site references to resolve: (expression chain, source range).
    pub references: Vec<(SymbolChain, Range)>,
}

impl SymbolTable {
    pub fn add_binding(&mut self, key: SymbolChain, binding: Binding) {
        self.symbols.entry(key).or_default().push(binding);
    }

    pub fn add_reference(&mut self, chain: SymbolChain, range: Range) {
        self.references.push((chain, range));
    }

    pub fn add_scope_group(&mut self, group: ScopeGroup) {
        self.conditionals.push(group);
    }
}

// ── Symbol table tree (arena) ───────────────────────────────────

/// Arena-allocated tree of all scopes in a single Python file.
///
/// The definition_table maps scope IDs to definition indices,
/// allowing reverse lookup from a scope to its defining definition
/// (e.g. the function scope → function definition).
#[derive(Debug, Clone)]
pub struct SymbolTableTree {
    tables: Vec<SymbolTable>,
    root: ScopeId,
    /// Maps scope IDs to the definition index that defines that scope.
    definition_table: FxHashMap<ScopeId, usize>,
    /// Reverse map: definition index → scope ID.
    definition_to_scope: FxHashMap<usize, ScopeId>,
}

impl SymbolTableTree {
    pub fn new(location: Range, scope_type: ScopeType, fqn: Option<String>) -> Self {
        let root = SymbolTable {
            symbols: FxHashMap::default(),
            parent: None,
            children: Vec::new(),
            conditionals: Vec::new(),
            location,
            scope_type,
            fqn,
            references: Vec::new(),
        };
        Self {
            tables: vec![root],
            root: ScopeId(0),
            definition_table: FxHashMap::default(),
            definition_to_scope: FxHashMap::default(),
        }
    }

    pub fn add_child(
        &mut self,
        parent_id: ScopeId,
        location: Range,
        scope_type: ScopeType,
        fqn: Option<String>,
    ) -> ScopeId {
        let child_id = ScopeId(self.tables.len());

        let child = SymbolTable {
            symbols: FxHashMap::default(),
            parent: Some(parent_id),
            children: Vec::new(),
            conditionals: Vec::new(),
            location,
            scope_type,
            fqn,
            references: Vec::new(),
        };

        self.tables.push(child);

        if let Some(parent) = self.tables.get_mut(parent_id.0) {
            parent.children.push(child_id);
        }

        child_id
    }

    pub fn get(&self, id: ScopeId) -> Option<&SymbolTable> {
        self.tables.get(id.0)
    }

    pub fn get_mut(&mut self, id: ScopeId) -> Option<&mut SymbolTable> {
        self.tables.get_mut(id.0)
    }

    pub fn add_binding(&mut self, scope_id: ScopeId, key: SymbolChain, binding: Binding) {
        if let Some(table) = self.tables.get_mut(scope_id.0) {
            table.add_binding(key, binding);
        }
    }

    pub fn add_reference(&mut self, scope_id: ScopeId, chain: SymbolChain, range: Range) {
        if let Some(table) = self.tables.get_mut(scope_id.0) {
            table.add_reference(chain, range);
        }
    }

    pub fn add_conditional(&mut self, scope_id: ScopeId, group: ScopeGroup) {
        if let Some(table) = self.tables.get_mut(scope_id.0) {
            table.add_scope_group(group);
        }
    }

    /// Associate a scope with the definition that created it.
    pub fn add_definition(&mut self, scope_id: ScopeId, def_idx: usize) {
        self.definition_table.insert(scope_id, def_idx);
        self.definition_to_scope.insert(def_idx, scope_id);
    }

    /// Get the definition index for a scope (e.g. function scope → function def index).
    pub fn get_definition(&self, scope_id: ScopeId) -> Option<usize> {
        self.definition_table.get(&scope_id).copied()
    }

    /// Get the scope for a definition index (reverse lookup).
    pub fn get_definition_scope(&self, def_idx: usize) -> Option<ScopeId> {
        self.definition_to_scope.get(&def_idx).copied()
    }

    pub fn root(&self) -> ScopeId {
        self.root
    }

    pub fn iter(&self) -> impl Iterator<Item = (ScopeId, &SymbolTable)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(i, table)| (ScopeId(i), table))
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }
}

// ── Resolution result types ─────────────────────────────────────

/// The result of resolving a single reference.
#[derive(Debug, Clone)]
pub enum ResolvedTarget {
    /// Resolved to a definition (def_idx in the file's CanonicalResult).
    Definition(usize),
    /// Resolved to an import (import_idx in the file's CanonicalResult).
    Import(usize),
    /// Partially resolved: the chain was resolved up to `resolved_index`,
    /// and the resolved prefix resolved to `target`.
    Partial(PartialResolution),
}

/// A partially resolved symbol chain.
#[derive(Debug, Clone)]
pub struct PartialResolution {
    pub chain: SymbolChain,
    /// Index into chain.symbols where the resolved portion ends.
    pub resolved_index: usize,
    pub target: Box<ResolvedTarget>,
}

/// A reference with its resolution result.
#[derive(Debug, Clone)]
pub struct ResolvedReference {
    pub chain: SymbolChain,
    pub range: Range,
    pub targets: Vec<ResolvedTarget>,
}

/// RHS of an assignment for the visitor.
#[derive(Debug, Clone)]
pub enum ParsedExpression {
    SymbolChain(SymbolChain),
    Lambda,
    Ignored,
}

/// Method type classification based on decorators.
#[derive(Debug, Clone, PartialEq)]
pub enum MethodType {
    Instance,
    Class,
    Static,
    Property,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_chain_display() {
        let chain = SymbolChain::new(vec![
            Symbol::Identifier("foo".into()),
            Symbol::Connector(Connector::Attribute),
            Symbol::Identifier("bar".into()),
            Symbol::Connector(Connector::Call),
        ]);
        assert_eq!(chain.to_string(), "foo.bar()");
    }

    #[test]
    fn symbol_chain_single() {
        let chain = SymbolChain::single("x");
        assert!(chain.is_identifier());
        assert!(chain.is_single());
        assert_eq!(chain.first_identifier(), Some("x"));
    }

    #[test]
    fn scope_type_isolation() {
        assert!(ScopeType::Module.is_isolated());
        assert!(ScopeType::Function.is_isolated());
        assert!(ScopeType::Class.is_isolated());
        assert!(ScopeType::Lambda.is_isolated());
        assert!(!ScopeType::If.is_isolated());
        assert!(!ScopeType::For.is_isolated());
    }

    #[test]
    fn symbol_table_tree_basics() {
        let mut tree = SymbolTableTree::new(Range::empty(), ScopeType::Module, None);
        assert_eq!(tree.len(), 1);

        let child = tree.add_child(
            tree.root(),
            Range::empty(),
            ScopeType::Function,
            Some("foo".into()),
        );
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.get(child).unwrap().parent, Some(tree.root()));
        assert!(tree.get(tree.root()).unwrap().children.contains(&child));
    }

    #[test]
    fn definition_table_bidirectional() {
        let mut tree = SymbolTableTree::new(Range::empty(), ScopeType::Module, None);
        let scope = tree.add_child(
            tree.root(),
            Range::empty(),
            ScopeType::Function,
            Some("func".into()),
        );
        tree.add_definition(scope, 42);

        assert_eq!(tree.get_definition(scope), Some(42));
        assert_eq!(tree.get_definition_scope(42), Some(scope));
    }

    #[test]
    fn binding_constructors() {
        let loc = Range::empty();
        let b = Binding::definition(0, loc);
        assert!(matches!(
            b.value,
            BindingValue::Definition(DefinitionBinding { def_idx: 0 })
        ));

        let b = Binding::import(3, loc);
        assert!(matches!(
            b.value,
            BindingValue::Import(ImportBinding { import_idx: 3 })
        ));

        let b = Binding::alias(SymbolChain::single("y"), loc);
        assert!(matches!(b.value, BindingValue::Alias(_)));

        let b = Binding::dead_end(loc);
        assert!(matches!(b.value, BindingValue::DeadEnd));
    }
}
