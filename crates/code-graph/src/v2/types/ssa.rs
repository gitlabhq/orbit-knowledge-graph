/// A value reachable at a reference point, computed by parser-level SSA.
///
/// Uses indices into the file's own def/import arrays — the parser
/// doesn't see the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParseValue {
    /// Points to a definition in this file's defs list.
    LocalDef(u32),
    /// Points to an import in this file's imports list.
    ImportRef(u32),
    /// A type FQN for nested member lookup (from self/this or type annotations).
    Type(String),
    /// Dead end — parameter, literal, or otherwise unresolvable.
    Opaque,
}
