use smallvec::SmallVec;

use crate::{ExpressionStep, Range};

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

/// A reference with SSA-computed reaching definitions.
///
/// Produced by `parse_full()`. The resolver maps `ParseValue` entries
/// to graph `NodeIndex` targets and emits edges.
#[derive(Debug, Clone)]
pub struct ReferenceEvent {
    /// The referenced name (terminal segment of the expression).
    pub name: String,
    /// Linearized expression chain (None for bare references).
    pub chain: Option<Vec<ExpressionStep>>,
    /// Reaching definitions from SSA, after alias resolution.
    pub reaching: SmallVec<[ParseValue; 2]>,
    /// Index into the file's defs for the enclosing definition scope.
    pub enclosing_def: Option<u32>,
    /// Position in the source file.
    pub range: Range,
}

/// Result of a single-pass parse, optionally with SSA.
///
/// All three vecs are independently populated — callers that only need
/// defs leave `references` empty, callers that only need refs leave
/// `definitions` empty, etc.
pub struct FileResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: code_graph_config::Language,
    pub definitions: Vec<crate::CanonicalDefinition>,
    pub imports: Vec<crate::CanonicalImport>,
    pub references: Vec<ReferenceEvent>,
}
