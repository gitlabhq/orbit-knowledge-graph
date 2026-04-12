use crate::Range;
use smallvec::SmallVec;
use std::sync::Arc;

/// Canonical definition categories. The linker uses this for
/// relationship determination without per-language dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DefKind {
    Class,
    Interface,
    Module,
    Function,
    Method,
    Constructor,
    Lambda,
    Property,
    EnumEntry,
    Other,
}

/// Language-agnostic definition.
#[derive(Debug, Clone)]
pub struct CanonicalDefinition {
    pub definition_type: String,
    pub kind: DefKind,
    pub name: String,
    pub fqn: CanonicalFqn,
    pub range: Range,
}

/// Language-agnostic FQN.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalFqn {
    pub parts: Arc<SmallVec<[FqnPart; 8]>>,
    pub separator: &'static str,
}

impl CanonicalFqn {
    pub fn new(parts: SmallVec<[FqnPart; 8]>, separator: &'static str) -> Self {
        Self {
            parts: Arc::new(parts),
            separator,
        }
    }

    pub fn name(&self) -> &str {
        self.parts.last().map(|p| p.name.as_str()).unwrap_or("")
    }

    pub fn parent(&self) -> Option<Self> {
        if self.parts.len() <= 1 {
            return None;
        }
        let parent_parts: SmallVec<[FqnPart; 8]> = self.parts[..self.parts.len() - 1].into();
        Some(Self::new(parent_parts, self.separator))
    }

    pub fn len(&self) -> usize {
        self.parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

impl std::fmt::Display for CanonicalFqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let joined: String = self
            .parts
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>()
            .join(self.separator);
        write!(f, "{joined}")
    }
}

/// A single part of an FQN.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FqnPart {
    pub part_type: &'static str,
    pub name: String,
    pub range: Range,
}

/// Language-agnostic import.
#[derive(Debug, Clone)]
pub struct CanonicalImport {
    pub import_type: &'static str,
    pub path: String,
    pub name: Option<String>,
    pub alias: Option<String>,
    pub fqn: Option<CanonicalFqn>,
    pub range: Range,
}

/// Language-agnostic reference (call site).
#[derive(Debug, Clone)]
pub struct CanonicalReference {
    pub reference_type: &'static str,
    pub name: String,
    pub range: Range,
    pub scope_fqn: Option<CanonicalFqn>,
}

/// Language-agnostic output of parsing a file.
#[derive(Debug, Clone)]
pub struct CanonicalFileResult {
    pub file_path: String,
    pub extension: String,
    pub file_size: u64,
    pub language: crate::Language,
    pub definitions: Vec<CanonicalDefinition>,
    pub imports: Vec<CanonicalImport>,
    pub references: Vec<CanonicalReference>,
}

/// Trait for converting language-specific definition types to canonical form.
pub trait ToCanonical {
    fn to_def_kind(&self) -> DefKind;
}
