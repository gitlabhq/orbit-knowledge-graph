//! The graph a run builds: one tree per file, the edges between them, and
//! the resolver's memory of how it linked them.

use crate::resolver::Resolver;
use crate::tree::{Edge, Tree};

pub struct SourceFile {
    pub path: String,
    pub content: String,
}

pub struct State {
    pub trees: Vec<Tree>,
    pub edges: Vec<Edge>,
    pub resolver: Resolver,
    /// Manifest files the resolver reads for module roots.
    pub configs: Vec<SourceFile>,
}
