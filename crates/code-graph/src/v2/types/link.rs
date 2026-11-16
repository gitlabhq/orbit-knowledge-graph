pub use orbit_utils::markdown::LinkTarget;

/// Page-level links a custom parser found in one file, resolved lexically to
/// repository paths. The pipeline turns them into `File -IMPORTS-> File` or,
/// when the anchor names a definition, `File -IMPORTS-> Definition` edges.
#[derive(Debug, Default)]
pub struct FileLinks {
    pub targets: Vec<LinkTarget>,
    /// `(definition index, anchor)` for the file's definitions a link can name.
    pub anchors: Vec<(usize, String)>,
}
