pub use orbit_utils::markdown::LinkTarget;

#[derive(Debug, Default)]
pub struct FileLinks {
    pub targets: Vec<(Option<usize>, LinkTarget)>,
    pub anchors: Vec<(usize, String)>,
}
