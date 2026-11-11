use serde::Deserialize;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    strum::Display,
    strum::IntoStaticStr,
    strum::VariantNames,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum QueryType {
    Traversal,
    Aggregation,
    PathFinding,
    Neighbors,
    /// Internal-only: consolidated hydration for multiple entity types.
    /// Generates a UNION ALL of search-like arms, one per node. Skips
    /// security context injection (IDs are pre-authorized by the pipeline).
    #[serde(skip)]
    Hydration,
}
