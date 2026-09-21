use ::jobs::{JobKind, JobRef};
use orbit_utils::traversal_path::TraversalPath;

pub const NAMESPACE_DATA: JobKind = JobKind::new("namespace_data");

pub fn namespace_data_job(
    plan_name: &str,
    traversal_path: &TraversalPath,
    namespace_id: i64,
) -> JobRef {
    JobRef {
        campaign: None,
        namespace_id: traversal_path
            .top_level_namespace_id()
            .unwrap_or(namespace_id),
        traversal_path: traversal_path.clone(),
        kind: NAMESPACE_DATA,
        key: plan_name.to_owned(),
    }
}
