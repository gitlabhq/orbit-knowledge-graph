#[derive(Clone, Debug)]
pub struct BillingInputs {
    pub realm: Option<String>,
    pub user_id: i64,
    pub source_type: String,
    pub organization_id: Option<i64>,
    pub instance_id: Option<String>,
    pub unique_instance_id: Option<String>,
    pub instance_version: Option<String>,
    pub global_user_id: Option<String>,
    pub host_name: Option<String>,
    pub root_namespace_id: Option<i64>,
    pub deployment_type: Option<String>,
    pub is_gitlab_team_member: Option<bool>,
    pub orbit_quota_exempt: Option<bool>,
    pub coding_agent: Option<String>,
    pub has_license_checksum: bool,
}
