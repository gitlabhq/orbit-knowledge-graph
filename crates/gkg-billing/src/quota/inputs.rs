#[derive(Clone, Debug)]
pub struct QuotaCheckInputs {
    pub source_type: String,
    pub user_id: i64,
    pub realm: Option<String>,
    pub global_user_id: Option<String>,
    pub root_namespace_id: Option<i64>,
    pub instance_id: Option<String>,
    pub unique_instance_id: Option<String>,
}
