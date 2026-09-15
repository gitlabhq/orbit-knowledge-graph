#[derive(Clone)]
pub struct SearchCandidate {
    pub id: i64,
    pub label: String,
    pub parent_group: String,
    pub diversity_group: String,
    pub degree: u64,
    pub document_length: u64,
}
