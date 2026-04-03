use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResourceCheck {
    pub resource_type: String,
    pub ids: Vec<i64>,
    pub ability: String,
}

#[derive(Debug, Clone)]
pub struct ResourceAuthorization {
    pub resource_type: String,
    pub authorized: HashMap<i64, bool>,
}
