use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ResourceCheck {
    pub provider: String,
    pub resource_type: String,
    pub ids: Vec<i64>,
    pub ability: String,
}

/// One provider's answer for one resource, by id.
#[derive(Debug, Clone)]
pub struct ResourceAuthorization {
    pub provider: String,
    pub resource_type: String,
    pub authorized: HashMap<i64, bool>,
}
