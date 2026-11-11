use std::collections::HashMap;

use compiler::orbit_authorization::Key;

#[derive(Debug, Clone)]
pub struct ResourceCheck {
    pub provider: String,
    pub resource_type: String,
    pub ids: Vec<Key>,
    pub ability: String,
}

/// One provider's answer for one resource, by key.
#[derive(Debug, Clone)]
pub struct ResourceAuthorization {
    pub provider: String,
    pub resource_type: String,
    pub authorized: HashMap<Key, bool>,
}
