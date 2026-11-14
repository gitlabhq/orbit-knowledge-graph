//! Deployment shapes: each names the connections one deployment serves.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::DataModelError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Shape {
    connections: Vec<String>,
}

/// The deployment shapes of one deployments file, by name.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct Deployments(BTreeMap<String, Shape>);

impl Deployments {
    pub fn parse(yaml: &str) -> Result<Self, DataModelError> {
        orbit_utils::yaml::from_str(yaml)
            .map_err(|error| DataModelError::Invalid(format!("deployment shapes: {error}")))
    }

    /// The shapes of `config/deployments.yaml`.
    pub fn embedded() -> Self {
        Self::parse(include_str!(concat!(
            env!("ORBIT_CONFIG_DIR"),
            "/deployments.yaml"
        )))
        .expect("config/deployments.yaml is valid")
    }

    pub fn shapes(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// The connections the shape `name` serves.
    pub fn connections(&self, name: &str) -> Result<&[String], DataModelError> {
        self.0
            .get(name)
            .map(|shape| shape.connections.as_slice())
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "deployment shape",
                name: name.to_string(),
            })
    }
}
