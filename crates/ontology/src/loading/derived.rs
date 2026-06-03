use serde::Deserialize;

use crate::OntologyError;
use crate::entities::DerivedEntity;
use crate::loading::EtlSettings;
use crate::loading::node::EtlYaml;

const DEFAULT_TRANSFORM: &str = "data_fusion";

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedYaml {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    emits: Vec<String>,
    etl: EtlYaml,
}

impl DerivedYaml {
    pub(crate) fn into_derived(
        self,
        name: String,
        etl_settings: &EtlSettings,
    ) -> Result<DerivedEntity, OntologyError> {
        if self
            .name
            .as_deref()
            .is_some_and(|declared| declared != name)
        {
            return Err(OntologyError::Validation(format!(
                "derived entity '{name}' declares a name that does not match its key"
            )));
        }

        let transform = match self.etl.transform() {
            Some(t) if t != DEFAULT_TRANSFORM => t.to_string(),
            _ => {
                return Err(OntologyError::Validation(format!(
                    "derived entity '{name}' must set etl.transform to a custom transform name"
                )));
            }
        };

        Ok(DerivedEntity {
            name,
            emits: self.emits,
            transform,
            etl: self.etl.into_config(etl_settings)?,
        })
    }
}
