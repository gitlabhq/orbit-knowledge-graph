use serde::Deserialize;

use crate::OntologyError;
use crate::entities::DerivedEntity;
use crate::etl::{DEFAULT_TRANSFORM, EtlScope};
use crate::loading::node::EtlYaml;
use crate::loading::{EtlSettings, ReadOntologyFile};

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedYaml {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    emits: Vec<String>,
    #[serde(default)]
    global: bool,
    etl: EtlYaml,
}

impl DerivedYaml {
    pub(crate) fn into_derived(
        self,
        name: String,
        etl_settings: &EtlSettings,
        reader: &impl ReadOntologyFile,
        yaml_dir: &str,
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

        let scope = if self.global {
            EtlScope::Global
        } else {
            EtlScope::Namespaced
        };
        let etl = self
            .etl
            .into_config(&name, etl_settings, scope, reader, yaml_dir)?;
        Ok(DerivedEntity {
            name,
            emits: self.emits,
            transform,
            etl,
        })
    }
}
