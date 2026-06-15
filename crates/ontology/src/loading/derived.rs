use serde::Deserialize;

use crate::OntologyError;
use crate::entities::DerivedEntity;
use crate::etl::DEFAULT_TRANSFORM;
use crate::loading::node::EtlYaml;
use crate::loading::{EtlSettings, ReadOntologyFile};

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedYaml {
    #[serde(default)]
    name: Option<String>,
    /// Transform-emitted kinds (MENTIONS, REOPENED) are not in the edge
    /// registry, so this stays the contract; edge sources validate against it.
    #[serde(default)]
    emits: Vec<String>,
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

        // A derived entity's `query:` file is verbatim and owns its SELECT, so
        // the loader synthesizes no column list.
        let etl = self
            .etl
            .into_config(&name, etl_settings, reader, yaml_dir, &[])?;

        Ok(DerivedEntity {
            name,
            emits: self.emits,
            transform,
            etl,
        })
    }
}
