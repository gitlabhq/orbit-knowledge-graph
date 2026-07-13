use serde::Deserialize;

use crate::OntologyError;
use crate::entities::DerivedEntity;
use crate::etl::{DEFAULT_TRANSFORM, EtlScope};
use crate::loading::EtlSettings;
use crate::loading::node::{IndexerYaml, PipelineYaml};

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedYaml {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    emits: Vec<String>,
    #[serde(default)]
    global: bool,
    #[serde(default)]
    indexer: Option<IndexerYaml>,
    pipelines: Vec<PipelineYaml>,
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

        if let Some(indexer) = &self.indexer {
            indexer.validate(&name)?;
        }
        let indexer = self.indexer;
        let pipeline = match <[PipelineYaml; 1]>::try_from(self.pipelines) {
            Ok([pipeline]) => pipeline,
            Err(pipelines) => {
                return Err(OntologyError::Validation(format!(
                    "derived entity '{name}' declares {} pipelines; derived entities support exactly one",
                    pipelines.len()
                )));
            }
        };

        let transform = pipeline.transform_type();
        if transform == DEFAULT_TRANSFORM {
            return Err(OntologyError::Validation(format!(
                "derived entity '{name}' must set transform.type to a custom transform name"
            )));
        }
        let transform = transform.to_string();

        let scope = if self.global {
            EtlScope::Global
        } else {
            EtlScope::Namespaced
        };
        let etl = pipeline.into_config(&name, etl_settings, scope, indexer.as_ref())?;
        Ok(DerivedEntity {
            name,
            emits: self.emits,
            transform,
            etl,
        })
    }
}
