use serde::Deserialize;

use crate::OntologyError;
use crate::entities::DerivedEntity;
use crate::etl::EtlScope;
use crate::loading::node::{IndexerYaml, PipelineYaml};
use crate::loading::{EtlSettings, ReadOntologyFile};

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
        yaml_path: &str,
        etl_settings: &EtlSettings,
        reader: &impl ReadOntologyFile,
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

        let scope = if self.global {
            EtlScope::Global
        } else {
            EtlScope::Namespaced
        };
        if let Some(indexer) = &self.indexer {
            indexer.validate(&name)?;
        }
        let reindex = super::node::ReindexDirective::from_indexer(self.indexer.as_ref());
        let pipelines = self
            .pipelines
            .into_iter()
            .map(|p| {
                p.into_pipeline(
                    &name,
                    etl_settings,
                    scope,
                    reader,
                    super::node::PipelineOptions {
                        yaml_path,
                        allow_rust_transform: true,
                        is_derived: true,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if pipelines
            .iter()
            .any(|p| matches!(p.transform, crate::etl::Transform::DataFusion { .. }))
        {
            return Err(OntologyError::Validation(format!(
                "derived entity '{name}' must set transform.type to a custom transform name"
            )));
        }
        if pipelines.is_empty() {
            return Err(OntologyError::Validation(format!(
                "derived entity '{name}' must declare at least one pipeline"
            )));
        }
        let reindex_on = reindex.resolve_reindex_sources(&name, &pipelines)?;
        Ok(DerivedEntity {
            name,
            emits: self.emits,
            pipelines,
            reindex_on,
        })
    }
}
