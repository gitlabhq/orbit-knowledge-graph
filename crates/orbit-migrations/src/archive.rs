use std::path::{Path, PathBuf};
use std::sync::Arc;

use ontology::Ontology;
use ontology::archive::OntologyArchive;
use orbit_utils::source_archive::{
    ArchiveLayout, SourceArchive, SourceArchiveError, file_name, parse_file_name,
};
use query_data_model::{Model, Packages};
use rust_embed::Embed;

use crate::catalog::CatalogError;

const LAYOUT: ArchiveLayout = ArchiveLayout {
    directory: "model-archives",
    root: "model",
};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/model-archives"]
#[include = "*.tar.gz"]
struct BundledArchives;

#[derive(Debug)]
pub struct ModelArchive(SourceArchive);

impl ModelArchive {
    pub fn bundled(schema_version: u32) -> Result<Option<Self>, SourceArchiveError> {
        BundledArchives::get(&file_name(schema_version))
            .map(|file| Self::from_bytes(schema_version, &file.data))
            .transpose()
    }

    pub fn bundled_versions() -> Result<Vec<u32>, SourceArchiveError> {
        BundledArchives::iter()
            .map(|path| parse_file_name(&path))
            .collect()
    }

    pub fn path(config_dir: &Path, schema_version: u32) -> PathBuf {
        LAYOUT.path(config_dir, schema_version)
    }

    pub fn from_packages(
        schema_version: u32,
        packages: &Packages,
    ) -> Result<Self, SourceArchiveError> {
        SourceArchive::pack(LAYOUT, schema_version, packages.sources()).map(Self)
    }

    pub fn from_bytes(schema_version: u32, bytes: &[u8]) -> Result<Self, SourceArchiveError> {
        SourceArchive::unpack(LAYOUT, schema_version, bytes).map(Self)
    }

    pub fn schema_version(&self) -> u32 {
        self.0.schema_version()
    }

    pub fn bytes(&self) -> &[u8] {
        self.0.bytes()
    }

    pub fn packages(&self) -> Packages {
        Packages::new(self.0.sources().clone())
    }

    pub fn matches(&self, packages: &Packages) -> bool {
        self.0.matches_sources(packages.sources())
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), SourceArchiveError> {
        self.0.write_atomic(path)
    }
}

#[derive(Debug)]
pub struct Release {
    ontology: OntologyArchive,
    model: ModelArchive,
}

pub struct LoadedRelease {
    pub ontology: Ontology,
    pub model: Arc<Model>,
}

impl Release {
    pub fn new(ontology: OntologyArchive, model: ModelArchive) -> Result<Self, CatalogError> {
        if ontology.schema_version() != model.schema_version() {
            return Err(CatalogError::SplitRelease {
                ontology: ontology.schema_version(),
                model: model.schema_version(),
            });
        }
        Ok(Self { ontology, model })
    }

    pub fn bundled(schema_version: u32) -> Result<Option<Self>, CatalogError> {
        match (
            OntologyArchive::bundled(schema_version)?,
            ModelArchive::bundled(schema_version)?,
        ) {
            (Some(ontology), Some(model)) => Self::new(ontology, model).map(Some),
            (None, None) => Ok(None),
            _ => Err(CatalogError::Missing(schema_version)),
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.ontology.schema_version()
    }

    pub fn ontology_archive(&self) -> &OntologyArchive {
        &self.ontology
    }

    pub fn model_archive(&self) -> &ModelArchive {
        &self.model
    }

    pub fn load_ontology(&self) -> Result<Ontology, CatalogError> {
        Ok(self.ontology.load_ontology()?)
    }

    pub fn packages(&self) -> Packages {
        self.model.packages()
    }

    pub fn load(&self) -> Result<LoadedRelease, CatalogError> {
        Ok(LoadedRelease {
            ontology: self.load_ontology()?,
            model: self.packages().load()?,
        })
    }
}
