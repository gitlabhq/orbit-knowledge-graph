use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use orbit_utils::source_archive::{ArchiveLayout, SourceArchive, SourceArchiveError};
use rust_embed::Embed;

use crate::loading::{ReadOntologyFile, load_with};
use crate::{Ontology, OntologyError};

const LAYOUT: ArchiveLayout = ArchiveLayout {
    directory: "ontology-archives",
    root: "ontology",
};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/ontology-archives"]
#[include = "*.tar.gz"]
struct BundledArchives;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("ontology {0}")]
    Source(#[from] SourceArchiveError),
    #[error("ontology archive is missing schema.yaml")]
    MissingSchema,
    #[error("ontology archive cannot be loaded: {0}")]
    Ontology(#[from] OntologyError),
}

#[derive(Debug)]
pub struct OntologyArchive(SourceArchive);

impl OntologyArchive {
    pub fn bundled(schema_version: u32) -> Result<Option<Self>, ArchiveError> {
        BundledArchives::get(&orbit_utils::source_archive::file_name(schema_version))
            .map(|file| Self::from_bytes(schema_version, &file.data))
            .transpose()
    }

    pub fn bundled_versions() -> Result<Vec<u32>, ArchiveError> {
        BundledArchives::iter()
            .map(|path| Ok(orbit_utils::source_archive::parse_file_name(&path)?))
            .collect()
    }

    pub fn path(config_dir: &Path, schema_version: u32) -> PathBuf {
        LAYOUT.path(config_dir, schema_version)
    }

    pub fn from_sources(
        schema_version: u32,
        sources: &BTreeMap<String, String>,
    ) -> Result<Self, ArchiveError> {
        Self::checked(SourceArchive::pack(LAYOUT, schema_version, sources)?)
    }

    pub fn from_bytes(schema_version: u32, bytes: &[u8]) -> Result<Self, ArchiveError> {
        Self::checked(SourceArchive::unpack(LAYOUT, schema_version, bytes)?)
    }

    fn checked(archive: SourceArchive) -> Result<Self, ArchiveError> {
        if archive.sources().contains_key("schema.yaml") {
            Ok(Self(archive))
        } else {
            Err(ArchiveError::MissingSchema)
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.0.schema_version()
    }

    pub fn bytes(&self) -> &[u8] {
        self.0.bytes()
    }

    pub fn sources(&self) -> &BTreeMap<String, String> {
        self.0.sources()
    }

    pub fn matches_sources(&self, sources: &BTreeMap<String, String>) -> bool {
        self.0.matches_sources(sources)
    }

    pub fn load_ontology(&self) -> Result<Ontology, ArchiveError> {
        Ok(load_with(self)?)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), ArchiveError> {
        Ok(self.0.write_atomic(path)?)
    }
}

impl ReadOntologyFile for OntologyArchive {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        self.sources()
            .get(path)
            .cloned()
            .ok_or_else(|| OntologyError::Io {
                path: path.into(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "file missing from ontology archive",
                ),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::OntologyArchive;
    use crate::Ontology;
    use crate::migrations::embedded_sources;

    const SCHEMA_VERSION: u32 = 42;

    #[test]
    fn every_bundled_archive_loads_at_its_declared_version() {
        let versions = OntologyArchive::bundled_versions().unwrap();
        assert!(!versions.is_empty());
        for version in versions {
            let archive = OntologyArchive::bundled(version).unwrap().unwrap();
            assert_eq!(archive.schema_version(), version);
            archive.load_ontology().unwrap();
        }
    }

    #[test]
    fn unsupported_versions_have_no_bundled_archive() {
        assert!(OntologyArchive::bundled(u32::MAX).unwrap().is_none());
    }

    #[test]
    fn retained_archives_reproduce_the_complete_ontology() {
        let archive = embedded_archive();
        let directory = tempfile::tempdir().unwrap();
        let archive_path = OntologyArchive::path(directory.path(), SCHEMA_VERSION);

        archive.write_atomic(&archive_path).unwrap();
        let saved_bytes = std::fs::read(archive_path).unwrap();
        let restored = OntologyArchive::from_bytes(SCHEMA_VERSION, &saved_bytes).unwrap();

        assert_eq!(restored.bytes(), archive.bytes());
        assert_eq!(restored.schema_version(), SCHEMA_VERSION);
        assert!(restored.matches_sources(&embedded_sources()));
        assert_eq!(
            restored.load_ontology().unwrap(),
            Ontology::load_embedded().unwrap()
        );
    }

    #[test]
    fn changed_source_contents_require_a_new_archive() {
        let sources = embedded_sources();
        let archive = embedded_archive();

        for source_path in sources.keys() {
            let mut changed_sources = sources.clone();
            changed_sources.get_mut(source_path).unwrap().push('\n');

            assert!(!archive.matches_sources(&changed_sources), "{source_path}");
        }
    }

    #[test]
    fn added_source_files_require_a_new_archive() {
        let archive = embedded_archive();
        let mut sources = embedded_sources();
        sources.insert("sql/new_query.sql.j2".into(), "SELECT 1".into());

        assert!(!archive.matches_sources(&sources));
    }

    #[test]
    fn removed_source_files_require_a_new_archive() {
        let archive = embedded_archive();
        let mut sources = embedded_sources();
        sources.remove("sql/namespace_storage_snapshot.sql.j2");

        assert!(!archive.matches_sources(&sources));
    }

    #[test]
    fn an_incomplete_archive_never_uses_embedded_files() {
        let archive = archive_missing_a_node();

        assert!(archive.load_ontology().is_err());
    }

    fn embedded_archive() -> OntologyArchive {
        OntologyArchive::from_sources(SCHEMA_VERSION, &embedded_sources()).unwrap()
    }

    fn archive_missing_a_node() -> OntologyArchive {
        let mut sources = embedded_sources();
        let missing_node = sources
            .keys()
            .find(|path| path.starts_with("nodes/"))
            .unwrap()
            .clone();
        sources.remove(&missing_node);
        OntologyArchive::from_sources(SCHEMA_VERSION, &sources).unwrap()
    }
}
