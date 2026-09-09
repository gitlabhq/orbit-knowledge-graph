use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use flate2::{Compression, GzBuilder};
use orbit_utils::fs_stream::{CapExceeded, Counter};
use serde::{Deserialize, Serialize};

use crate::loading::{ReadOntologyFile, load_with};
use crate::{Ontology, OntologyError};

const ARCHIVE_DIRECTORY: &str = "ontology-archives";
const ARCHIVE_ROOT: &str = "ontology";
const FORMAT_VERSION: u32 = 1;
const MANIFEST_PATH: &str = "manifest.json";
const MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FILES: u64 = 4096;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("ontology archive I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("ontology archive limit: {0}")]
    Cap(#[from] CapExceeded),
    #[error("invalid ontology archive manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("invalid ontology archive: {0}")]
    Invalid(String),
    #[error("ontology archive declares schema {actual}, expected {expected}")]
    Version { expected: u32, actual: u32 },
    #[error("unsupported ontology archive format {0}")]
    UnsupportedFormat(u32),
    #[error("ontology archive cannot be loaded: {0}")]
    Ontology(#[from] OntologyError),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    schema_version: u32,
}

#[derive(Debug)]
pub struct OntologyArchive {
    schema_version: u32,
    bytes: Vec<u8>,
    sources: BTreeMap<String, String>,
}

impl OntologyArchive {
    pub fn path(config_dir: &Path, schema_version: u32) -> PathBuf {
        config_dir
            .join(ARCHIVE_DIRECTORY)
            .join(format!("v{schema_version}.tar.gz"))
    }

    pub fn from_sources(
        schema_version: u32,
        sources: &BTreeMap<String, String>,
    ) -> Result<Self, ArchiveError> {
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            schema_version,
        };
        let encoder = GzBuilder::new()
            .mtime(0)
            .operating_system(255)
            .write(Vec::new(), Compression::default());
        let mut builder = tar::Builder::new(encoder);
        append_file(&mut builder, MANIFEST_PATH, &serde_json::to_vec(&manifest)?)?;

        for (path, content) in sources {
            if path == MANIFEST_PATH || !orbit_utils::fs::is_safe_relative_path(Path::new(path)) {
                return Err(ArchiveError::Invalid(format!(
                    "reserved or unsafe path {path}"
                )));
            }
            append_file(&mut builder, path, content.as_bytes())?;
        }

        let encoder = builder.into_inner()?;
        let bytes = encoder.finish()?;
        Self::from_bytes(schema_version, &bytes)
    }

    pub fn from_bytes(schema_version: u32, bytes: &[u8]) -> Result<Self, ArchiveError> {
        let mut sources = read_sources(bytes)?;

        let manifest_json = sources
            .remove(MANIFEST_PATH)
            .ok_or_else(|| ArchiveError::Invalid("missing manifest.json".into()))?;
        let manifest: Manifest = serde_json::from_str(&manifest_json)?;

        if manifest.format_version != FORMAT_VERSION {
            return Err(ArchiveError::UnsupportedFormat(manifest.format_version));
        }

        if manifest.schema_version != schema_version {
            return Err(ArchiveError::Version {
                expected: schema_version,
                actual: manifest.schema_version,
            });
        }

        if !sources.contains_key("schema.yaml") {
            return Err(ArchiveError::Invalid("missing schema.yaml".into()));
        }

        Ok(Self {
            schema_version,
            bytes: bytes.to_vec(),
            sources,
        })
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn matches_sources(&self, sources: &BTreeMap<String, String>) -> bool {
        &self.sources == sources
    }

    pub fn load_ontology(&self) -> Result<Ontology, ArchiveError> {
        Ok(load_with(self)?)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), ArchiveError> {
        let parent = path
            .parent()
            .ok_or_else(|| ArchiveError::Invalid("archive path has no parent".into()))?;
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(self.bytes())?;
        temporary.persist(path).map_err(|error| error.error)?;
        Ok(())
    }
}

impl ReadOntologyFile for OntologyArchive {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        self.sources
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

fn read_sources(bytes: &[u8]) -> Result<BTreeMap<String, String>, ArchiveError> {
    let mut source_bytes = Counter::new("ontology source bytes", MAX_SOURCE_BYTES);
    let mut file_count = Counter::new("ontology files", MAX_FILES);
    let mut sources = BTreeMap::new();

    let mut archive = tar::Archive::new(GzDecoder::new(bytes));
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }

        let relative_path = source_path(&entry.path()?)?;
        file_count.add(1)?;
        source_bytes.add(entry.size())?;

        let mut content = String::new();
        entry.read_to_string(&mut content)?;
        sources.insert(relative_path, content);
    }

    Ok(sources)
}

fn source_path(entry_path: &Path) -> Result<String, ArchiveError> {
    let relative_path = entry_path.strip_prefix(ARCHIVE_ROOT).map_err(|_| {
        ArchiveError::Invalid(format!(
            "entry {} is outside the {ARCHIVE_ROOT} root",
            entry_path.display()
        ))
    })?;
    if relative_path.as_os_str().is_empty()
        || !orbit_utils::fs::is_safe_relative_path(relative_path)
    {
        return Err(ArchiveError::Invalid(format!(
            "path traversal detected: {}",
            entry_path.display()
        )));
    }
    Ok(relative_path.to_string_lossy().into_owned())
}

fn append_file<W: Write>(
    builder: &mut tar::Builder<W>,
    path: &str,
    content: &[u8],
) -> Result<(), ArchiveError> {
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append_data(&mut header, Path::new(ARCHIVE_ROOT).join(path), content)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::{ArchiveError, OntologyArchive};
    use crate::Ontology;
    use crate::migrations::embedded_sources;

    const SCHEMA_VERSION: u32 = 42;

    #[test]
    fn identical_sources_produce_identical_archive_bytes() {
        assert_eq!(embedded_archive().bytes(), embedded_archive().bytes());
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

    #[test]
    fn an_archive_cannot_be_loaded_as_another_schema_version() {
        let archive = embedded_archive();

        assert!(matches!(
            OntologyArchive::from_bytes(SCHEMA_VERSION + 1, archive.bytes()),
            Err(ArchiveError::Version { expected, actual })
                if expected == SCHEMA_VERSION + 1 && actual == SCHEMA_VERSION
        ));
    }

    #[test]
    fn truncated_archives_are_rejected() {
        let archive = embedded_archive();
        let truncated = &archive.bytes()[..archive.bytes().len() / 2];

        assert!(OntologyArchive::from_bytes(SCHEMA_VERSION, truncated).is_err());
    }

    #[test]
    fn oversized_files_are_rejected_before_their_contents_are_read() {
        const OVERSIZED_SOURCE_BYTES: u64 = 32 * 1024 * 1024;
        let bytes = archive_with_header_only("ontology/schema.yaml", OVERSIZED_SOURCE_BYTES);

        assert!(matches!(
            OntologyArchive::from_bytes(SCHEMA_VERSION, &bytes),
            Err(ArchiveError::Cap(_))
        ));
    }

    #[test]
    fn archives_cannot_escape_the_archive_root() {
        let bytes = archive_with_header_only("ontology/../schema.yaml", 0);

        let error = OntologyArchive::from_bytes(SCHEMA_VERSION, &bytes).unwrap_err();

        assert!(error.to_string().contains("path traversal"), "{error}");
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

    fn archive_with_header_only(path: &str, declared_size: u64) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
        header.set_size(declared_size);
        header.set_cksum();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(header.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }
}
