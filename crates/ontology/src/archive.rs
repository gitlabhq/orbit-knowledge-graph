use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::{Compression, GzBuilder};
use orbit_utils::archive::extract_tar_gz;
use orbit_utils::fs_stream::{CapExceeded, Counter, Decision, FileInventoryEntry, FileStreamHooks};
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
    #[error("ontology archive extraction: {0}")]
    Extraction(#[from] orbit_utils::fs_stream::StreamError),
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
        let extraction_directory = tempfile::tempdir()?;
        let mut limits = ArchiveLimits {
            source_bytes: Counter::new("ontology source bytes", MAX_SOURCE_BYTES),
            file_count: Counter::new("ontology files", MAX_FILES),
        };

        let inventory = extract_tar_gz(bytes, extraction_directory.path(), &mut limits)?;

        let mut sources = BTreeMap::new();
        for file in inventory {
            let source_path = extraction_directory.path().join(&file.path);
            let content = std::fs::read_to_string(source_path)?;
            sources.insert(file.path, content);
        }

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

    pub fn source_fingerprints(&self) -> BTreeMap<String, String> {
        crate::migrations::source_fingerprints_from(&self.sources)
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

struct ArchiveLimits {
    source_bytes: Counter,
    file_count: Counter,
}

impl FileStreamHooks for ArchiveLimits {
    fn admit(&mut self, file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        self.file_count.add(1)?;
        self.source_bytes.add(file.size)
    }

    fn on_non_regular(&mut self, _file: &FileInventoryEntry) -> Decision {
        Decision::Drop
    }
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
