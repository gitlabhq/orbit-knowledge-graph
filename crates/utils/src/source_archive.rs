use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use flate2::{Compression, GzBuilder};
use serde::{Deserialize, Serialize};

use crate::fs_walk::{CapExceeded, Counter};

const FORMAT_VERSION: u32 = 1;
const MANIFEST_PATH: &str = "manifest.json";
const MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FILES: u64 = 4096;

#[derive(Debug, thiserror::Error)]
pub enum SourceArchiveError {
    #[error("archive I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("archive limit: {0}")]
    Cap(#[from] CapExceeded),
    #[error("invalid archive manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("invalid archive: {0}")]
    Invalid(String),
    #[error("archive declares schema {actual}, expected {expected}")]
    Version { expected: u32, actual: u32 },
    #[error("unsupported archive format {0}")]
    UnsupportedFormat(u32),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    schema_version: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct ArchiveLayout {
    pub directory: &'static str,
    pub root: &'static str,
}

impl ArchiveLayout {
    pub fn path(&self, config_dir: &Path, schema_version: u32) -> PathBuf {
        config_dir
            .join(self.directory)
            .join(file_name(schema_version))
    }
}

pub fn file_name(schema_version: u32) -> String {
    format!("v{schema_version}.tar.gz")
}

pub fn parse_file_name(name: &str) -> Result<u32, SourceArchiveError> {
    name.strip_prefix('v')
        .and_then(|name| name.strip_suffix(".tar.gz"))
        .and_then(|version| version.parse::<u32>().ok())
        .filter(|version| name == file_name(*version))
        .ok_or_else(|| SourceArchiveError::Invalid(format!("invalid archive filename {name}")))
}

#[derive(Debug, Clone)]
pub struct SourceArchive {
    schema_version: u32,
    bytes: Vec<u8>,
    sources: BTreeMap<String, String>,
}

impl SourceArchive {
    pub fn pack(
        layout: ArchiveLayout,
        schema_version: u32,
        sources: &BTreeMap<String, String>,
    ) -> Result<Self, SourceArchiveError> {
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            schema_version,
        };
        let encoder = GzBuilder::new()
            .mtime(0)
            .operating_system(255)
            .write(Vec::new(), Compression::default());
        let mut builder = tar::Builder::new(encoder);
        append_file(
            &mut builder,
            layout.root,
            MANIFEST_PATH,
            &serde_json::to_vec(&manifest)?,
        )?;
        for (path, content) in sources {
            if path == MANIFEST_PATH || !crate::fs::is_safe_relative_path(Path::new(path)) {
                return Err(SourceArchiveError::Invalid(format!(
                    "reserved or unsafe path {path}"
                )));
            }
            append_file(&mut builder, layout.root, path, content.as_bytes())?;
        }
        let bytes = builder.into_inner()?.finish()?;
        Self::unpack(layout, schema_version, &bytes)
    }

    pub fn unpack(
        layout: ArchiveLayout,
        schema_version: u32,
        bytes: &[u8],
    ) -> Result<Self, SourceArchiveError> {
        let mut sources = read_sources(layout.root, bytes)?;
        let manifest = sources
            .remove(MANIFEST_PATH)
            .ok_or_else(|| SourceArchiveError::Invalid("missing manifest.json".into()))?;
        let manifest: Manifest = serde_json::from_str(&manifest)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(SourceArchiveError::UnsupportedFormat(
                manifest.format_version,
            ));
        }
        if manifest.schema_version != schema_version {
            return Err(SourceArchiveError::Version {
                expected: schema_version,
                actual: manifest.schema_version,
            });
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

    pub fn sources(&self) -> &BTreeMap<String, String> {
        &self.sources
    }

    pub fn matches_sources(&self, sources: &BTreeMap<String, String>) -> bool {
        &self.sources == sources
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), SourceArchiveError> {
        let parent = path
            .parent()
            .ok_or_else(|| SourceArchiveError::Invalid("archive path has no parent".into()))?;
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(self.bytes())?;
        temporary.persist(path).map_err(|error| error.error)?;
        Ok(())
    }
}

fn read_sources(root: &str, bytes: &[u8]) -> Result<BTreeMap<String, String>, SourceArchiveError> {
    let mut source_bytes = Counter::new("archive source bytes", Some(MAX_SOURCE_BYTES));
    let mut file_count = Counter::new("archive files", Some(MAX_FILES));
    let mut sources = BTreeMap::new();
    let mut archive = tar::Archive::new(GzDecoder::new(bytes));
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let relative_path = source_path(root, &entry.path()?)?;
        file_count.add(1)?;
        source_bytes.add(entry.size())?;
        let mut content = String::new();
        entry.read_to_string(&mut content)?;
        sources.insert(relative_path, content);
    }
    Ok(sources)
}

fn source_path(root: &str, entry_path: &Path) -> Result<String, SourceArchiveError> {
    let relative_path = entry_path.strip_prefix(root).map_err(|_| {
        SourceArchiveError::Invalid(format!(
            "entry {} is outside the {root} root",
            entry_path.display()
        ))
    })?;
    if relative_path.as_os_str().is_empty() || !crate::fs::is_safe_relative_path(relative_path) {
        return Err(SourceArchiveError::Invalid(format!(
            "path traversal detected: {}",
            entry_path.display()
        )));
    }
    Ok(relative_path.to_string_lossy().into_owned())
}

fn append_file<W: Write>(
    builder: &mut tar::Builder<W>,
    root: &str,
    path: &str,
    content: &[u8],
) -> Result<(), SourceArchiveError> {
    let mut header = tar::Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append_data(&mut header, Path::new(root).join(path), content)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::{ArchiveLayout, SourceArchive, SourceArchiveError};

    const LAYOUT: ArchiveLayout = ArchiveLayout {
        directory: "archives",
        root: "sources",
    };
    const SCHEMA_VERSION: u32 = 42;

    fn sources() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("a/one.yaml".to_string(), "one: 1\n".to_string()),
            ("two.yaml".to_string(), "two: 2\n".to_string()),
        ])
    }

    fn archive() -> SourceArchive {
        SourceArchive::pack(LAYOUT, SCHEMA_VERSION, &sources()).unwrap()
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

    #[test]
    fn identical_sources_produce_identical_archive_bytes() {
        assert_eq!(archive().bytes(), archive().bytes());
    }

    #[test]
    fn written_archives_restore_their_sources() {
        let directory = tempfile::tempdir().unwrap();
        let path = LAYOUT.path(directory.path(), SCHEMA_VERSION);
        archive().write_atomic(&path).unwrap();
        let restored =
            SourceArchive::unpack(LAYOUT, SCHEMA_VERSION, &std::fs::read(path).unwrap()).unwrap();
        assert_eq!(restored.bytes(), archive().bytes());
        assert!(restored.matches_sources(&sources()));
    }

    #[test]
    fn an_archive_cannot_be_loaded_as_another_schema_version() {
        assert!(matches!(
            SourceArchive::unpack(LAYOUT, SCHEMA_VERSION + 1, archive().bytes()),
            Err(SourceArchiveError::Version { expected, actual })
                if expected == SCHEMA_VERSION + 1 && actual == SCHEMA_VERSION
        ));
    }

    #[test]
    fn truncated_archives_are_rejected() {
        let bytes = archive().bytes().to_vec();
        assert!(SourceArchive::unpack(LAYOUT, SCHEMA_VERSION, &bytes[..bytes.len() / 2]).is_err());
    }

    #[test]
    fn oversized_files_are_rejected_before_their_contents_are_read() {
        let bytes = archive_with_header_only("sources/two.yaml", 32 * 1024 * 1024);
        assert!(matches!(
            SourceArchive::unpack(LAYOUT, SCHEMA_VERSION, &bytes),
            Err(SourceArchiveError::Cap(_))
        ));
    }

    #[test]
    fn archives_cannot_escape_the_archive_root() {
        let bytes = archive_with_header_only("sources/../two.yaml", 0);
        let error = SourceArchive::unpack(LAYOUT, SCHEMA_VERSION, &bytes).unwrap_err();
        assert!(error.to_string().contains("path traversal"), "{error}");
    }

    #[test]
    fn archive_file_names_carry_their_schema_version() {
        assert_eq!(super::parse_file_name(&super::file_name(7)).unwrap(), 7);
        assert!(super::parse_file_name("v07.tar.gz").is_err());
    }
}
