use std::io::Write;

use flate2::{Compression, write::GzEncoder};
use ontology::Ontology;
use ontology::archive::{ArchiveError, OntologyArchive};
use ontology::migrations::{embedded_sources, source_fingerprints};
use orbit_utils::fs_stream::StreamError;

#[test]
fn retained_archives_reproduce_the_complete_ontology() {
    let sources = embedded_sources();
    let archive = OntologyArchive::from_sources(42, &sources).unwrap();
    let repeated = OntologyArchive::from_sources(42, &sources).unwrap();
    assert_eq!(archive.bytes(), repeated.bytes());

    let directory = tempfile::tempdir().unwrap();
    let archive_path = OntologyArchive::path(directory.path(), 42);
    archive.write_atomic(&archive_path).unwrap();

    let saved_bytes = std::fs::read(archive_path).unwrap();
    let restored = OntologyArchive::from_bytes(42, &saved_bytes).unwrap();
    assert_eq!(restored.bytes(), archive.bytes());
    assert_eq!(restored.schema_version(), 42);
    assert_eq!(restored.source_fingerprints(), source_fingerprints());
    assert_eq!(
        restored.load_ontology().unwrap(),
        Ontology::load_embedded().unwrap()
    );
}

#[test]
fn an_incomplete_archive_never_uses_embedded_files() {
    let mut sources = embedded_sources();
    let missing_node = sources
        .keys()
        .find(|path| path.starts_with("nodes/"))
        .unwrap()
        .clone();
    sources.remove(&missing_node);

    let archive = OntologyArchive::from_sources(42, &sources).unwrap();
    assert!(archive.load_ontology().is_err());
}

#[test]
fn an_archive_cannot_be_loaded_as_another_schema_version() {
    let archive = OntologyArchive::from_sources(42, &embedded_sources()).unwrap();

    assert!(matches!(
        OntologyArchive::from_bytes(43, archive.bytes()),
        Err(ArchiveError::Version {
            expected: 43,
            actual: 42
        })
    ));
}

#[test]
fn truncated_archives_are_rejected() {
    let archive = OntologyArchive::from_sources(42, &embedded_sources()).unwrap();
    let truncated = &archive.bytes()[..archive.bytes().len() / 2];

    assert!(OntologyArchive::from_bytes(42, truncated).is_err());
}

#[test]
fn oversized_files_are_rejected_before_their_contents_are_read() {
    const OVERSIZED_SOURCE_BYTES: u64 = 32 * 1024 * 1024;

    let mut header = tar::Header::new_gnu();
    header.set_path("ontology/schema.yaml").unwrap();
    header.set_size(OVERSIZED_SOURCE_BYTES);
    header.set_cksum();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(header.as_bytes()).unwrap();
    let bytes = encoder.finish().unwrap();

    assert!(matches!(
        OntologyArchive::from_bytes(42, &bytes),
        Err(ArchiveError::Extraction(StreamError::Cap(_)))
    ));
}

#[test]
fn archives_cannot_traverse_outside_the_extraction_directory() {
    let unsafe_path = b"ontology/../schema.yaml";
    let mut header = tar::Header::new_gnu();
    header.as_mut_bytes()[..unsafe_path.len()].copy_from_slice(unsafe_path);
    header.set_size(0);
    header.set_cksum();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(header.as_bytes()).unwrap();
    let bytes = encoder.finish().unwrap();

    let error = OntologyArchive::from_bytes(42, &bytes).unwrap_err();
    assert!(error.to_string().contains("path traversal"), "{error}");
}
