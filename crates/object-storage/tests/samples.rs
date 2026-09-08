use std::path::Path;

use orbit_server_config::{AppConfig, ObjectStorageConfigError};

/// Samples with static S3 auth take their keys from the secret mount, so the test provides one;
/// the others get none, because mounted keys next to an identity or GCS section fail validation
/// on purpose. Samples also point at certificate and key files that do not exist on a developer
/// machine, so a missing file is the only validation failure a sample may produce.
#[test]
fn every_sample_loads_through_the_server_config_layers() {
    let samples = Path::new(env!("CARGO_MANIFEST_DIR")).join("samples");
    let secrets = tempfile::tempdir().unwrap();
    let s3_secrets = secrets.path().join("s3");
    let s3_keys = s3_secrets.join("object_storage/s3");
    std::fs::create_dir_all(&s3_keys).unwrap();
    std::fs::write(s3_keys.join("access_key_id"), "sample-key-id").unwrap();
    std::fs::write(s3_keys.join("secret_access_key"), "sample-secret").unwrap();
    let no_secrets = secrets.path().join("none");

    let mut seen = 0;
    for entry in std::fs::read_dir(&samples).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "yaml") {
            continue;
        }
        seen += 1;
        let uses_static_keys = std::fs::read_to_string(&path)
            .unwrap()
            .contains("auth: static");
        let secret_dir = if uses_static_keys {
            &s3_secrets
        } else {
            &no_secrets
        };
        let app =
            AppConfig::load_from_sources(path.to_str().unwrap(), secret_dir.to_str().unwrap())
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let storage = app
            .object_storage
            .unwrap_or_else(|| panic!("{}: no object_storage section", path.display()));
        match storage.validate() {
            Ok(()) | Err(ObjectStorageConfigError::FileNotFound { .. }) => {}
            Err(e) => panic!("{}: {e}", path.display()),
        }
    }
    assert!(seen >= 6, "expected the committed samples, found {seen}");
}
