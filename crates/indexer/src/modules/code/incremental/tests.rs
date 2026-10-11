use super::super::repository::service::test_utils::build_tar_gz;
use super::*;
use orbit_utils::vfs::Decision;

fn stream(bytes: Vec<u8>) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes.into()) }))
}

fn config() -> CodeIndexingPipelineConfig {
    orbit_server_config::AppConfig::embedded_defaults()
        .engine
        .handlers
        .code_indexing_task
        .pipeline
}

#[tokio::test]
async fn loading_retains_filtered_metadata_and_spills_at_the_memory_limit() {
    let archive = build_tar_gz(&[
        ("root/src/main.rs", b"fn main() {}"),
        ("root/Cargo.toml", b"[package]"),
        ("root/image.png", b"image"),
        ("root/binary.rs", b"\0binary"),
    ]);
    let mut config = config();
    for budget in [0, 1024] {
        config.source_memory_budget_bytes = budget;
        let repository = load(stream(archive.clone()), &config).await.unwrap();
        assert_eq!(repository.usage().files, 4);
        assert_eq!(&*repository.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(&*repository.read("Cargo.toml").unwrap(), b"[package]");
        for (path, reason) in [("image.png", "excluded_extension"), ("binary.rs", "binary")] {
            assert_eq!(
                repository.stat(path).unwrap().decision,
                Some(Decision::List(reason))
            );
            assert_eq!(
                repository.read(path).unwrap_err().kind(),
                std::io::ErrorKind::Unsupported
            );
        }
        assert_eq!(repository.usage().resident == 0, budget == 0);
        assert_eq!(repository.usage().spilled > 0, budget == 0);
    }
}

#[tokio::test]
async fn loading_distinguishes_empty_archives_and_total_byte_limits() {
    let mut config = config();
    config.max_total_bytes = 4;
    assert!(matches!(
        load(stream(Vec::new()), &config).await,
        Err(RepositoryCacheError::EmptyArchive)
    ));
    let archive = build_tar_gz(&[("root/file.png", b"oversize")]);
    assert!(matches!(
        load(stream(archive), &config).await,
        Err(RepositoryCacheError::RepositoryTooLarge)
    ));
}
