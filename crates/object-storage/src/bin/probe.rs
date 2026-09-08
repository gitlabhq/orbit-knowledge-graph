//! Round-trips a config file against the bucket it describes.
//!
//! Loads the file with the same three layers as the server (file, secret
//! directory, `GKG_*` env), builds the store, then writes, reads, lists,
//! copies and deletes objects under a unique `orbit-probe/<run id>/` prefix.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, bail, ensure};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use orbit_object_storage::build_store;
use orbit_server_config::AppConfig;
use tokio::io::AsyncWriteExt;
use tracing::info;

const MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;

#[derive(Parser)]
#[command(about = "Connection probe for the object_storage config section")]
struct Args {
    /// YAML file with an `object_storage` section, shaped like config/default.yaml.
    #[arg(long)]
    config: PathBuf,
    /// Directory laid out like /etc/secrets; files become config values.
    #[arg(long)]
    secrets_dir: Option<PathBuf>,
    /// Size of the multipart upload check; must exceed one part to exercise multipart.
    #[arg(long, default_value_t = 12 * 1024 * 1024)]
    multipart_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,object_store=warn".into()),
        )
        .init();
    let args = Args::parse();

    let secrets_dir = args
        .secrets_dir
        .as_deref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| orbit_server_config::SECRET_FILE_DIR.to_string());
    let config_path = args.config.to_string_lossy().into_owned();
    let app = AppConfig::load_from_sources(&config_path, &secrets_dir)
        .with_context(|| format!("loading {config_path}"))?;
    let Some(storage) = app.object_storage else {
        bail!("{config_path} has no object_storage section");
    };
    info!(
        provider = <&str>::from(storage.provider),
        bucket = %storage.bucket,
        prefix = storage.prefix.as_deref().unwrap_or(""),
        "config loaded"
    );

    let store = build_store(&storage).context("building object store")?;
    let base = Path::from(format!("orbit-probe/{}", uuid::Uuid::new_v4()));
    let outcome = round_trip(&store, &base, args.multipart_bytes).await;
    cleanup(&store, &base).await;
    outcome?;
    info!("PROBE OK");
    Ok(())
}

async fn round_trip(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    multipart_bytes: usize,
) -> anyhow::Result<()> {
    let small = base.clone().join("small.txt");
    let payload = Bytes::from_static(b"orbit object storage probe");

    timed("put", store.put(&small, PutPayload::from(payload.clone()))).await?;
    let meta = timed("head", store.head(&small)).await?;
    ensure!(
        meta.size == payload.len() as u64,
        "head size {} != {}",
        meta.size,
        payload.len()
    );
    let body = timed("get", async { store.get(&small).await?.bytes().await }).await?;
    ensure!(body == payload, "get returned different bytes");

    let listed: Vec<_> = timed("list", store.list(Some(base)).try_collect()).await?;
    ensure!(
        listed.iter().any(|m| m.location == small),
        "list under {base} did not return {small}"
    );

    let copy = base.clone().join("copy.txt");
    timed("copy", store.copy(&small, &copy)).await?;
    timed("head copy", store.head(&copy)).await?;

    let large = base.clone().join("multipart.bin");
    timed("multipart put", async {
        let mut writer =
            BufWriter::with_capacity(Arc::clone(store), large.clone(), MULTIPART_PART_BYTES);
        let chunk = vec![0xA5u8; 1024 * 1024];
        let mut remaining = multipart_bytes;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            writer.write_all(&chunk[..n]).await?;
            remaining -= n;
        }
        writer.shutdown().await
    })
    .await?;
    let meta = timed("head multipart", store.head(&large)).await?;
    ensure!(
        meta.size == multipart_bytes as u64,
        "multipart size {} != {multipart_bytes}",
        meta.size
    );
    let tail = timed(
        "get_range",
        store.get_range(&large, meta.size - 16..meta.size),
    )
    .await?;
    ensure!(
        tail.iter().all(|b| *b == 0xA5),
        "multipart tail bytes differ"
    );

    for path in [&small, &copy, &large] {
        timed("delete", store.delete(path)).await?;
    }
    let left: Vec<_> = store.list(Some(base)).try_collect().await?;
    ensure!(
        left.is_empty(),
        "{} objects left under {base} after delete",
        left.len()
    );
    Ok(())
}

async fn cleanup(store: &Arc<dyn ObjectStore>, base: &Path) {
    let Ok(left) = store.list(Some(base)).try_collect::<Vec<_>>().await else {
        return;
    };
    for meta in left {
        if let Err(error) = store.delete(&meta.location).await {
            tracing::warn!(%error, location = %meta.location, "cleanup failed");
        }
    }
}

async fn timed<T, E>(step: &str, fut: impl Future<Output = Result<T, E>>) -> anyhow::Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let started = Instant::now();
    let result = fut.await.with_context(|| format!("step '{step}' failed"))?;
    info!(
        step,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "ok"
    );
    Ok(result)
}
