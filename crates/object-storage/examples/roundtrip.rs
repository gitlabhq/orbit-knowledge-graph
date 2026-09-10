use std::path::Path;

use bytes::Bytes;
use orbit_object_storage::ObjectStorage;
use orbit_server_config::{AppConfig, SECRET_FILE_DIR};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .ok_or("usage: roundtrip <config.yaml> [secrets-dir]")?;
    let secrets = args.get(2).map_or(SECRET_FILE_DIR, String::as_str);
    let config = AppConfig::load_from(Some(Path::new(config_path)), Path::new(secrets))?;
    let storage = ObjectStorage::new(&config.object_storage)?;
    storage
        .write("roundtrip/hello.txt", Bytes::from_static(b"hello"))
        .await?;
    let bytes = storage.read("roundtrip/hello.txt").await?;
    assert_eq!(bytes, "hello");
    storage.delete("roundtrip/hello.txt").await?;
    println!(
        "ok {:?} {:?} {}",
        config.object_storage.provider, config.object_storage.auth, config.object_storage.bucket
    );
    Ok(())
}
