//! A local S3 server in a container, seeded with Parquet files through `object_store`, and the
//! DuckDB connection config that reads it in either location mode.

use std::path::{Path, PathBuf};
use std::time::Duration;

use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectStoreExt, PutPayload};
use orbit_server_config::{
    DuckDbConnection, DuckDbLocation, S3Credentials, S3Endpoint, S3UrlStyle,
};
use testcontainers::ContainerAsync;
use testcontainers::core::{ContainerPort, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rustfs::RustFS;

/// RustFS 1.0.4. MinIO's community images are no longer published, and RustFS is the S3 server
/// testcontainers-modules ships that pulls and needs no setup beyond its keys. It logs to files,
/// so readiness is the start line and the bucket request retries until the server answers.
const RUSTFS_TAG: &str =
    "1.0.4@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2";
const S3_PORT: u16 = 9000;
const REGION: &str = "us-east-1";
pub const BUCKET: &str = "orbit-lake";

/// The extension files DuckDB loads for S3, cached under the workspace target directory; they
/// download once, on the first run that lacks them.
pub fn extension_directory() -> PathBuf {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/duckdb-extensions");
    std::fs::create_dir_all(&directory).expect("create the DuckDB extension directory");
    let directory = directory
        .canonicalize()
        .expect("resolve the DuckDB extension directory");
    let conn = duckdb::Connection::open_in_memory().expect("open DuckDB");
    conn.execute_batch(&format!(
        "SET extension_directories = ['{}']",
        directory.display()
    ))
    .expect("point DuckDB at the extension directory");
    for extension in ["httpfs", "ducklake"] {
        let installed: bool = conn
            .query_row(
                "SELECT installed FROM duckdb_extensions() WHERE extension_name = ?",
                [extension],
                |row| row.get(0),
            )
            .expect("list DuckDB extensions");
        if !installed {
            conn.execute_batch(&format!("INSTALL {extension}"))
                .unwrap_or_else(|error| panic!("install {extension}: {error}"));
        }
    }
    directory
}

/// One bucket on a RustFS container that testcontainers removes when this drops.
pub struct S3Lake {
    _container: ContainerAsync<RustFS>,
    pub endpoint: String,
    pub credentials: S3Credentials,
    store: AmazonS3,
}

impl S3Lake {
    pub async fn start() -> Self {
        let credentials = S3Credentials {
            access_key_id: format!("test-{}", uuid::Uuid::new_v4().simple()),
            secret_access_key: uuid::Uuid::new_v4().simple().to_string(),
            session_token: None,
        };
        let container = RustFS::default()
            .with_tag(RUSTFS_TAG)
            .with_env_var("RUSTFS_ACCESS_KEY", &credentials.access_key_id)
            .with_env_var("RUSTFS_SECRET_KEY", &credentials.secret_access_key)
            .with_ready_conditions(vec![WaitFor::message_on_stdout(
                "Starting: /usr/bin/rustfs",
            )])
            .start()
            .await
            .expect("start the RustFS container");
        let port = container
            .get_host_port_ipv4(ContainerPort::Tcp(S3_PORT))
            .await
            .expect("the RustFS S3 port");
        let endpoint = format!("127.0.0.1:{port}");
        let store = AmazonS3Builder::new()
            .with_endpoint(format!("http://{endpoint}"))
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_region(REGION)
            .with_bucket_name(BUCKET)
            .with_access_key_id(&credentials.access_key_id)
            .with_secret_access_key(&credentials.secret_access_key)
            .build()
            .expect("build the S3 client");
        let lake = Self {
            _container: container,
            endpoint,
            credentials,
            store,
        };
        lake.create_bucket().await;
        lake
    }

    /// `object_store` has no bucket call, so a presigned PUT on the bucket's root creates it.
    async fn create_bucket(&self) {
        let url = self
            .store
            .signed_url(
                reqwest::Method::PUT,
                &ObjectPath::from(""),
                Duration::from_secs(60),
            )
            .await
            .expect("sign the bucket request");
        let client = reqwest::Client::new();
        for _ in 0..100 {
            match client.put(url.clone()).send().await {
                Ok(response) if response.status().is_success() => return,
                Ok(response) if response.status().is_client_error() => {
                    panic!("RustFS refused the bucket: {}", response.status())
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        panic!("RustFS never accepted the bucket");
    }

    pub async fn put(&self, key: &str, bytes: Vec<u8>) {
        self.store
            .put(&ObjectPath::from(key), PutPayload::from(bytes))
            .await
            .unwrap_or_else(|error| panic!("upload {key}: {error}"));
    }

    pub fn connection(&self, location: DuckDbLocation) -> DuckDbConnection {
        DuckDbConnection {
            extension_directory: extension_directory(),
            s3: S3Endpoint {
                endpoint: self.endpoint.clone(),
                region: REGION.into(),
                url_style: S3UrlStyle::Path,
                use_ssl: false,
            },
            credentials: self.credentials.clone(),
            location,
        }
    }

    /// A DuckDB connection with this lake's secret, for seeding; it holds no lockdown.
    pub fn seeder(&self) -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().expect("open DuckDB");
        conn.execute_batch(&format!(
            "SET extension_directories = ['{}']; LOAD httpfs; LOAD ducklake;
             CREATE SECRET seed (TYPE s3, PROVIDER config, KEY_ID '{}', SECRET '{}', REGION '{REGION}',
                 ENDPOINT '{}', URL_STYLE 'path', USE_SSL false);",
            extension_directory().display(),
            self.credentials.access_key_id,
            self.credentials.secret_access_key,
            self.endpoint
        ))
        .expect("set up the seeding connection");
        conn
    }
}
