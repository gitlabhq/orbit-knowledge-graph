use std::env;
use std::time::Duration;

use gitaly_protos::proto::blob_service_client::BlobServiceClient;
use gitaly_protos::proto::repository_service_client::RepositoryServiceClient;
use gitaly_protos::proto::{
    GetArchiveRequest, ListBlobsRequest, RemoveRepositoryRequest, Repository,
    RepositoryExistsRequest, get_archive_request,
};
use gitaly_protos::websocket::{GRANT_HEADER, connect_channel};
use tonic::metadata::MetadataValue;
use tonic::{Code, Request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        return Err("usage: ws_e2e <ws-url> <grant> <storage> <repo> <other-repo>".into());
    }
    let url = &args[1];
    let grant: MetadataValue<_> = args[2].parse()?;
    let storage = &args[3];
    let repo = repository(storage, &args[4]);
    let other_repo = repository(storage, &args[5]);

    let channel = connect_channel(url).await?;
    let mut repository_client = RepositoryServiceClient::new(channel.clone());
    let mut blob_client = BlobServiceClient::new(channel);

    let response = repository_client
        .repository_exists(authorize(
            RepositoryExistsRequest {
                repository: Some(repo.clone()),
            },
            &grant,
        ))
        .await?
        .into_inner();
    println!("[1 unary] RepositoryExists exists={}", response.exists);

    let mut blobs = blob_client
        .list_blobs(authorize(
            ListBlobsRequest {
                repository: Some(repo.clone()),
                revisions: vec!["HEAD".into()],
                limit: 0,
                bytes_limit: 128,
                with_paths: true,
            },
            &grant,
        ))
        .await?
        .into_inner();
    let mut blob_responses = 0;
    let mut blob_entries = 0;
    while let Some(response) = blobs.message().await? {
        blob_responses += 1;
        blob_entries += response.blobs.len();
    }
    println!("[2 server-streaming] ListBlobs responses={blob_responses} entries={blob_entries}");

    let mut archive = repository_client
        .get_archive(authorize(
            GetArchiveRequest {
                repository: Some(repo.clone()),
                commit_id: "HEAD".into(),
                prefix: "e2e/".into(),
                format: get_archive_request::Format::Tar as i32,
                path: b".".to_vec(),
                exclude: Vec::new(),
                elide_path: false,
                include_lfs_blobs: false,
            },
            &grant,
        ))
        .await?
        .into_inner();
    let mut archive_frames = 0;
    let mut archive_bytes = 0;
    while let Some(response) = archive.message().await? {
        archive_frames += 1;
        archive_bytes += response.data.len();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    println!(
        "[3 large/backpressure] GetArchive bytes={archive_bytes} frames={archive_frames} delayed_each_frame=2ms"
    );

    let method_error = repository_client
        .remove_repository(authorize(
            RemoveRepositoryRequest {
                repository: Some(repo),
            },
            &grant,
        ))
        .await
        .expect_err("RemoveRepository must be denied");
    assert_eq!(method_error.code(), Code::PermissionDenied);
    println!(
        "[4 authz negative] code={:?} message={}",
        method_error.code(),
        method_error.message()
    );

    let scope_error = blob_client
        .list_blobs(authorize(
            ListBlobsRequest {
                repository: Some(other_repo),
                revisions: vec!["HEAD".into()],
                limit: 1,
                bytes_limit: 0,
                with_paths: false,
            },
            &grant,
        ))
        .await
        .expect_err("out-of-scope repository must be denied");
    assert_eq!(scope_error.code(), Code::PermissionDenied);
    println!(
        "[5 repo-scope negative] code={:?} message={}",
        scope_error.code(),
        scope_error.message()
    );

    println!("RESULT: PRIMARY RUST MATRIX PASS");
    Ok(())
}

fn authorize<T>(message: T, grant: &MetadataValue<tonic::metadata::Ascii>) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(GRANT_HEADER, grant.clone());
    request
}

fn repository(storage: &str, relative_path: &str) -> Repository {
    Repository {
        storage_name: storage.into(),
        relative_path: relative_path.into(),
        ..Default::default()
    }
}
