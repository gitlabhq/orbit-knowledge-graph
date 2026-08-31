use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::StreamExt;
use gitaly_protos::proto::ListBlobsResponse;
use gitlab_client::GitlabClient;
use orbit_server_config::GitlabClientConfiguration;
use prost::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = GitlabClient::new(GitlabClientConfiguration {
        base_url: "http://127.0.0.1:3000".into(),
        signing_key: BASE64.encode("unused-for-poc-path"),
        resolve_host: None,
    })?;
    let mut stream = client.list_blobs(4, &["HEAD".into()]).await?;
    let mut responses = 0;
    let mut entries = 0;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        let length = u32::from_be_bytes(frame[..4].try_into()?) as usize;
        let response = ListBlobsResponse::decode(&frame[4..4 + length])?;
        responses += 1;
        entries += response.blobs.len();
    }
    println!(
        "ORBIT LIST_BLOBS SEAM PASS: GitlabClient::list_blobs project_id=4 responses={responses} entries={entries}"
    );
    Ok(())
}
