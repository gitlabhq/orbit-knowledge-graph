use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clap::Parser;
use gkg_server::proto::execute_query_message::Content;
use gkg_server::proto::knowledge_graph_service_client::KnowledgeGraphServiceClient;
use gkg_server::proto::redaction_exchange::Content as RedactionContent;
use gkg_server::proto::{
    ExecuteQueryMessage, ExecuteQueryRequest, RedactionExchange, RedactionResponse,
    ResourceAuthorization, ResponseFormat,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Parser)]
#[command(name = "gkg-query")]
#[command(about = "Execute GKG JSON DSL queries against a running gRPC server")]
struct Cli {
    /// gRPC server address
    #[arg(long, env = "GKG_GRPC_URL", default_value = "http://127.0.0.1:50054")]
    server: String,

    /// JWT signing key (base64-encoded). Must match the server's verifying key.
    #[arg(long, env = "GKG_JWT_KEY")]
    jwt_key: String,

    /// Traversal paths for the security context (e.g., "1/2/3/").
    /// Use "1/" for org-wide admin access.
    #[arg(long, short, required = true, num_args = 1..)]
    traversal_paths: Vec<String>,

    /// Auto-approve all redaction checks (allow everything through)
    #[arg(long, default_value = "true")]
    approve_all: bool,

    /// Forge JWT without admin flag (use group_traversal_ids for security)
    #[arg(long)]
    no_admin: bool,

    /// JSON DSL query string
    query: String,
}

fn forge_jwt(key_b64: &str, traversal_paths: &[String], admin: bool) -> Result<String> {
    let raw_key = STANDARD
        .decode(key_b64.trim())
        .context("JWT key is not valid base64")?;

    let org_id: u64 = traversal_paths
        .first()
        .and_then(|p| p.split('/').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "sub": "gkg-query-cli",
        "iss": "gitlab",
        "aud": "gitlab-knowledge-graph",
        "iat": now,
        "exp": now + 3600,
        "user_id": 1,
        "username": "gkg-query-cli",
        "admin": admin,
        "organization_id": org_id,
        "group_traversal_ids": traversal_paths,
    });

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&raw_key),
    )
    .context("failed to sign JWT")
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let token = forge_jwt(&cli.jwt_key, &cli.traversal_paths, !cli.no_admin)?;

    let mut client = KnowledgeGraphServiceClient::connect(cli.server.clone())
        .await
        .with_context(|| format!("failed to connect to {}", cli.server))?;

    let (tx, rx) = mpsc::channel::<ExecuteQueryMessage>(16);

    tx.send(ExecuteQueryMessage {
        content: Some(Content::Request(ExecuteQueryRequest {
            query: cli.query.clone(),
            format: ResponseFormat::Raw as i32,
            query_type: 0,
        })),
    })
    .await?;

    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);

    let response = client.execute_query(request).await?;
    let mut stream = response.into_inner();

    while let Some(msg) = stream.message().await? {
        match msg.content {
            Some(Content::Result(result)) => {
                if let Some(ref content) = result.content {
                    match content {
                        gkg_server::proto::execute_query_result::Content::ResultJson(json) => {
                            let parsed: serde_json::Value = serde_json::from_str(json)
                                .unwrap_or(serde_json::Value::String(json.clone()));
                            println!("{}", serde_json::to_string_pretty(&parsed)?);
                        }
                        gkg_server::proto::execute_query_result::Content::FormattedText(text) => {
                            println!("{text}");
                        }
                    }
                }
                if let Some(ref meta) = result.metadata {
                    eprintln!("query_type={} rows={}", meta.query_type, meta.row_count,);
                    for sql in &meta.raw_query_strings {
                        eprintln!("sql: {sql}");
                    }
                    if let Some(ref stats) = meta.clickhouse_stats {
                        let elapsed_ms = stats.elapsed_ns as f64 / 1_000_000.0;
                        eprintln!(
                            "rows_read={} read_bytes={} result_rows={} elapsed={:.1}ms",
                            stats.read_rows, stats.read_bytes, stats.result_rows, elapsed_ms,
                        );
                    }
                }
                break;
            }
            Some(Content::Error(err)) => {
                bail!("[{}] {}", err.code, err.message);
            }
            Some(Content::Redaction(exchange)) => {
                if cli.approve_all
                    && let Some(RedactionContent::Required(required)) = exchange.content
                {
                    let authorizations = required
                        .resources
                        .iter()
                        .map(|r| {
                            let authorized: HashMap<i64, bool> =
                                r.resource_ids.iter().map(|id| (*id, true)).collect();
                            ResourceAuthorization {
                                resource_type: r.resource_type.clone(),
                                authorized,
                            }
                        })
                        .collect();

                    tx.send(ExecuteQueryMessage {
                        content: Some(Content::Redaction(RedactionExchange {
                            content: Some(RedactionContent::Response(RedactionResponse {
                                result_id: required.result_id,
                                authorizations,
                            })),
                        })),
                    })
                    .await?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}
