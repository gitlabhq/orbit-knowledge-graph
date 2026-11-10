use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::DeleteStreamErrorKind;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("NATS cleanup failed: {}", errors.join(", "))]
pub struct NatsCleanupError {
    pub errors: Vec<String>,
}

pub async fn cleanup_schema_version_buckets(
    nats_client: &async_nats::Client,
    schema_version: u32,
    managed_bucket_names: &[&str],
) -> Result<(), NatsCleanupError> {
    let backing_stream_names: Vec<String> = managed_bucket_names
        .iter()
        .map(|bucket_name| format!("KV_{bucket_name}_v{schema_version}"))
        .collect();

    delete_nats_streams(nats_client, &backing_stream_names, schema_version).await
}

async fn delete_nats_streams(
    nats_client: &async_nats::Client,
    stream_names: &[String],
    schema_version: u32,
) -> Result<(), NatsCleanupError> {
    let jetstream = async_nats::jetstream::new(nats_client.clone());
    let mut errors: Vec<String> = Vec::new();

    for stream_name in stream_names {
        match jetstream.delete_stream(stream_name).await {
            Ok(_) => {
                orbit_log_fields::info!(
                    safe(SCHEMA_VERSION) = schema_version,
                    safe(NATS_STREAM) = stream_name.as_str(),
                    "deleted NATS stream"
                );
            }
            Err(error)
                if matches!(
                    error.kind(),
                    DeleteStreamErrorKind::JetStream(jetstream_error)
                        if jetstream_error.kind() == ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                orbit_log_fields::debug!(
                    safe(SCHEMA_VERSION) = schema_version,
                    safe(NATS_STREAM) = stream_name.as_str(),
                    "NATS stream already deleted"
                );
            }
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(SCHEMA_VERSION) = schema_version,
                    safe(NATS_STREAM) = stream_name.as_str(),
                    safe(ERROR_TYPE) = "delete_stream",
                    expanded(ERROR_MESSAGE) = %error,
                    "failed to delete NATS stream"
                );
                errors.push(format!("{stream_name}: {error}"));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(NatsCleanupError { errors })
    }
}
