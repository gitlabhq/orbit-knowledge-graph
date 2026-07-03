use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::Duration;

use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::DeleteStreamErrorKind;
use futures::TryStreamExt;
use tracing::{debug, info, warn};

use crate::dead_letter::DEAD_LETTER_STREAM;
use crate::indexing_status::INDEXING_PROGRESS_BUCKET;
use crate::locking::INDEXING_LOCKS_BUCKET;
use crate::schema::version::SCHEMA_VERSION;
use crate::topic::INDEXER_STREAM;
use crate::types::Subscription;

pub const MANAGED_STREAMS: &[&str] = &[INDEXER_STREAM, DEAD_LETTER_STREAM];

pub const MANAGED_BUCKETS: &[&str] = &[INDEXING_LOCKS_BUCKET, INDEXING_PROGRESS_BUCKET];

pub static NATS_VERSIONER: LazyLock<NatsVersioner> =
    LazyLock::new(|| NatsVersioner::new(release_segment(), *SCHEMA_VERSION));

static SANITIZE_RELEASE_PATTERN: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new("[^a-zA-Z0-9]+").expect("valid literal regex"));

fn sanitize_release(raw: &str) -> String {
    SANITIZE_RELEASE_PATTERN
        .replace_all(raw, "-")
        .trim_matches('-')
        .to_string()
}

fn release_segment() -> String {
    sanitize_release(gkg_utils::version::get())
}

pub struct NatsVersioner {
    release: String,
    schema_version: u32,
}

impl NatsVersioner {
    pub fn new(release: impl Into<String>, schema_version: u32) -> Self {
        Self {
            release: release.into(),
            schema_version,
        }
    }

    pub fn stream(&self, base: &str) -> String {
        format!("{base}_V{}", self.release)
    }

    pub fn bucket(&self, base: &str) -> String {
        format!("{base}_v{}", self.schema_version)
    }

    pub fn subject(&self, base: &str) -> String {
        format!("v{}.{base}", self.release)
    }

    pub fn tag(&self) -> String {
        format!("v{}", self.release)
    }

    pub fn release(&self) -> &str {
        &self.release
    }

    pub fn resolve_stream_and_subject(&self, subscription: &Subscription) -> (String, String) {
        if subscription.manage_stream {
            (
                self.stream(&subscription.stream),
                self.subject(&subscription.subject),
            )
        } else {
            (
                subscription.stream.to_string(),
                subscription.subject.to_string(),
            )
        }
    }
}

pub fn release_from_stream_name(name: &str) -> Option<&str> {
    MANAGED_STREAMS.iter().find_map(|base| {
        name.strip_prefix(base)
            .and_then(|rest| rest.strip_prefix("_V"))
            .filter(|release| !release.is_empty())
    })
}

pub async fn gc_idle_release_streams(
    client: &async_nats::Client,
    threshold: Duration,
) -> Result<(), CleanupError> {
    let cutoff = chrono::Utc::now()
        .timestamp()
        .saturating_sub(i64::try_from(threshold.as_secs()).unwrap_or(i64::MAX));

    let mut streams_by_release: HashMap<String, Vec<String>> = HashMap::new();
    let mut alive: HashSet<String> = HashSet::new();
    let mut streams = async_nats::jetstream::new(client.clone()).streams();

    // A listing error must abort before any deletion: sweeping on a partial
    // survey could miss the stream carrying a live release's activity.
    while let Some(info) = streams
        .try_next()
        .await
        .map_err(|e| CleanupError(vec![format!("failed to list streams: {e}")]))?
    {
        let Some(release) = release_from_stream_name(&info.config.name) else {
            continue;
        };
        if release == NATS_VERSIONER.release()
            || info.created.unix_timestamp() > cutoff
            || info.state.last_timestamp.unix_timestamp() > cutoff
            || info.state.consumer_count > 0
        {
            alive.insert(release.to_string());
        }
        streams_by_release
            .entry(release.to_string())
            .or_default()
            .push(info.config.name);
    }

    let mut errors: Vec<String> = Vec::new();
    for (release, names) in &streams_by_release {
        if alive.contains(release) {
            continue;
        }
        info!(release, "release GC: deleting idle release streams");
        if let Err(CleanupError(mut stream_errors)) = delete_streams(client, names, release).await {
            errors.append(&mut stream_errors);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupError(errors))
    }
}

pub fn code_work_stream_name() -> String {
    NATS_VERSIONER.stream(INDEXER_STREAM)
}

pub fn code_work_consumer_name(consumer_name: &str) -> String {
    let versioned_subject =
        NATS_VERSIONER.subject(crate::topic::CODE_INDEXING_TASK_SUBJECT_PATTERN);
    format!(
        "{consumer_name}-{}",
        super::broker::escape_subject_for_durable(&versioned_subject)
    )
}

fn schema_bucket_stream_names(schema_version: u32) -> Vec<String> {
    let versioner = NatsVersioner::new("", schema_version);
    MANAGED_BUCKETS
        .iter()
        .map(|base| format!("KV_{}", versioner.bucket(base)))
        .collect()
}

async fn delete_streams(
    nats_client: &async_nats::Client,
    names: &[String],
    context: &str,
) -> Result<(), CleanupError> {
    let jetstream = async_nats::jetstream::new(nats_client.clone());
    let mut errors: Vec<String> = Vec::new();

    for name in names {
        match jetstream.delete_stream(name).await {
            Ok(_) => info!(context, stream = %name, "deleted versioned stream"),
            Err(e)
                if matches!(
                    e.kind(),
                    DeleteStreamErrorKind::JetStream(js_err)
                        if js_err.kind() == ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                debug!(context, stream = %name, "versioned stream already deleted, skipping");
            }
            Err(e) => {
                warn!(context, stream = %name, error = %e, "failed to delete versioned stream");
                errors.push(format!("stream {name}: {e}"));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupError(errors))
    }
}

pub async fn cleanup_schema_state(
    nats_client: &async_nats::Client,
    schema_version: u32,
) -> Result<(), CleanupError> {
    delete_streams(
        nats_client,
        &schema_bucket_stream_names(schema_version),
        &format!("schema_v{schema_version}"),
    )
    .await
}

#[derive(Debug)]
pub struct CleanupError(Vec<String>);

impl std::fmt::Display for CleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to clean up NATS entities: {}", self.0.join(", "))
    }
}

impl std::error::Error for CleanupError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Subscription;

    #[test]
    fn versioner_formats_messaging_by_release_and_state_by_schema() {
        let v = NatsVersioner::new("0-84-1", 77);

        assert_eq!(v.stream("GKG_INDEXER"), "GKG_INDEXER_V0-84-1");
        assert_eq!(
            v.subject("sdlc.global.indexing.requested"),
            "v0-84-1.sdlc.global.indexing.requested"
        );
        assert_eq!(v.tag(), "v0-84-1");
        assert_eq!(v.bucket("indexing_locks"), "indexing_locks_v77");
    }

    #[test]
    fn sanitize_release_collapses_non_alphanumeric_runs() {
        assert_eq!(sanitize_release("0.84.1"), "0-84-1");
        assert_eq!(sanitize_release("0.0.0-dev"), "0-0-0-dev");
        assert_eq!(sanitize_release("0.84.1-3-gabcdef"), "0-84-1-3-gabcdef");
        assert_eq!(sanitize_release("v1.2..3"), "v1-2-3");
    }

    #[test]
    fn global_versioner_keys_state_by_schema_version() {
        let v = *SCHEMA_VERSION;
        assert_eq!(
            NATS_VERSIONER.bucket(INDEXING_LOCKS_BUCKET),
            format!("{INDEXING_LOCKS_BUCKET}_v{v}")
        );
    }

    #[test]
    fn global_versioner_keys_messaging_by_release() {
        let expected = release_segment();
        assert_eq!(
            NATS_VERSIONER.stream(INDEXER_STREAM),
            format!("{INDEXER_STREAM}_V{expected}")
        );
    }

    #[test]
    fn schema_bucket_stream_names_cover_state_entities_only() {
        let names = schema_bucket_stream_names(62);

        assert!(names.contains(&"KV_indexing_locks_v62".to_string()));
        assert!(names.contains(&"KV_orbit_indexing_progress_v62".to_string()));
        assert_eq!(names.len(), MANAGED_BUCKETS.len());
    }

    #[test]
    fn release_from_stream_name_parses_managed_streams_only() {
        assert_eq!(
            release_from_stream_name("GKG_INDEXER_V0-84-1"),
            Some("0-84-1")
        );
        assert_eq!(
            release_from_stream_name("GKG_DEAD_LETTERS_V0-0-0-dev"),
            Some("0-0-0-dev")
        );
        assert_eq!(release_from_stream_name("GKG_INDEXER_V"), None);
        assert_eq!(release_from_stream_name("GKG_INDEXER"), None);
        assert_eq!(release_from_stream_name("siphon_stream_main_db"), None);
        assert_eq!(release_from_stream_name("KV_indexing_locks_v62"), None);
        assert_eq!(release_from_stream_name("OTHER_APP_V67"), None);
    }

    #[test]
    fn cleanup_name_sets_exclude_foreign_entities() {
        let buckets = schema_bucket_stream_names(54);

        assert!(!buckets.contains(&"KV_someone_else_v54".to_string()));
        assert!(!buckets.contains(&"siphon_db".to_string()));
    }

    #[test]
    fn resolve_stream_and_subject_versions_managed_subscriptions() {
        let v = NatsVersioner::new("0-84-1", 77);
        let subscription = Subscription::new("GKG_INDEXER", "sdlc.global.indexing.requested");

        let (stream, subject) = v.resolve_stream_and_subject(&subscription);

        assert_eq!(stream, "GKG_INDEXER_V0-84-1");
        assert_eq!(subject, "v0-84-1.sdlc.global.indexing.requested");
    }

    #[test]
    fn resolve_stream_and_subject_preserves_unmanaged_subscriptions() {
        let v = NatsVersioner::new("0-84-1", 77);
        let mut subscription = Subscription::new("siphon_db", "tables.merge_requests");
        subscription.manage_stream = false;

        let (stream, subject) = v.resolve_stream_and_subject(&subscription);

        assert_eq!(stream, "siphon_db");
        assert_eq!(subject, "tables.merge_requests");
    }

    #[test]
    fn code_work_stream_name_is_release_versioned() {
        assert_eq!(
            super::code_work_stream_name(),
            format!("{INDEXER_STREAM}_V{}", release_segment())
        );
    }

    #[test]
    fn code_work_consumer_name_matches_handler_durable() {
        let release = release_segment();
        assert_eq!(
            super::code_work_consumer_name("gkg-indexer"),
            format!("gkg-indexer-v{release}-code-task-indexing-requested-wildcard-wildcard")
        );
    }
}
