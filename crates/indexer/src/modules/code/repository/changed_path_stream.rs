use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChangedPath {
    pub path: String,
    pub status: ChangeStatus,
    #[serde(default)]
    pub old_path: String,
    pub new_mode: u32,
    #[serde(default)]
    pub old_mode: u32,
    pub old_blob_id: String,
    pub new_blob_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChangeStatus {
    Deleted,
    Renamed,
    Added,
    Modified,
    Copied,
    TypeChange,
    #[serde(other)]
    Unknown,
}

type LineStream = Pin<Box<dyn Stream<Item = Result<ChangedPath, ChangedPathDecodeError>> + Send>>;

pub struct ChangedPathStream {
    lines: LineStream,
}

impl ChangedPathStream {
    pub fn new<S, E>(stream: Pin<Box<S>>) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + ?Sized + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let reader = StreamReader::new(stream.map(|r| r.map_err(std::io::Error::other)));
        let codec = LinesCodec::new_with_max_length(1024 * 1024);
        let lines = FramedRead::new(reader, codec).filter_map(|result| async {
            match result {
                Ok(line) if line.is_empty() => None,
                Ok(line) => Some(serde_json::from_str::<ChangedPath>(&line).map_err(|e| {
                    ChangedPathDecodeError(format!("failed to parse changed path: {e}"))
                })),
                Err(e) => Some(Err(ChangedPathDecodeError(format!("line read error: {e}")))),
            }
        });

        Self {
            lines: Box::pin(lines),
        }
    }

    pub async fn next_path(&mut self) -> Result<Option<ChangedPath>, ChangedPathDecodeError> {
        self.lines.next().await.transpose()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ChangedPathDecodeError(String);

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_from_str(body: &str) -> ChangedPathStream {
        let bytes = Bytes::from(body.to_string());
        let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(futures::stream::once(async move { Ok(bytes) }));
        ChangedPathStream::new(stream)
    }

    async fn collect_paths(body: &str) -> Result<Vec<ChangedPath>, ChangedPathDecodeError> {
        let mut stream = stream_from_str(body);
        let mut paths = Vec::new();
        while let Some(path) = stream.next_path().await? {
            paths.push(path);
        }
        Ok(paths)
    }

    #[tokio::test]
    async fn parses_single_line() {
        let body = r#"{"path":"src/main.rs","status":"ADDED","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"abc123"}"#;
        let result = collect_paths(body).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "src/main.rs");
        assert_eq!(result[0].status, ChangeStatus::Added);
        assert_eq!(result[0].new_blob_id, "abc123");
    }

    #[tokio::test]
    async fn parses_multiple_lines() {
        let body = concat!(
            r#"{"path":"a.rs","status":"ADDED","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"aaa"}"#,
            "\n",
            r#"{"path":"b.rs","status":"MODIFIED","old_path":"","new_mode":33188,"old_blob_id":"bbb","new_blob_id":"ccc"}"#,
        );
        let result = collect_paths(body).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].status, ChangeStatus::Added);
        assert_eq!(result[1].status, ChangeStatus::Modified);
    }

    #[tokio::test]
    async fn skips_empty_lines() {
        let body = "\n\n";
        let result = collect_paths(body).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn parses_renamed_with_old_path() {
        let body = r#"{"path":"new.rs","status":"RENAMED","old_path":"old.rs","new_mode":33188,"old_blob_id":"aaa","new_blob_id":"aaa"}"#;
        let result = collect_paths(body).await.unwrap();

        assert_eq!(result[0].status, ChangeStatus::Renamed);
        assert_eq!(result[0].old_path, "old.rs");
    }

    #[tokio::test]
    async fn unknown_status_deserializes() {
        let body = r#"{"path":"a.rs","status":"SOMETHING_NEW","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"aaa"}"#;
        let result = collect_paths(body).await.unwrap();

        assert_eq!(result[0].status, ChangeStatus::Unknown);
    }

    #[tokio::test]
    async fn parses_all_known_statuses() {
        for (json_status, expected) in [
            ("DELETED", ChangeStatus::Deleted),
            ("RENAMED", ChangeStatus::Renamed),
            ("ADDED", ChangeStatus::Added),
            ("MODIFIED", ChangeStatus::Modified),
            ("COPIED", ChangeStatus::Copied),
            ("TYPE_CHANGE", ChangeStatus::TypeChange),
        ] {
            let body = format!(
                r#"{{"path":"a","status":"{json_status}","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"x"}}"#
            );
            let result = collect_paths(&body).await.unwrap();
            assert_eq!(result[0].status, expected, "failed for {json_status}");
        }
    }

    #[tokio::test]
    async fn handles_line_split_across_chunks() {
        let line = r#"{"path":"a.rs","status":"ADDED","old_path":"","new_mode":33188,"old_blob_id":"","new_blob_id":"aaa"}"#;
        let full = format!("{line}\n");
        let mid = full.len() / 2;

        let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(futures::stream::iter(vec![
                Ok(Bytes::from(full[..mid].to_string())),
                Ok(Bytes::from(full[mid..].to_string())),
            ]));
        let mut decoder = ChangedPathStream::new(stream);
        let path = decoder.next_path().await.unwrap().unwrap();

        assert_eq!(path.path, "a.rs");
        assert_eq!(path.status, ChangeStatus::Added);
    }
}
