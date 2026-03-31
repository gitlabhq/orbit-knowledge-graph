use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub enum ProcessingError {
    Skipped(String, String), // file_path, reason
    Error(String, String),   // file_path, error_message
}

/// Read a text file efficiently with size checks.
///
/// - Opens the file once and inspects metadata from the handle
pub async fn read_text_file(
    full_path: &Path,
    max_file_size: usize,
) -> Result<String, ProcessingError> {
    let file_path = full_path.to_string_lossy().to_string();

    // Open file and inspect metadata from the handle
    let mut file = File::open(full_path).await.map_err(|e| {
        ProcessingError::Error(file_path.clone(), format!("Failed to open file: {e}"))
    })?;

    let metadata = file.metadata().await.map_err(|e| {
        ProcessingError::Error(file_path.clone(), format!("Failed to read metadata: {e}"))
    })?;

    let file_len = metadata.len() as usize;
    if file_len > max_file_size {
        return Err(ProcessingError::Skipped(
            file_path,
            format!("File too large: {} bytes", metadata.len()),
        ));
    }

    if file_len == 0 {
        return Ok(String::new());
    }

    // Read the entire file into a buffer
    let mut bytes = Vec::with_capacity(file_len);
    file.read_to_end(&mut bytes).await.map_err(|e| {
        ProcessingError::Error(file_path.clone(), format!("Failed to read file: {e}"))
    })?;

    // Validate UTF-8; skip if not valid text
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(_) => Err(ProcessingError::Skipped(
            file_path,
            "Non-UTF-8 content".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_read_text_file_ok() {
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "hello world").unwrap();

        let content = read_text_file(file.path(), 1024)
            .await
            .expect("should read");
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn test_read_text_file_empty() {
        let file = NamedTempFile::new().unwrap();
        let content = read_text_file(file.path(), 1024).await.expect("empty ok");
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn test_read_text_file_too_large() {
        let file = NamedTempFile::new().unwrap();
        let data = vec![b'a'; 2048];
        std::fs::write(file.path(), &data).unwrap();

        let err = read_text_file(file.path(), 1024)
            .await
            .expect_err("should error");
        match err {
            ProcessingError::Skipped(_path, reason) => {
                assert!(reason.contains("File too large"));
            }
            _ => panic!("unexpected error type"),
        }
    }
}
