// ██╗  ██╗███╗   ██╗ ██████╗ ██╗    ██╗██╗     ███████╗██████╗  ██████╗ ███████╗
// ██║ ██╔╝████╗  ██║██╔═══██╗██║    ██║██║     ██╔════╝██╔══██╗██╔════╝ ██╔════╝
// █████╔╝ ██╔██╗ ██║██║   ██║██║ █╗ ██║██║     █████╗  ██║  ██║██║  ███╗█████╗
// ██╔═██╗ ██║╚██╗██║██║   ██║██║███╗██║██║     ██╔══╝  ██║  ██║██║   ██║██╔══╝
// ██║  ██╗██║ ╚████║╚██████╔╝╚███╔███╔╝███████╗███████╗██████╔╝╚██████╔╝███████╗
// ╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝  ╚══╝╚══╝ ╚══════╝╚══════╝╚═════╝  ╚═════╝ ╚══════╝
//
//  ██████╗ ██████╗  █████╗ ██████╗ ██╗  ██╗
// ██╔════╝ ██╔══██╗██╔══██╗██╔══██╗██║  ██║
// ██║  ███╗██████╔╝███████║██████╔╝███████║
// ██║   ██║██╔══██╗██╔══██║██╔═══╝ ██╔══██║
// ╚██████╔╝██║  ██║██║  ██║██║     ██║  ██║
//  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝

use futures::StreamExt;
use futures::stream::BoxStream;
use log::info;
use parser_core::parser::detect_language_from_extension;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::analysis::{AnalysisService, types::GraphData};

use crate::loading::{FileInfo, FileSource, ProcessingError, read_text_file};
use crate::parsing::processor::FileProcessor;

pub use crate::parsing::processor::{
    ErroredFile, FileProcessingResult, ProcessingStage, ProcessingStats, SkippedFile,
};

type ParseFilesResult = (
    Vec<FileProcessingResult>,
    Vec<SkippedFile>,
    Vec<ErroredFile>,
    Vec<(String, String)>,
);

#[derive(Debug)]
enum IndexingProcessingResult {
    Success(FileProcessingResult),
    Skipped(SkippedFile),
    Error(ErroredFile),
}

#[derive(Debug, Clone, Copy)]
pub struct IndexingConfig {
    pub worker_threads: usize,
    pub max_file_size: usize,
    pub respect_gitignore: bool,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            worker_threads: 0,
            max_file_size: 5_000_000,
            respect_gitignore: true,
        }
    }
}

pub struct RepositoryIndexingResult {
    pub total_processing_time: Duration,
    pub repository_name: String,
    pub repository_path: String,
    pub skipped_files: Vec<SkippedFile>,
    pub errored_files: Vec<ErroredFile>,
    pub errors: Vec<(String, String)>,
    pub graph_data: Option<GraphData>,
    pub database_path: Option<String>,
    pub database_loaded: bool,
}

#[derive(Debug)]
pub enum AnalyzeAndWriteErrors {
    FailedToLoadDatabase(String),
    FailedToAnalyze(String),
    FailedToWrite(String),
}

#[derive(Debug)]
pub enum FatalIndexingError {
    FailedToGetFiles(String),
    FailedToProcessFiles(String),
    FailedToAnalyze(AnalyzeAndWriteErrors),
    FailedToWrite(AnalyzeAndWriteErrors),
    FailedToLoadDatabase(AnalyzeAndWriteErrors),
    FailedToSyncChanges(String),
}

impl std::fmt::Display for FatalIndexingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FatalIndexingError::FailedToGetFiles(msg) => write!(f, "Failed to get files: {msg}"),
            FatalIndexingError::FailedToProcessFiles(msg) => {
                write!(f, "Failed to process files: {msg}")
            }
            FatalIndexingError::FailedToAnalyze(err) => write!(f, "Failed to analyze: {err:?}"),
            FatalIndexingError::FailedToWrite(err) => write!(f, "Failed to write: {err:?}"),
            FatalIndexingError::FailedToLoadDatabase(err) => {
                write!(f, "Failed to load database: {err:?}")
            }
            FatalIndexingError::FailedToSyncChanges(msg) => {
                write!(f, "Failed to sync changes: {msg}")
            }
        }
    }
}

impl std::error::Error for FatalIndexingError {}

pub struct RepositoryIndexer {
    pub name: String,
    pub path: String,
    graph_identity: Option<GraphIdentity>,
}

#[derive(Debug, Clone)]
struct GraphIdentity {
    project_id: i64,
    branch: String,
}

impl RepositoryIndexer {
    pub fn new(name: String, path: String) -> Self {
        Self {
            name,
            path,
            graph_identity: None,
        }
    }

    pub fn with_name(name: String, path: String) -> Self {
        Self::new(name, path)
    }

    pub fn with_graph_identity(
        name: String,
        path: String,
        project_id: i64,
        branch: String,
    ) -> Self {
        Self {
            name,
            path,
            graph_identity: Some(GraphIdentity { project_id, branch }),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Index files from a streaming file source.
    ///
    /// Files are processed as they are discovered - no upfront collection.
    pub async fn index_files<F: FileSource>(
        &self,
        file_source: F,
        config: &IndexingConfig,
    ) -> Result<RepositoryIndexingResult, FatalIndexingError> {
        let start_time = Instant::now();
        info!("Starting repository indexing for: {}", self.name);

        // Stream files directly into the parse pipeline
        let file_stream = file_source.stream_files(config);

        let (file_results, skipped_files, errored_files, errors) =
            self.parse_file_stream(file_stream, config).await?;

        let file_results_len = file_results.len();
        let total_files = file_results_len + skipped_files.len() + errored_files.len();

        let analysis_service = AnalysisService::new(self.name.clone(), self.path.clone());

        let mut graph_data = analysis_service
            .analyze_results(file_results)
            .map_err(|e| {
                FatalIndexingError::FailedToAnalyze(AnalyzeAndWriteErrors::FailedToAnalyze(
                    e.to_string(),
                ))
            })?;
        if let Some(graph_identity) = &self.graph_identity {
            graph_data.assign_node_ids(graph_identity.project_id, &graph_identity.branch);
        }

        let skipped_files_len = skipped_files.len();
        let errored_files_len = errored_files.len();

        let indexing_result = RepositoryIndexingResult {
            total_processing_time: start_time.elapsed(),
            repository_name: self.name.clone(),
            repository_path: self.path.clone(),
            skipped_files,
            errored_files,
            errors,
            graph_data: Some(graph_data),
            database_path: None,
            database_loaded: false,
        };

        info!(
            "✅ Repository indexing completed for '{}' in {:?}",
            self.name, indexing_result.total_processing_time
        );

        if total_files > 0 {
            info!(
                "📊 Final results: {:.1}% complete - {} processed, {} skipped, {} errors",
                (file_results_len as f64 / total_files as f64) * 100.0,
                file_results_len,
                skipped_files_len,
                errored_files_len
            );
        }

        Ok(indexing_result)
    }

    /// Parse files from a stream, processing them as they arrive.
    ///
    /// This is the core streaming pipeline:
    /// 1. Files stream in from directory walker
    /// 2. Async file read (bounded concurrency)
    /// 3. CPU-bound parsing (bounded by semaphore)
    async fn parse_file_stream<E: std::fmt::Display + Send + Sync + 'static>(
        &self,
        file_stream: BoxStream<'static, Result<FileInfo, E>>,
        config: &IndexingConfig,
    ) -> Result<ParseFilesResult, FatalIndexingError> {
        // Calculate optimal worker count
        let num_cores = num_cpus::get();
        let worker_count = if config.worker_threads == 0 {
            std::cmp::max(num_cores, 4)
        } else {
            config.worker_threads
        };

        info!("Using {worker_count} CPU workers (spawn_blocking)");
        let io_concurrency = std::cmp::max(worker_count * 2, 8);
        let cpu_sem = Arc::new(Semaphore::new(worker_count));

        // Collect results
        let mut file_results = Vec::new();
        let mut skipped_files = Vec::new();
        let mut errored_files = Vec::new();
        let mut errors = Vec::new();

        let repo_path = self.path.clone();
        let max_file_size = config.max_file_size;
        let start_time = Instant::now();
        let mut last_progress_log = Instant::now();
        let mut files_discovered = 0usize;

        // Stream pipeline: discovery -> read -> parse
        let pipeline = file_stream
            // Handle file discovery errors
            .filter_map(|result| async {
                match result {
                    Ok(file_info) => Some(file_info),
                    Err(e) => {
                        // Log discovery errors but continue
                        log::warn!("File discovery error: {}", e);
                        None
                    }
                }
            })
            // Prepare full paths
            .map(move |file_info| {
                let file_path_str = file_info.path.to_string_lossy().to_string();
                let full_path = if file_path_str.starts_with(&repo_path) {
                    file_info.path.to_path_buf()
                } else {
                    Path::new(&repo_path).join(&file_info.path)
                };
                (file_info, full_path)
            })
            // Async file read with bounded concurrency
            .map(move |(file_info, full_path)| async move {
                let content_res = read_text_file(&full_path, max_file_size).await;
                (file_info, content_res)
            })
            .buffer_unordered(io_concurrency)
            // CPU-bound parsing with semaphore
            .map(|(file_info, content_res)| {
                let cpu_sem = Arc::clone(&cpu_sem);
                async move {
                    match content_res {
                        Ok(content) => {
                            let _permit = cpu_sem.acquire_owned().await.expect("semaphore closed");

                            let parse_res = tokio_rayon::spawn(move || {
                                let processor = FileProcessor::from_file_info(file_info, &content);
                                processor.process()
                            })
                            .await;

                            match parse_res {
                                crate::parsing::processor::ProcessingResult::Success(
                                    file_result,
                                ) => IndexingProcessingResult::Success(file_result),
                                crate::parsing::processor::ProcessingResult::Skipped(skipped) => {
                                    IndexingProcessingResult::Skipped(skipped)
                                }
                                crate::parsing::processor::ProcessingResult::Error(errored) => {
                                    IndexingProcessingResult::Error(ErroredFile {
                                        file_path: errored.file_path,
                                        language: errored.language,
                                        error_message: format!(
                                            "Parse error: {:?}",
                                            errored.error_message
                                        ),
                                        error_stage: ProcessingStage::Unknown,
                                    })
                                }
                            }
                        }
                        Err(processing_error) => match processing_error {
                            ProcessingError::Skipped(file_path, reason) => {
                                IndexingProcessingResult::Skipped(SkippedFile {
                                    file_path,
                                    reason,
                                    file_size: None,
                                })
                            }
                            ProcessingError::Error(file_path, error_msg) => {
                                IndexingProcessingResult::Error(ErroredFile {
                                    file_path,
                                    language: detect_language_from_extension(file_info.extension())
                                        .ok(),
                                    error_message: error_msg,
                                    error_stage: ProcessingStage::FileSystem,
                                })
                            }
                        },
                    }
                }
            })
            .buffer_unordered(worker_count);

        tokio::pin!(pipeline);

        while let Some(result) = pipeline.next().await {
            files_discovered += 1;

            match result {
                IndexingProcessingResult::Success(file_result) => {
                    file_results.push(file_result);
                }
                IndexingProcessingResult::Skipped(skipped) => {
                    skipped_files.push(skipped);
                }
                IndexingProcessingResult::Error(errored) => {
                    errors.push((errored.file_path.clone(), errored.error_message.clone()));
                    errored_files.push(errored);
                }
            }

            // Log progress every 2 seconds
            if last_progress_log.elapsed() >= Duration::from_secs(2) {
                let elapsed = start_time.elapsed();
                let files_per_sec = files_discovered as f64 / elapsed.as_secs_f64();
                info!(
                    "🔄 Streaming: {} files - {:.1} files/sec - {} processed, {} skipped, {} errors",
                    files_discovered,
                    files_per_sec,
                    file_results.len(),
                    skipped_files.len(),
                    errored_files.len()
                );
                last_progress_log = Instant::now();
            }
        }

        info!(
            "✅ Streaming pipeline completed: {} processed, {} skipped, {} errors ({} total)",
            file_results.len(),
            skipped_files.len(),
            errored_files.len(),
            files_discovered
        );

        Ok((file_results, skipped_files, errored_files, errors))
    }
}
