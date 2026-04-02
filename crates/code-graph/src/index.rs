use futures::StreamExt;
use futures::stream::BoxStream;
use log::info;
use parser_core::parser::detect_language_from_extension;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use code_graph_linker::analysis::{AnalysisService, types::GraphData};

use crate::fs::{FileInfo, FileSource, FsConfig, ProcessingError, read_text_file};
use crate::parse::{self, ErroredFile, FileProcessingResult, ProcessingStage, SkippedFile};

#[derive(Debug, Clone, Default)]
pub struct IndexConfig {
    pub fs: FsConfig,
    pub worker_threads: usize,
}

pub struct IndexResult {
    pub total_processing_time: Duration,
    pub repository_name: String,
    pub repository_path: String,
    pub skipped_files: Vec<SkippedFile>,
    pub errored_files: Vec<ErroredFile>,
    pub errors: Vec<(String, String)>,
    pub graph_data: Option<GraphData>,
}

#[derive(Debug)]
pub enum IndexError {
    FailedToGetFiles(String),
    FailedToProcessFiles(String),
    FailedToAnalyze(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::FailedToGetFiles(msg) => write!(f, "Failed to get files: {msg}"),
            IndexError::FailedToProcessFiles(msg) => {
                write!(f, "Failed to process files: {msg}")
            }
            IndexError::FailedToAnalyze(msg) => write!(f, "Failed to analyze: {msg}"),
        }
    }
}

impl std::error::Error for IndexError {}

#[derive(Debug, Clone)]
struct GraphIdentity {
    project_id: i64,
    branch: String,
}

pub struct RepositoryIndexer {
    pub name: String,
    pub path: String,
    graph_identity: Option<GraphIdentity>,
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

    /// fs → parse → link
    pub async fn index_files<F: FileSource>(
        &self,
        file_source: F,
        config: &IndexConfig,
    ) -> Result<IndexResult, IndexError> {
        let start_time = Instant::now();
        info!("Starting repository indexing for: {}", self.name);

        // --- fs ---
        let file_stream = file_source.stream_files(&config.fs);

        // --- parse (streaming) ---
        let (file_results, skipped_files, errored_files, errors) =
            self.run_parse_pipeline(file_stream, config).await?;

        let file_results_len = file_results.len();
        let total_files = file_results_len + skipped_files.len() + errored_files.len();

        // --- link ---
        let analysis_service = AnalysisService::new(self.name.clone(), self.path.clone());

        let mut graph_data = analysis_service
            .analyze_results(file_results)
            .map_err(|e| IndexError::FailedToAnalyze(e.to_string()))?;

        if let Some(identity) = &self.graph_identity {
            graph_data.assign_node_ids(identity.project_id, &identity.branch);
        }

        let result = IndexResult {
            total_processing_time: start_time.elapsed(),
            repository_name: self.name.clone(),
            repository_path: self.path.clone(),
            skipped_files,
            errored_files,
            errors,
            graph_data: Some(graph_data),
        };

        info!(
            "Repository indexing completed for '{}' in {:?}",
            self.name, result.total_processing_time
        );

        if total_files > 0 {
            info!(
                "Results: {:.1}% complete - {} processed, {} skipped, {} errors",
                (file_results_len as f64 / total_files as f64) * 100.0,
                file_results_len,
                result.skipped_files.len(),
                result.errored_files.len(),
            );
        }

        Ok(result)
    }

    async fn run_parse_pipeline<E: std::fmt::Display + Send + Sync + 'static>(
        &self,
        file_stream: BoxStream<'static, Result<FileInfo, E>>,
        config: &IndexConfig,
    ) -> Result<
        (
            Vec<FileProcessingResult>,
            Vec<SkippedFile>,
            Vec<ErroredFile>,
            Vec<(String, String)>,
        ),
        IndexError,
    > {
        let num_cores = num_cpus::get();
        let worker_count = if config.worker_threads == 0 {
            std::cmp::max(num_cores, 4)
        } else {
            config.worker_threads
        };

        info!("Using {worker_count} CPU workers");
        let io_concurrency = std::cmp::max(worker_count * 2, 8);
        let cpu_sem = Arc::new(Semaphore::new(worker_count));

        let mut file_results = Vec::new();
        let mut skipped_files = Vec::new();
        let mut errored_files = Vec::new();
        let mut errors = Vec::new();

        let repo_path = self.path.clone();
        let max_file_size = config.fs.max_file_size;
        let start_time = Instant::now();
        let mut last_progress_log = Instant::now();
        let mut files_discovered = 0usize;

        let pipeline = file_stream
            .filter_map(|result| async {
                match result {
                    Ok(file_info) => Some(file_info),
                    Err(e) => {
                        log::warn!("File discovery error: {}", e);
                        None
                    }
                }
            })
            .map(move |file_info| {
                let file_path_str = file_info.path.to_string_lossy().to_string();
                let full_path = if file_path_str.starts_with(&repo_path) {
                    file_info.path.to_path_buf()
                } else {
                    Path::new(&repo_path).join(&file_info.path)
                };
                (file_info, full_path)
            })
            .map(move |(file_info, full_path)| async move {
                let content_res = read_text_file(&full_path, max_file_size).await;
                (file_info, content_res)
            })
            .buffer_unordered(io_concurrency)
            .map(|(file_info, content_res)| {
                let cpu_sem = Arc::clone(&cpu_sem);
                async move {
                    match content_res {
                        Ok(content) => {
                            let _permit = cpu_sem.acquire_owned().await.expect("semaphore closed");

                            let parse_res = tokio_rayon::spawn(move || {
                                let path = file_info.path.to_string_lossy();
                                parse::parse(&path, &content)
                            })
                            .await;

                            match parse_res {
                                crate::parse::ProcessingResult::Success(file_result) => {
                                    PipelineResult::Success(file_result)
                                }
                                crate::parse::ProcessingResult::Skipped(skipped) => {
                                    PipelineResult::Skipped(skipped)
                                }
                                crate::parse::ProcessingResult::Error(errored) => {
                                    PipelineResult::Error(ErroredFile {
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
                                PipelineResult::Skipped(SkippedFile {
                                    file_path,
                                    reason,
                                    file_size: None,
                                })
                            }
                            ProcessingError::Error(file_path, error_msg) => {
                                PipelineResult::Error(ErroredFile {
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
                PipelineResult::Success(file_result) => {
                    file_results.push(file_result);
                }
                PipelineResult::Skipped(skipped) => {
                    skipped_files.push(skipped);
                }
                PipelineResult::Error(errored) => {
                    errors.push((errored.file_path.clone(), errored.error_message.clone()));
                    errored_files.push(errored);
                }
            }

            if last_progress_log.elapsed() >= Duration::from_secs(2) {
                let elapsed = start_time.elapsed();
                let files_per_sec = files_discovered as f64 / elapsed.as_secs_f64();
                info!(
                    "Streaming: {} files - {:.1} files/sec - {} processed, {} skipped, {} errors",
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
            "Pipeline completed: {} processed, {} skipped, {} errors ({} total)",
            file_results.len(),
            skipped_files.len(),
            errored_files.len(),
            files_discovered
        );

        Ok((file_results, skipped_files, errored_files, errors))
    }
}

#[derive(Debug)]
enum PipelineResult {
    Success(FileProcessingResult),
    Skipped(SkippedFile),
    Error(ErroredFile),
}
