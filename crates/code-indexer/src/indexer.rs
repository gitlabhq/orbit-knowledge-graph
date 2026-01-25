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

use futures::stream::{self, StreamExt};
use gitalisk_core::repository::gitalisk_repository::FileInfo;
use log::info;
use parser_core::parser::detect_language_from_extension;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// Simplified imports - file processing is now handled by the File module
use crate::analysis::{AnalysisService, types::GraphData};
// use crate::metrics::ActiveIndexingGuard;

use crate::parsing::processor::FileProcessor;
use crate::project::source::FileSource;
// use crate::writer::{WriterResult, WriterService};

pub use crate::parsing::processor::{
    ErroredFile, FileProcessingResult, ProcessingStage, ProcessingStats, SkippedFile,
};
use crate::project::io::{ProcessingError, read_text_file};

type ParseFilesResult = (
    Vec<FileProcessingResult>,
    Vec<SkippedFile>,
    Vec<ErroredFile>,
    Vec<(String, String)>,
);

// Removed legacy worker task struct in favor of pipelined processing

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
    pub errors: Vec<(String, String)>, // Kept for backward compatibility
    pub graph_data: Option<GraphData>,
    // pub writer_result: Option<WriterResult>,
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

pub struct RepositoryIndexer {
    pub name: String,
    pub path: String,
}

impl RepositoryIndexer {
    pub fn new(name: String, path: String) -> Self {
        Self { name, path }
    }

    pub fn with_name(name: String, path: String) -> Self {
        Self { name, path }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// FIXME: SEPARATE THIS INTO A SEPARATE MODULE/EXECUTOR
    pub async fn index_files<F: FileSource>(
        &self,
        file_source: F,
        config: &IndexingConfig,
    ) -> Result<RepositoryIndexingResult, FatalIndexingError> {
        // let _guard = ActiveIndexingGuard::new();
        // let _timer = metrics::INDEXING_DURATION_SECONDS.start_timer();

        let start_time = Instant::now();
        info!("Starting repository indexing for: {}", self.name);

        let files = file_source
            .get_files(config)
            .map_err(|e| FatalIndexingError::FailedToGetFiles(e.to_string()))?;

        let total_files = files.len();

        let (file_results, skipped_files, errored_files, errors) =
            self.parse_files(files, config).await?;

        let file_results_len = file_results.len();

        // // Analysis phase timing
        // let _analysis_timer = metrics::INDEXING_PHASE_DURATION_SECONDS
        //     .with_label_values(&["analysis"])
        //     .start_timer();

        let analysis_service = AnalysisService::new(self.name.clone(), self.path.clone());

        let graph_data = analysis_service
            .analyze_results(file_results)
            .map_err(|e| {
                FatalIndexingError::FailedToAnalyze(AnalyzeAndWriteErrors::FailedToAnalyze(
                    e.to_string(),
                ))
            })?;

        // drop(_analysis_timer);

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
            // writer_result: None,
            database_path: None,
            database_loaded: false,
        };

        // indexing_result.graph_data = Some(graph_data);
        // indexing_result.writer_result = Some(writer_result);

        info!(
            "✅ Repository indexing completed for '{}' in {:?}",
            self.name, indexing_result.total_processing_time
        );
        info!(
            "📊 Final results: {:.1}% complete - {} processed, {} skipped, {} errors",
            (file_results_len as f64 / total_files as f64) * 100.0,
            file_results_len,
            skipped_files_len,
            errored_files_len
        );

        Ok(indexing_result)
    }

    pub async fn parse_files(
        &self,
        files: Vec<FileInfo>,
        config: &IndexingConfig,
    ) -> Result<ParseFilesResult, FatalIndexingError> {
        // let _phase_timer = metrics::INDEXING_PHASE_DURATION_SECONDS
        //     .with_label_values(&["parsing"])
        //     .start_timer();

        if files.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
        }

        let total_files = files.len();
        info!("Processing {total_files} files");

        // Calculate optimal worker count
        let num_cores = num_cpus::get();
        let worker_count = if config.worker_threads == 0 {
            std::cmp::max(num_cores, 4)
        } else {
            config.worker_threads
        };

        info!("Using {worker_count} CPU workers (spawn_blocking)");
        // FIXME: make this configurable in the future
        let io_concurrency = std::cmp::max(worker_count * 2, 8);
        let cpu_sem = Arc::new(Semaphore::new(worker_count));

        // Collect results
        let mut file_results = Vec::with_capacity(total_files);
        let mut skipped_files = Vec::new();
        let mut errored_files = Vec::new();
        let mut errors = Vec::new();

        let repo_path = self.path.clone();
        let max_file_size = config.max_file_size;
        let start_time = Instant::now();
        let mut last_progress = 0usize;

        // Stage 1: async read -> Stage 2: CPU parse (bounded)
        // TODO: investigate splitting the pipeline into multiple modules
        // and turning the entire execution pipeline into a streaming model.
        let pipeline = stream::iter(files.into_iter().map(|file_info| {
            let file_path_str = file_info.path.to_string_lossy().to_string();
            let full_path = if file_path_str.starts_with(&repo_path) {
                file_info.path.to_path_buf()
            } else {
                Path::new(&repo_path).join(&file_info.path)
            };
            (file_info, full_path)
        }))
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
                        // Acquire CPU permit then parse in blocking pool
                        let _permit = cpu_sem.acquire_owned().await.expect("semaphore closed");

                        let parse_res = tokio_rayon::spawn(move || {
                            let processor = FileProcessor::from_file_info(file_info, &content);
                            processor.process()
                        })
                        .await;

                        match parse_res {
                            crate::parsing::processor::ProcessingResult::Success(file_result) => {
                                IndexingProcessingResult::Success(file_result)
                            }
                            crate::parsing::processor::ProcessingResult::Skipped(skipped) => {
                                IndexingProcessingResult::Skipped(skipped)
                            }
                            crate::parsing::processor::ProcessingResult::Error(errored) => {
                                IndexingProcessingResult::Error(ErroredFile {
                                    file_path: errored.file_path,
                                    language: errored.language,
                                    error_message: format!(
                                        "Task execution failed: {:?}",
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

            let completed = file_results.len() + skipped_files.len() + errored_files.len();
            let progress = (completed * 100) / total_files;
            if progress >= last_progress + 10 && progress <= 100 {
                let elapsed = start_time.elapsed();
                let files_per_sec = completed as f64 / elapsed.as_secs_f64();
                info!(
                    "🔄 Progress: {}% ({}/{} files) - {:.1} files/sec - {} processed, {} skipped, {} errors",
                    progress,
                    completed,
                    total_files,
                    files_per_sec,
                    file_results.len(),
                    skipped_files.len(),
                    errored_files.len()
                );
                last_progress = progress;
            }
        }

        let final_completed = file_results.len() + skipped_files.len() + errored_files.len();
        info!(
            "✅ Pipelined processing completed: {} processed, {} skipped, {} errors ({} total)",
            file_results.len(),
            skipped_files.len(),
            errored_files.len(),
            final_completed
        );

        // // Record file processing metrics by language
        // for file in &file_results {
        //     let language_str = format!("{:?}", file.language).to_lowercase();
        //     // metrics::FILES_PROCESSED_TOTAL
        //     //     .with_label_values(&[&language_str])
        //     //     .inc();
        // }

        // for errored in &errored_files {
        //     let language = match errored.language {
        //         Some(language) => format!("{language:?}").to_lowercase(),
        //         None => "unknown".to_string(),
        //     };

        //     // metrics::FILES_ERRORED_TOTAL
        //     //     .with_label_values(&[&language])
        //     //     .inc();
        // }

        Ok((file_results, skipped_files, errored_files, errors))
    }
}
