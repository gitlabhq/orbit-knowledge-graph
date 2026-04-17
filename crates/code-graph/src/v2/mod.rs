pub mod custom;
pub mod langs;
pub mod pipeline;
pub mod registry;

pub use pipeline::{
    GenericPipeline, LanguagePipeline, Pipeline, PipelineConfig, PipelineOutput, PipelineResult,
};
pub use registry::{dispatch_by_tag, dispatch_language};
