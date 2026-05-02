//! Pipeline registry — maps Language variants and Tag strings to pipeline impls.
//!
//! All pipeline registration lives here. Adding a new language or custom
//! pipeline: add one line to the `register_v2_pipelines!` invocation below.

use crate::v2::config::Language;

use crate::v2::langs::custom::js::JsPipeline;
use crate::v2::langs::custom::rust::RustPipeline;
use crate::v2::langs::generic::csharp::{CSharpDsl, CSharpRules};
use crate::v2::langs::generic::go::{GoDsl, GoRules};
use crate::v2::langs::generic::java::{JavaDsl, JavaRules};
use crate::v2::langs::generic::kotlin::{KotlinDsl, KotlinRules};
use crate::v2::langs::generic::python::{PythonDsl, PythonRules};
use crate::v2::langs::generic::ruby::{RubyDsl, RubyRules};
use std::sync::Arc;

use crate::v2::pipeline::{
    BatchTx, FileInput, GenericPipeline, LanguagePipeline, PipelineContext, PipelineError,
};

// ── Macro ───────────────────────────────────────────────────────

/// Pipeline registration macro. Generates `dispatch_language` and `dispatch_by_tag`.
///
/// Pipeline types wrapped in `[]` to avoid comma ambiguity in generics.
macro_rules! register_v2_pipelines {
    // Done.
    (@munch [$($langs:tt)*] [$($tags:tt)*]) => {
        register_v2_pipelines!(@emit_lang $($langs)*);
        register_v2_pipelines!(@emit_tag $($tags)*);
    };
    // Tag entry (before ident arm — first-match-wins).
    (@munch [$($langs:tt)*] [$($tags:tt)*] Tag($tag:literal) => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [$($langs)*] [$($tags)* [$tag => $p]] $($rest)*);
    };
    // Language entry.
    (@munch [$($langs:tt)*] [$($tags:tt)*] $v:ident => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [$($langs)* [$v => $p]] [$($tags)*] $($rest)*);
    };
    // Emit dispatch_language (called by Pipeline::run).
    (@emit_lang $( [$variant:ident => [$($pipeline:tt)*]] )* ) => {
        pub fn dispatch_language(
            language: Language,
            files: &[FileInput],
            ctx: &Arc<PipelineContext>,
            btx: &BatchTx<'_>,
        ) -> Option<Result<(), Vec<PipelineError>>> {
            #[allow(unreachable_patterns)]
            Some(match language {
                $(Language::$variant => <$($pipeline)*>::process_files(files, ctx, btx),)*
                _ => return None,
            })
        }
    };
    // Emit dispatch_by_tag (called by YAML test harness).
    (@emit_tag $( [$tag:literal => [$($pipeline:tt)*]] )* ) => {
        pub fn dispatch_by_tag(
            tag: &str,
            files: &[FileInput],
            ctx: &Arc<PipelineContext>,
            btx: &BatchTx<'_>,
        ) -> Option<Result<(), Vec<PipelineError>>> {
            Some(match tag {
                $($tag => <$($pipeline)*>::process_files(files, ctx, btx),)*
                _ => return None,
            })
        }
    };
    ($($entries:tt)*) => {
        register_v2_pipelines!(@munch [] [] $($entries)*);
    };
}

// ── Registration ────────────────────────────────────────────────

register_v2_pipelines! {
    JavaScript => [JsPipeline],
    TypeScript => [JsPipeline],
    Python  => [GenericPipeline<PythonDsl, PythonRules>],
    Java    => [GenericPipeline<JavaDsl, JavaRules>],
    Kotlin  => [GenericPipeline<KotlinDsl, KotlinRules>],
    CSharp  => [GenericPipeline<CSharpDsl, CSharpRules>],
    Go      => [GenericPipeline<GoDsl, GoRules>],
    Ruby    => [GenericPipeline<RubyDsl, RubyRules>],
    Rust    => [RustPipeline],
    Tag("js") => [JsPipeline],
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::pipeline::GraphStatsCounters;
    use crate::v2::pipeline::PipelineConfig;
    use std::sync::atomic::AtomicUsize;

    struct NoopConverter;
    impl crate::v2::sink::GraphConverter for NoopConverter {
        fn convert(
            &self,
            _graph: crate::v2::linker::CodeGraph,
        ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, crate::v2::SinkError> {
            Ok(Vec::new())
        }
    }

    fn test_ctx() -> Arc<PipelineContext> {
        Arc::new(PipelineContext {
            config: PipelineConfig::default(),
            tracer: crate::v2::trace::Tracer::new(false),
            root_path: "/".to_string(),
            skipped: std::sync::Mutex::new(Vec::new()),
            faults: std::sync::Mutex::new(Vec::new()),
        })
    }

    #[test]
    fn javascript_pipeline_is_registered() {
        let ctx = test_ctx();
        let conv = NoopConverter;
        let (tx, _rx) = crossbeam_channel::unbounded();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &tx,
            &conv,
            &errors,
            GraphStatsCounters::new(&dirs, &files, &d, &i, &e),
        );
        assert!(dispatch_language(Language::JavaScript, &[], &ctx, &btx).is_some());
    }

    #[test]
    fn typescript_pipeline_is_registered() {
        let ctx = test_ctx();
        let conv = NoopConverter;
        let (tx, _rx) = crossbeam_channel::unbounded();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &tx,
            &conv,
            &errors,
            GraphStatsCounters::new(&dirs, &files, &d, &i, &e),
        );
        assert!(dispatch_language(Language::TypeScript, &[], &ctx, &btx).is_some());
    }

    #[test]
    fn js_pipeline_tag_is_registered() {
        let ctx = test_ctx();
        let conv = NoopConverter;
        let (tx, _rx) = crossbeam_channel::unbounded();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &tx,
            &conv,
            &errors,
            GraphStatsCounters::new(&dirs, &files, &d, &i, &e),
        );
        assert!(dispatch_by_tag("js", &[], &ctx, &btx).is_some());
    }
}
