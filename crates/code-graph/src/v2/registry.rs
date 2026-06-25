//! Pipeline registry — maps Language variants and Tag strings to pipeline impls.
//!
//! All pipeline registration lives here. Adding a new language or custom
//! pipeline: add one line to the `register_v2_pipelines!` invocation below.

use crate::v2::config::{Language, LanguageFamily};

use crate::v2::langs::custom::js::JsPipeline;
use crate::v2::langs::custom::rust::RustPipeline;
use crate::v2::langs::generic::bash::{BashDsl, BashRules};
use crate::v2::langs::generic::c::{CDsl, CRules};
use crate::v2::langs::generic::cpp::{CppDsl, CppRules};
use crate::v2::langs::generic::csharp::{CSharpDsl, CSharpRules};
use crate::v2::langs::generic::elixir::{ElixirDsl, ElixirRules};
use crate::v2::langs::generic::go::{GoDsl, GoRules};
use crate::v2::langs::generic::hcl::{HclDsl, HclRules};
use crate::v2::langs::generic::java::{JavaDsl, JavaRules};
use crate::v2::langs::generic::kotlin::{KotlinDsl, KotlinRules};
use crate::v2::langs::generic::lua::{LuaDsl, LuaRules};
use crate::v2::langs::generic::php::{PhpDsl, PhpRules};
use crate::v2::langs::generic::python::{PythonDsl, PythonRules};
use crate::v2::langs::generic::ruby::{RubyDsl, RubyRules};
use crate::v2::langs::generic::swift::{SwiftDsl, SwiftRules};
use std::sync::Arc;

use crate::v2::inventory::{FamilyFileInput, FileInput};
use crate::v2::pipeline::{
    BatchTx, GenericPipeline, LanguageContext, LanguagePipeline, PipelineContext, PipelineError,
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
    // Emit dispatch_language, lang_ctx_for, dispatch_by_tag.
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

        /// Build a [`LanguageContext`] for the given language at runtime.
        /// Auto-generated from the pipeline registration table.
        /// Returns `None` for custom pipelines (JS, Rust) that don't
        /// implement `lang_ctx`.
        pub fn lang_ctx_for(
            language: Language,
            ctx: &Arc<PipelineContext>,
        ) -> Option<Arc<LanguageContext>> {
            #[allow(unreachable_patterns)]
            match language {
                $(Language::$variant => <$($pipeline)*>::lang_ctx(ctx),)*
                _ => None,
            }
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
    Bash    => [GenericPipeline<BashDsl, BashRules>],
    C       => [GenericPipeline<CDsl, CRules>],
    Cpp     => [GenericPipeline<CppDsl, CppRules>],
    JavaScript => [JsPipeline],
    TypeScript => [JsPipeline],
    Python  => [GenericPipeline<PythonDsl, PythonRules>],
    Java    => [GenericPipeline<JavaDsl, JavaRules>],
    Kotlin  => [GenericPipeline<KotlinDsl, KotlinRules>],
    CSharp  => [GenericPipeline<CSharpDsl, CSharpRules>],
    Go      => [GenericPipeline<GoDsl, GoRules>],
    Elixir  => [GenericPipeline<ElixirDsl, ElixirRules>],
    Ruby    => [GenericPipeline<RubyDsl, RubyRules>],
    Lua     => [GenericPipeline<LuaDsl, LuaRules>],
    Php     => [GenericPipeline<PhpDsl, PhpRules>],
    Swift   => [GenericPipeline<SwiftDsl, SwiftRules>],
    Rust    => [RustPipeline],
    Hcl     => [GenericPipeline<HclDsl, HclRules>],
    Tag("js") => [JsPipeline],
}

// ── Family dispatch ─────────────────────────────────────────────

/// Dispatch a language family to the appropriate pipeline(s).
///
/// For single-language families and families where all members use
/// custom pipelines, groups files by language and delegates to
/// [`dispatch_language`]. For multi-language generic-pipeline
/// families, runs a shared [`FamilyPipeline`] so all members share
/// a single `CodeGraph` and can resolve symbols across languages.
pub fn dispatch_family(
    _family: LanguageFamily,
    files: &[FamilyFileInput],
    ctx: &Arc<PipelineContext>,
    btx: &BatchTx<'_>,
) -> Option<Result<(), Vec<PipelineError>>> {
    // Collect the distinct languages present in this batch.
    let mut languages: rustc_hash::FxHashSet<Language> = rustc_hash::FxHashSet::default();
    for f in files {
        languages.insert(f.language);
    }

    // If there's only one language, delegate to the per-language pipeline
    // directly -- avoids building the family machinery for the common case.
    if languages.len() == 1 {
        let lang = *languages.iter().next().unwrap();
        let paths: Vec<FileInput> = files.iter().map(|f| f.path.clone()).collect();
        return dispatch_language(lang, &paths, ctx, btx);
    }

    // Multiple languages: try to build LanguageContexts for each.
    // If any member doesn't support it (custom pipeline), fall back
    // to running each language's pipeline separately.
    let mut member_ctxs: rustc_hash::FxHashMap<Language, Arc<LanguageContext>> =
        rustc_hash::FxHashMap::default();
    let mut has_custom = false;
    for &lang in &languages {
        match lang_ctx_for(lang, ctx) {
            Some(lctx) => {
                member_ctxs.insert(lang, lctx);
            }
            None => {
                has_custom = true;
            }
        }
    }

    if has_custom {
        // Fall back: run each language's pipeline over its files separately.
        let mut by_lang: rustc_hash::FxHashMap<Language, Vec<FileInput>> =
            rustc_hash::FxHashMap::default();
        for f in files {
            by_lang.entry(f.language).or_default().push(f.path.clone());
        }
        let mut all_errors: Vec<PipelineError> = Vec::new();
        let mut any_matched = false;
        for (lang, paths) in &by_lang {
            match dispatch_language(*lang, paths, ctx, btx) {
                Some(Ok(())) => any_matched = true,
                Some(Err(errs)) => {
                    any_matched = true;
                    all_errors.extend(errs);
                }
                None => {
                    tracing::warn!(%lang, "no pipeline registered for language");
                }
            }
        }
        return if !any_matched {
            None
        } else if all_errors.is_empty() {
            Some(Ok(()))
        } else {
            Some(Err(all_errors))
        };
    }

    // All members are generic-pipeline languages: run FamilyPipeline
    // with a shared CodeGraph.
    Some(crate::v2::pipeline::FamilyPipeline::run(
        files,
        &member_ctxs,
        ctx,
        btx,
    ))
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
            file_timings: std::sync::Mutex::new(Vec::new()),
            language_timings: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn noop_on_batch()
    -> impl Fn(&str, arrow::record_batch::RecordBatch) -> Result<(), crate::v2::SinkError> {
        |_: &str, _: arrow::record_batch::RecordBatch| Ok(())
    }

    #[test]
    fn javascript_pipeline_is_registered() {
        let ctx = test_ctx();
        let conv = NoopConverter;
        let on_batch = noop_on_batch();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &on_batch,
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
        let on_batch = noop_on_batch();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &on_batch,
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
        let on_batch = noop_on_batch();
        let (dirs, files, d, i, e) = (
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        );
        let errors = std::sync::Mutex::new(Vec::new());
        let btx = BatchTx::new(
            &on_batch,
            &conv,
            &errors,
            GraphStatsCounters::new(&dirs, &files, &d, &i, &e),
        );
        assert!(dispatch_by_tag("js", &[], &ctx, &btx).is_some());
    }
}
