//! Pipeline registry — maps Language variants and Tag strings to pipeline impls.
//!
//! All pipeline registration lives here. Adding a new language or custom
//! pipeline: add one line to the `register_v2_pipelines!` invocation below.

use code_graph_config::Language;
use code_graph_linker::v2::NoRules;
use parser_core::dsl::types::DslParser;

use crate::v2::custom::ruby::RubyPipeline;
use crate::v2::langs::csharp::CSharpDsl;
use crate::v2::langs::go::{GoDsl, GoRules};
use crate::v2::langs::java::{JavaDsl, JavaRules};
use crate::v2::langs::kotlin::{KotlinDsl, KotlinRules};
use crate::v2::langs::python::{PythonDsl, PythonRules};
use crate::v2::langs::ruby::{RubyDsl, RubyRules};
use crate::v2::pipeline::{
    FileInput, GenericPipeline, LanguagePipeline, PipelineError, PipelineOutput,
};

// ── Macro ───────────────────────────────────────────────────────

/// Pipeline registration macro. Generates `dispatch_language` and `dispatch_by_tag`.
///
/// Pipeline types wrapped in `[]` to avoid comma ambiguity in generics.
macro_rules! register_v2_pipelines {
    // Entry points — match first entry to avoid catch-all recursion.
    (Tag($tag:literal) => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [] [[$tag => $p]] $($rest)*);
    };
    ($v:ident => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [[$v => $p]] [] $($rest)*);
    };
    // Done.
    (@munch [$($langs:tt)*] [$($tags:tt)*]) => {
        register_v2_pipelines!(@emit_lang $($langs)*);
        register_v2_pipelines!(@emit_tag $($tags)*);
    };
    // Tag entry (before ident arm — first-match-wins).
    (@munch [$($langs:tt)*] [$($tags:tt)*] Tag($tag:literal) => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [$($langs)*] [$($tags)* [$tag => $p]] $($rest)*);
    };
    (@munch [$($langs:tt)*] [$($tags:tt)*] Tag($tag:literal) => $p:tt) => {
        register_v2_pipelines!(@munch [$($langs)*] [$($tags)* [$tag => $p]]);
    };
    // Language entry.
    (@munch [$($langs:tt)*] [$($tags:tt)*] $v:ident => $p:tt , $($rest:tt)*) => {
        register_v2_pipelines!(@munch [$($langs)* [$v => $p]] [$($tags)*] $($rest)*);
    };
    (@munch [$($langs:tt)*] [$($tags:tt)*] $v:ident => $p:tt) => {
        register_v2_pipelines!(@munch [$($langs)* [$v => $p]] [$($tags)*]);
    };
    // Emit dispatch_language (called by Pipeline::run).
    (@emit_lang $( [$variant:ident => [$($pipeline:tt)*]] )* ) => {
        pub fn dispatch_language(
            language: Language,
            files: &[FileInput],
            root_path: &str,
        ) -> Option<Result<PipelineOutput, Vec<PipelineError>>> {
            Some(match language {
                $(Language::$variant => <$($pipeline)*>::process_files(files, root_path),)*
                _ => return None,
            })
        }
    };
    // Emit dispatch_by_tag (called by YAML test harness).
    (@emit_tag $( [$tag:literal => [$($pipeline:tt)*]] )* ) => {
        pub fn dispatch_by_tag(
            tag: &str,
            files: &[FileInput],
            root_path: &str,
        ) -> Option<Result<PipelineOutput, Vec<PipelineError>>> {
            Some(match tag {
                $($tag => <$($pipeline)*>::process_files(files, root_path),)*
                _ => return None,
            })
        }
    };
}

// ── Registration ────────────────────────────────────────────────

register_v2_pipelines! {
    Python  => [GenericPipeline<DslParser<PythonDsl>, PythonRules>],
    Java    => [GenericPipeline<DslParser<JavaDsl>, JavaRules>],
    Kotlin  => [GenericPipeline<DslParser<KotlinDsl>, KotlinRules>],
    CSharp  => [GenericPipeline<DslParser<CSharpDsl>, NoRules<CSharpDsl>>],
    Go      => [GenericPipeline<DslParser<GoDsl>, GoRules>],
    Ruby    => [GenericPipeline<DslParser<RubyDsl>, RubyRules>],
    Tag("ruby_prism") => [RubyPipeline],
}
