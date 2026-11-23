//! An engine: the pipeline compiled over every family member's rules in one
//! interner, its mounted frontends, and the budgets. Built once and shared
//! between runs.

use crate::engine::Registration;
use crate::engine::lang::DataSpec;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::sentinel::Limits;

pub struct Engine {
    /// The mounted frontends, the engine's own language first.
    pub frontends: Vec<Mounted>,
    pub lang: Lang,
    /// The mounted project frontend, which builds the project tree.
    pub project: Box<dyn crate::frontend::project::ProjectFrontend>,
    pub limits: Limits,
    pub pipeline: crate::engine::Pipeline,
}

/// A mounted frontend and the rules the trees it parses run.
pub struct Mounted {
    pub frontend: Box<dyn crate::frontend::Frontend>,
    pub rules: usize,
}

/// The YAML an engine compiles: pipeline.yaml, the rule files every
/// frontend shares, and the libraries rule files import by name.
#[derive(Clone, Copy)]
pub struct PipelineSource {
    pub pipeline: &'static str,
    pub rules: &'static [(&'static str, &'static str)],
    pub libs: &'static [(&'static str, &'static str)],
}

/// How an engine is put together: the frontends it mounts, each with the
/// rule file its trees run, the pipeline, the modules, the budgets, and the
/// interner it compiles into. The first frontend reads what no other does.
pub struct EngineBuilder {
    frontends: Vec<(Box<dyn crate::frontend::Frontend>, Option<&'static str>)>,
    pipeline: Option<PipelineSource>,
    modules: &'static [Registration],
    limits: Limits,
    lang: Option<Lang>,
}

impl EngineBuilder {
    /// Mounts a frontend whose trees run `rules`, a language's own rule
    /// file, if it has one.
    pub fn frontend(
        mut self,
        frontend: Box<dyn crate::frontend::Frontend>,
        rules: Option<&'static str>,
    ) -> Self {
        self.frontends.push((frontend, rules));
        self
    }

    pub fn pipeline(mut self, source: PipelineSource) -> Self {
        self.pipeline = Some(source);
        self
    }

    /// The modules the engine mounts; `modules::ALL` unless given.
    pub fn modules(mut self, modules: &'static [Registration]) -> Self {
        self.modules = modules;
        self
    }

    /// The budgets; unlimited unless given.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// An existing interner, so ids in a graph restored from a snapshot and
    /// ids in the rules agree; a fresh one unless given.
    pub fn lang(mut self, lang: Lang) -> Self {
        self.lang = Some(lang);
        self
    }

    /// Compiles the pipeline against the mounted modules; frontends that
    /// share a rule file share its compiled rules.
    pub fn build(self) -> Result<Engine, LoadError> {
        let source = self
            .pipeline
            .ok_or_else(|| LoadError("an engine needs a pipeline".into()))?;
        if self.frontends.is_empty() {
            return Err(LoadError("an engine needs a frontend".into()));
        }
        let lang = self.lang.unwrap_or_default();
        let mut langs: Vec<Option<&'static str>> = Vec::new();
        let mut frontends = Vec::new();
        for (frontend, yaml) in self.frontends {
            let same = |s: &Option<&str>| s.map(str::as_ptr) == yaml.map(str::as_ptr);
            let rules = match langs.iter().position(same) {
                Some(at) => at,
                None => {
                    langs.push(yaml);
                    langs.len() - 1
                }
            };
            frontends.push(Mounted { frontend, rules });
        }
        let pipeline = crate::engine::Pipeline::load(
            &lang,
            &crate::engine::lang::Sources {
                pipeline: source.pipeline,
                rules: source.rules,
                libs: source.libs,
                langs: &langs,
            },
            self.modules,
        )?;
        let project = Box::new(project(&pipeline, langs.len())?);
        Ok(Engine {
            frontends,
            pipeline,
            project,
            lang,
            limits: self.limits,
        })
    }
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            frontends: Vec::new(),
            pipeline: None,
            modules: crate::modules::ALL,
            limits: Limits::UNLIMITED,
            lang: None,
        }
    }

    /// The mounted frontend that reads `path`, if any.
    pub fn frontend_for(&self, path: &str) -> Option<&Mounted> {
        self.frontends.iter().find(|m| m.frontend.accepts(path))
    }

    /// Whether a mounted frontend reads `path`.
    pub fn reads(&self, path: &str) -> bool {
        self.frontend_for(path).is_some()
    }

    /// The rules a tree labelled `path` runs: its frontend's, or the
    /// engine's own language's; frontends that share a rule file share an
    /// index.
    pub fn rules_index(&self, path: &str) -> usize {
        self.frontend_for(path).unwrap_or(&self.frontends[0]).rules
    }
}

fn texts(spec: Option<&DataSpec>) -> Vec<&str> {
    match spec {
        Some(DataSpec::Str(s)) => vec![s.as_str()],
        Some(DataSpec::List(items)) => items.iter().flat_map(|i| texts(Some(i))).collect(),
        _ => Vec::new(),
    }
}

/// The project tree frontend, inlining every language's `parse_files`.
fn project(
    pipeline: &crate::engine::Pipeline,
    files: usize,
) -> Result<crate::frontend::project::ProjectTree, LoadError> {
    use crate::frontend::project::{ParseFile, ParseFormat, ProjectTree};
    let mut project = ProjectTree::default();
    for index in 0..files {
        let Some(DataSpec::List(entries)) = pipeline.setting(index, "parse_files") else {
            continue;
        };
        for entry in entries {
            let fields = texts(Some(entry));
            let (name, format) = match fields.as_slice() {
                [name, "json"] => (*name, ParseFormat::Json),
                [name, "toml"] => (*name, ParseFormat::Toml),
                [name, "raw", extract] => (*name, ParseFormat::Raw(regex::Regex::new(extract)?)),
                _ => return Err(LoadError(format!("parse_files entry {fields:?}"))),
            };
            if !project.parse_files.iter().any(|p| p.name == name) {
                project.parse_files.push(ParseFile {
                    name: name.to_string(),
                    format,
                });
            }
        }
    }
    Ok(project)
}
