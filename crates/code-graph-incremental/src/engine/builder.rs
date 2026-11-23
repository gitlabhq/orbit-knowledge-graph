//! An engine: the pipeline compiled over every family member's rules in one
//! interner, its mounted frontends, and the budgets. Built once and shared
//! between runs.

use crate::engine::Registration;
use crate::engine::lang::DataSpec;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::language::SupportLang;
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

/// How an engine is configured: the family it serves, the modules it
/// mounts, its budgets, and the interner it compiles into. Everything else
/// comes from pipeline.yaml.
pub struct EngineBuilder {
    lang_id: SupportLang,
    modules: &'static [Registration],
    limits: Limits,
    lang: Option<Lang>,
}

impl EngineBuilder {
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

    /// Compiles the family's rules against the mounted modules.
    pub fn build(self) -> Result<Engine, LoadError> {
        let lang = self.lang.unwrap_or_default();
        Engine::load(self.lang_id, lang, self.limits, self.modules)
    }
}

impl Engine {
    pub fn builder(lang_id: SupportLang) -> EngineBuilder {
        EngineBuilder {
            lang_id,
            modules: crate::modules::ALL,
            limits: Limits::UNLIMITED,
            lang: None,
        }
    }

    fn load(
        lang_id: SupportLang,
        lang: Lang,
        limits: Limits,
        modules: &'static [Registration],
    ) -> Result<Self, LoadError> {
        let members = lang_id.family_members();
        let mut frontends = Vec::new();
        let mut sources: Vec<Option<&'static str>> = Vec::new();
        for &member in &members {
            let yaml = crate::language::lang_yaml(member);
            let index = match sources
                .iter()
                .position(|s| s.map(str::as_ptr) == yaml.map(str::as_ptr))
            {
                Some(index) => index,
                None => {
                    sources.push(yaml);
                    sources.len() - 1
                }
            };
            let frontend = crate::frontend::for_language(member, limits.max_tree_depth);
            frontends.push((
                member,
                Mounted {
                    frontend,
                    rules: index,
                },
            ));
        }
        frontends.sort_by_key(|(member, _)| *member != lang_id);
        let frontends = frontends.into_iter().map(|(_, m)| m).collect();
        let pipeline = crate::engine::Pipeline::load(
            &lang,
            &crate::engine::lang::Sources {
                pipeline: crate::config::PIPELINE,
                rules: crate::config::RULES,
                libs: crate::config::LIBS,
                langs: &sources,
            },
            modules,
        )?;
        let project = Box::new(project(&pipeline, sources.len())?);
        Ok(Self {
            frontends,
            pipeline,
            project,
            lang,
            limits,
        })
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
