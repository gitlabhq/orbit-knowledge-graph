//! An engine: the pipeline compiled over every family member's rules in one
//! interner, its mounted frontends, and the budgets. Built once and shared
//! between runs.

use crate::engine::Registration;
use crate::error::LoadError;
use crate::frontend::project::SetFrontend;
use crate::intern::Lang;
use crate::sentinel::Limits;

pub struct Engine {
    /// The mounted frontends, the engine's own language first.
    pub frontends: Vec<Mounted>,
    pub lang: Lang,
    /// Per tree set, the frontend that builds its trees; none for the set
    /// input files parse into.
    pub sets: Vec<Option<Box<dyn SetFrontend>>>,
    /// The rule set of trees that span the family: every member's own rules,
    /// then the shared ones, over the members' data together.
    pub family_rules: usize,
    pub limits: Limits,
    pub pipeline: crate::engine::Pipeline,
}

/// A mounted frontend, the rules the trees it parses run, and whether it
/// reads files itself or only parses what rules hand it.
pub struct Mounted {
    pub frontend: Box<dyn crate::frontend::Frontend>,
    pub rules: usize,
    pub reads_files: bool,
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
    frontends: Vec<(
        Box<dyn crate::frontend::Frontend>,
        Option<&'static str>,
        bool,
    )>,
    sets: Vec<(&'static str, Box<dyn SetFrontend>)>,
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
        self.frontends.push((frontend, rules, true));
        self
    }

    /// Mounts a frontend that reads no files itself: rules parse nodes with
    /// it, and the regions it builds run `rules`.
    pub fn embedded(
        mut self,
        frontend: Box<dyn crate::frontend::Frontend>,
        rules: Option<&'static str>,
    ) -> Self {
        self.frontends.push((frontend, rules, false));
        self
    }

    /// Mounts the frontend that builds the trees of the whole tree set `set`.
    pub fn set_frontend(mut self, set: &'static str, frontend: Box<dyn SetFrontend>) -> Self {
        self.sets.push((set, frontend));
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
        for (frontend, yaml, reads_files) in self.frontends {
            let same = |s: &Option<&str>| s.map(str::as_ptr) == yaml.map(str::as_ptr);
            let rules = match langs.iter().position(same) {
                Some(at) => at,
                None => {
                    langs.push(yaml);
                    langs.len() - 1
                }
            };
            frontends.push(Mounted {
                frontend,
                rules,
                reads_files,
            });
        }
        let pipeline = crate::engine::Pipeline::load(
            &lang,
            &crate::engine::lang::Sources {
                pipeline: source.pipeline,
                rules: source.rules,
                libs: source.libs,
                langs: &langs,
                family: &file_rules(&frontends),
            },
            self.modules,
        )?;
        let family_rules = langs.len();
        let mut sets: Vec<Option<Box<dyn SetFrontend>>> =
            lang.tree_sets().iter().map(|_| None).collect();
        for (name, mut frontend) in self.sets {
            let set = lang
                .tree_set(name)
                .filter(|&set| !lang.tree_sets()[set].per_file)
                .ok_or_else(|| LoadError(format!("{name} is not a whole tree set")))?;
            frontend.load(&pipeline, family_rules)?;
            sets[set] = Some(frontend);
        }
        if let Some(missing) = lang
            .tree_sets()
            .iter()
            .zip(&sets)
            .find(|(s, f)| !s.per_file && f.is_none())
        {
            return Err(LoadError(format!(
                "tree set {} has no frontend",
                missing.0.name
            )));
        }
        Ok(Engine {
            frontends,
            pipeline,
            sets,
            family_rules,
            lang,
            limits: self.limits,
        })
    }
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            frontends: Vec::new(),
            sets: Vec::new(),
            pipeline: None,
            modules: crate::modules::ALL,
            limits: Limits::UNLIMITED,
            lang: None,
        }
    }

    /// The mounted frontend that reads `path`, if any.
    pub fn frontend_for(&self, path: &str) -> Option<&Mounted> {
        self.frontends
            .iter()
            .find(|m| m.reads_files && m.frontend.accepts(path))
    }

    /// The mounted frontend `name` names, reading files or not.
    pub fn frontend_named(&self, name: &str) -> Option<&Mounted> {
        self.frontends.iter().find(|m| m.frontend.is_named(name))
    }

    /// Whether a set frontend reads the contents of the file at `path`.
    pub fn set_reads(&self, path: &str) -> bool {
        self.sets.iter().flatten().any(|f| f.reads(path))
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

    /// The rules tree `fi` of `trees` runs: its file's, or the family's for a
    /// tree of a whole set.
    pub fn rules_of<S: crate::tree::Storage>(
        &self,
        trees: crate::tree::Trees<'_, S>,
        fi: u32,
    ) -> usize {
        match trees.set_of(fi) == self.lang.input_set() {
            true => self.rules_index(&trees.get(fi).label),
            false => self.family_rules,
        }
    }
}

fn file_rules(frontends: &[Mounted]) -> Vec<usize> {
    let mut rules = Vec::new();
    for m in frontends.iter().filter(|m| m.reads_files) {
        if !rules.contains(&m.rules) {
            rules.push(m.rules);
        }
    }
    rules.sort_unstable();
    rules
}
