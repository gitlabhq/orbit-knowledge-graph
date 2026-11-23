//! An engine: the pipeline compiled over every family member's rules in one
//! interner, its mounted frontends, the project settings as the members'
//! union, and the budgets. Built once and shared between runs.

use rustc_hash::FxHashMap;

use crate::engine::Registration;
use crate::engine::lang::DataSpec;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::language::SupportLang;
use crate::sentinel::Limits;

pub struct Engine {
    pub export_config: crate::backend::tables::ExportConfig,
    /// The mounted frontends, the env's own language first.
    pub frontends: Vec<Box<dyn crate::frontend::Frontend>>,
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub members: Vec<SupportLang>,
    pub project: Project,
    pub limits: Limits,
    pub pipeline: crate::engine::Pipeline,
    rules: FxHashMap<SupportLang, usize>,
}

#[derive(Clone)]
pub enum ParseFormat {
    Json,
    Toml,
    Raw(regex::Regex),
}

#[derive(Clone)]
pub struct ParseFile {
    pub name: String,
    pub format: ParseFormat,
}

/// What the project tree reads for the whole family: each member's rule
/// index in member order, and the members' settings merged.
#[derive(Default)]
pub struct Project {
    pub rules: Vec<usize>,
    pub lookup_from: Vec<u16>,
    pub parse_files: Vec<ParseFile>,
}

/// How an engine is configured: the family it serves, the modules it
/// mounts, its budgets, the interner it compiles into, and the export's
/// column overrides. Everything else comes from pipeline.yaml.
pub struct EngineBuilder {
    lang_id: SupportLang,
    modules: &'static [Registration],
    limits: Limits,
    lang: Option<Lang>,
    export_config: crate::backend::tables::ExportConfig,
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

    pub fn export_config(mut self, config: crate::backend::tables::ExportConfig) -> Self {
        self.export_config = config;
        self
    }

    /// Compiles the family's rules against the mounted modules.
    pub fn build(self) -> Result<Engine, LoadError> {
        let lang = self.lang.unwrap_or_default();
        Engine::load(
            self.lang_id,
            lang,
            self.limits,
            self.modules,
            self.export_config,
        )
    }
}

impl Engine {
    pub fn builder(lang_id: SupportLang) -> EngineBuilder {
        EngineBuilder {
            lang_id,
            modules: crate::modules::ALL,
            limits: Limits::UNLIMITED,
            lang: None,
            export_config: Default::default(),
        }
    }

    fn load(
        lang_id: SupportLang,
        lang: Lang,
        limits: Limits,
        modules: &'static [Registration],
        export_config: crate::backend::tables::ExportConfig,
    ) -> Result<Self, LoadError> {
        let members = lang_id.family_members();
        let mut rules: FxHashMap<SupportLang, usize> = FxHashMap::default();
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
            rules.insert(member, index);
        }
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
        let project = project(&pipeline, &lang, sources.len())?;
        let mut frontends = vec![crate::frontend::for_language(
            lang_id,
            limits.max_tree_depth,
        )];
        for &member in members.iter().filter(|&&m| m != lang_id) {
            frontends.push(crate::frontend::for_language(member, limits.max_tree_depth));
        }
        Ok(Self {
            frontends,
            export_config,
            pipeline,
            project,
            lang,
            lang_id,
            members,
            limits,
            rules,
        })
    }

    pub fn in_family(&self, lang: SupportLang) -> bool {
        self.members.contains(&lang)
    }

    /// The index of a path's language rules; languages that share a rule
    /// file share an index.
    pub fn rules_index(&self, path: &str) -> usize {
        let lang = SupportLang::from_path(path)
            .filter(|l| self.in_family(*l))
            .unwrap_or(self.lang_id);
        self.rules[&lang]
    }
}

fn texts(spec: Option<&DataSpec>) -> Vec<&str> {
    match spec {
        Some(DataSpec::Str(s)) => vec![s.as_str()],
        Some(DataSpec::List(items)) => items.iter().flat_map(|i| texts(Some(i))).collect(),
        _ => Vec::new(),
    }
}

fn project(
    pipeline: &crate::engine::Pipeline,
    lang: &Lang,
    files: usize,
) -> Result<Project, LoadError> {
    let mut project = Project {
        rules: (0..files).collect(),
        ..Project::default()
    };
    for index in 0..files {
        for kind in texts(pipeline.setting(index, "lookup_from")) {
            let kind = lang.intern_kind(kind);
            if !project.lookup_from.contains(&kind) {
                project.lookup_from.push(kind);
            }
        }
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
