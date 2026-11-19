//! One family's compiled environment: the pipeline over every member's rules
//! in one interner, the project settings as the members' union, and the
//! budgets. Long-lived and shared between runs.

use rustc_hash::FxHashMap;

use crate::engine::lang::DataSpec;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::language::SupportLang;
use crate::sentinel::Limits;

pub struct Env {
    pub export_config: crate::export::ExportConfig,
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

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Self::with_limits(lang_id, Limits::UNLIMITED)
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Result<Self, LoadError> {
        Self::with_lang(lang_id, Lang::new(), limits)
    }

    /// Compiles the family's rules into an existing interner, so ids in a
    /// graph restored from a snapshot and ids in the rules agree.
    pub fn with_lang(lang_id: SupportLang, lang: Lang, limits: Limits) -> Result<Self, LoadError> {
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
        let pipeline = crate::engine::Pipeline::load(&lang, &sources)?;
        let project = project(&pipeline, &lang, sources.len())?;
        Ok(Self {
            export_config: crate::export::ExportConfig::default(),
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
