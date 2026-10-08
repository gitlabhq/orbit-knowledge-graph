//! One family's compiled environment: every member's rules in one interner,
//! the project-level resolve config as their union, and the budgets.
//! Long-lived and shared between runs.

use rustc_hash::FxHashMap;

use crate::error::LoadError;
use crate::intern::Lang;
use crate::rules::{LangConfig, ResolveConfig, ResolveStage};
use crate::sentinel::Limits;
use crate::treesitter::SupportLang;
use crate::{rules, treesitter};

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub members: Vec<SupportLang>,
    pub resolve: FamilyResolve,
    pub limits: Limits,
    rules: FxHashMap<SupportLang, usize>,
    compiled: Vec<LangConfig>,
}

/// What the resolver reads for the whole project: the members' resolve
/// config merged and their stages in member order, one rule file counted once.
#[derive(Default)]
pub struct FamilyResolve {
    pub config: ResolveConfig,
    pub stages: Vec<ResolveStage>,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Self::with_limits(lang_id, Limits::load()?)
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Result<Self, LoadError> {
        Self::with_lang(lang_id, Lang::new(), limits)
    }

    /// Compiles the family's rules into an existing interner, so ids in a
    /// graph restored from a snapshot and ids in the rules agree.
    pub fn with_lang(lang_id: SupportLang, lang: Lang, limits: Limits) -> Result<Self, LoadError> {
        let members = lang_id.family_members();
        let mut rules: FxHashMap<SupportLang, usize> = FxHashMap::default();
        let mut compiled: Vec<LangConfig> = Vec::new();
        let mut sources: Vec<Option<&'static str>> = Vec::new();
        let mut resolve = FamilyResolve::default();
        for &member in &members {
            let yaml = treesitter::lang_yaml(member);
            let shared = sources
                .iter()
                .position(|s| s.map(str::as_ptr) == yaml.map(str::as_ptr));
            let index = match shared {
                Some(index) => index,
                None => {
                    let mut config = match yaml {
                        Some(yaml) => rules::load_lang(yaml, &lang)?,
                        None => LangConfig::default(),
                    };
                    resolve.config.merge(&config.config.resolve);
                    resolve.stages.append(&mut config.resolve_stages);
                    compiled.push(config);
                    sources.push(yaml);
                    compiled.len() - 1
                }
            };
            rules.insert(member, index);
        }
        Ok(Self {
            lang,
            lang_id,
            members,
            resolve,
            limits,
            rules,
            compiled,
        })
    }

    pub fn in_family(&self, lang: SupportLang) -> bool {
        self.members.contains(&lang)
    }

    /// The file-level rules for a path: rewrite, link and display come from
    /// the file's own language.
    pub fn rules_for(&self, path: &str) -> &LangConfig {
        let lang = SupportLang::from_path(path)
            .filter(|l| self.in_family(*l))
            .unwrap_or(self.lang_id);
        &self.compiled[self.rules[&lang]]
    }
}
