//! One language's compiled environment: rule stages, config and budgets.
//! Long-lived and shared between runs.

use crate::error::LoadError;
use crate::intern::Lang;
use crate::pattern::Rewrite;
use crate::rules::{Config, ResolveStage};
use crate::sentinel::Limits;
use crate::treesitter::SupportLang;
use crate::{rules, treesitter};

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve_stages: Vec<ResolveStage>,
    pub config: Config,
    pub limits: Limits,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Self::with_limits(lang_id, Limits::default())
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Result<Self, LoadError> {
        let lang = Lang::new();
        let (rewrite_stages, resolve_stages, config) = match treesitter::lang_yaml(lang_id) {
            Some(yaml) => rules::load_lang(yaml, &lang)?,
            None => (vec![], vec![], Config::default()),
        };
        Ok(Self {
            lang,
            lang_id,
            rewrite_stages,
            resolve_stages,
            config,
            limits,
        })
    }
}
