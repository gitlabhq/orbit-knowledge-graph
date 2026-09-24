//! One language's compiled environment: rule stages, config and budgets.
//! Long-lived and shared between runs.

use crate::error::LoadError;
use crate::intern::Lang;
use crate::rules::LangConfig;
use crate::sentinel::Limits;
use crate::treesitter::SupportLang;
use crate::{rules, treesitter};

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub rules: LangConfig,
    pub limits: Limits,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Self::with_limits(lang_id, Limits::default())
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Result<Self, LoadError> {
        let lang = Lang::new();
        let rules = match treesitter::lang_yaml(lang_id) {
            Some(yaml) => rules::load_lang(yaml, &lang)?,
            None => LangConfig::default(),
        };
        Ok(Self {
            lang,
            lang_id,
            rules,
            limits,
        })
    }
}
