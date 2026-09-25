//! One language's environment: interners, grammar and budgets. Long-lived
//! and shared between runs.

use crate::error::LoadError;
use crate::intern::Lang;
use crate::sentinel::Limits;
use crate::treesitter::SupportLang;

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub limits: Limits,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Ok(Self::with_limits(lang_id, Limits::load()?))
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Self {
        Self::with_lang(lang_id, Lang::new(), limits)
    }

    /// Builds on an existing interner, so ids in a graph restored from a
    /// snapshot and ids the rules use agree.
    pub fn with_lang(lang_id: SupportLang, lang: Lang, limits: Limits) -> Self {
        Self {
            lang,
            lang_id,
            limits,
        }
    }
}
