use crate::error::LoadError;
use crate::intern::Lang;
use crate::pattern::Rewrite;
use crate::resolver::Resolver;
use crate::rules::{Config, ResolveStage};
use crate::sentinel::{Limits, Sentinel};
use crate::tree::{Edge, Tree};
use crate::treesitter::SupportLang;
use crate::{rules, treesitter};

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve_stages: Vec<ResolveStage>,
    pub config: Config,
    pub limits: Limits,
    /// The run-wide deadline. Every phase checks it beside its own.
    pub sentinel: Sentinel,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Result<Self, LoadError> {
        Self::with_limits(lang_id, Limits::default())
    }

    pub fn with_limits(lang_id: SupportLang, limits: Limits) -> Result<Self, LoadError> {
        let sentinel = Sentinel::new("run", "", limits.total_ms);
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
            sentinel,
        })
    }
}

pub struct State {
    pub trees: Vec<Tree>,
    pub edges: Vec<Edge>,
    pub resolver: Resolver,
}

impl State {
    pub fn new(env: &Env) -> Self {
        Self {
            trees: Vec::new(),
            edges: Vec::new(),
            resolver: Resolver::new(&env.lang),
        }
    }
}
