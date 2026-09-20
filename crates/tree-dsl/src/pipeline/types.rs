use crate::intern::Lang;
use crate::pattern::Rewrite;
use crate::resolver::Resolver;
use crate::rules::ResolveConfig;
use crate::tree::{Edge, Tree};
use crate::treesitter::SupportLang;
use crate::{rules, treesitter};

pub struct Env {
    pub lang: Lang,
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve_config: ResolveConfig,
}

impl Env {
    pub fn for_lang(lang_id: SupportLang) -> Self {
        let lang = Lang::new();
        let (rewrite_stages, resolve_config) = match treesitter::lang_yaml(lang_id) {
            Some(yaml) => rules::load_lang(yaml, &lang),
            None => (vec![], ResolveConfig::default()),
        };
        Self {
            lang,
            lang_id,
            rewrite_stages,
            resolve_config,
        }
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
