use crate::lang::Lang;
use crate::run::LangDef;

const PYTHON_RULES_YAML: &str = include_str!("python.yaml");

pub fn lang_def(lang: &mut Lang) -> LangDef {
    LangDef {
        rewrites: crate::rules::load_rules(PYTHON_RULES_YAML, lang),
    }
}
