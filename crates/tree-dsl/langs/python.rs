use crate::lang::Lang;
use crate::run::LangDef;

const PYTHON_YAML: &str = include_str!("python.yaml");

pub fn lang_def(lang: &mut Lang) -> LangDef {
    let (rewrites, resolve) = crate::rules::load_lang(PYTHON_YAML, lang);
    LangDef { rewrites, resolve }
}
