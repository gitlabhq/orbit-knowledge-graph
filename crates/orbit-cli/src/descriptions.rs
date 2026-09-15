use std::sync::LazyLock;

static PROMPTS: LazyLock<orbit_prompts::Prompts> = LazyLock::new(|| {
    orbit_prompts::Prompts::load_embedded("local").expect("prompts are validated by build.rs")
});

pub(crate) fn short(name: &str) -> &'static str {
    prompt(name).short()
}

pub(crate) fn mcp(name: &str) -> &'static str {
    prompt(name).description()
}

fn prompt(name: &str) -> &'static orbit_prompts::Prompt {
    PROMPTS
        .get(name)
        .unwrap_or_else(|| panic!("prompt `{name}` missing from config/prompts/local"))
}
