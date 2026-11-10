use std::sync::LazyLock;

static PROMPTS: LazyLock<orbit_prompts::Prompts> = LazyLock::new(|| {
    orbit_prompts::Prompts::load_embedded("local").expect("prompts are validated by build.rs")
});

static REMOTE_PROMPTS: LazyLock<orbit_prompts::Prompts> = LazyLock::new(|| {
    orbit_prompts::Prompts::load_embedded("remote").expect("prompts are validated by build.rs")
});

pub(crate) fn gql_syntax() -> &'static str {
    REMOTE_PROMPTS
        .get("tools/query_graph_gql")
        .expect("config/prompts/remote/tools/query_graph_gql.yml is embedded")
        .description()
}

pub(crate) fn summary(name: &str) -> &'static str {
    prompt(name).summary()
}

pub(crate) fn short(name: &str) -> &'static str {
    prompt(name).short()
}

pub(crate) fn long(name: &str) -> &'static str {
    prompt(name).description()
}

fn prompt(name: &str) -> &'static orbit_prompts::Prompt {
    PROMPTS
        .get(name)
        .unwrap_or_else(|| panic!("prompt `{name}` missing from config/prompts/local"))
}
