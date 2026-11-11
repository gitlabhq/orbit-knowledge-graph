use compiler::orbit_authorization::Grants;
use query_data_model::{Catalog, ClickHouse, DuckDb};
use query_engine::pipeline::{AccessLevel, AuthorizedPath, SecurityContext};
use std::sync::Arc;

/// The scope answers the default deployment's `gitlab.session` gives `session`.
pub fn session_grants(session: SecurityContext) -> Grants {
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, embedded_model().model(), session)
        .expect("the default deployment answers the session")
}

pub fn test_ctx() -> Grants {
    session_grants(SecurityContext::new(1, vec!["1/".into()]).unwrap())
}

pub fn admin_ctx() -> Grants {
    session_grants(
        SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
        )
        .unwrap()
        .with_role(true, Some(AccessLevel::Owner as u32)),
    )
}

pub fn embedded_model() -> Arc<Catalog<ClickHouse>> {
    Arc::new(Catalog::<ClickHouse>::embedded().expect("embedded packages must load"))
}

pub fn embedded_local_model() -> Arc<Catalog<DuckDb>> {
    Arc::new(Catalog::<DuckDb>::embedded().expect("embedded packages must load"))
}
