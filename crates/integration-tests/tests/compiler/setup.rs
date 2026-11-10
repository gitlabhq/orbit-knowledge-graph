use compiler::{AccessLevel, AuthorizedPath, SecurityContext};
use query_data_model::{Catalog, ClickHouse, DuckDb};
use std::sync::Arc;

pub fn test_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

pub fn admin_ctx() -> SecurityContext {
    SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32))
}

pub fn embedded_model() -> Arc<Catalog<ClickHouse>> {
    Arc::new(Catalog::<ClickHouse>::embedded().expect("embedded packages must load"))
}

pub fn embedded_local_model() -> Arc<Catalog<DuckDb>> {
    Arc::new(Catalog::<DuckDb>::embedded().expect("embedded packages must load"))
}
