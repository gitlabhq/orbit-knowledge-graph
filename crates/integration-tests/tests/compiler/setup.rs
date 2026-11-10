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

pub fn compile_pair(
    json: &str,
    orbit_query: &str,
    data_model: &Arc<Catalog<ClickHouse>>,
    context: &SecurityContext,
) -> compiler::Result<compiler::CompiledQueryContext> {
    let json_result = compiler::compile(json, compiler::Frontend::JsonDsl, data_model, context);
    let orbit_query_result =
        compiler::compile(orbit_query, compiler::Frontend::Gql, data_model, context);
    match (json_result, orbit_query_result) {
        (Ok(json), Ok(orbit_query_result)) => {
            assert_eq!(
                json.base.sql, orbit_query_result.base.sql,
                "SQL differs for {orbit_query}"
            );
            assert_eq!(
                json.base.params, orbit_query_result.base.params,
                "parameters differ for {orbit_query}"
            );
            assert_eq!(json.query_type, orbit_query_result.query_type);
            assert_eq!(
                json.hydration, orbit_query_result.hydration,
                "hydration differs for {orbit_query}"
            );
            Ok(json)
        }
        (Err(json), Err(orbit_query_result)) => {
            assert_eq!(
                std::mem::discriminant(&json),
                std::mem::discriminant(&orbit_query_result),
                "rejection differs for {orbit_query}: JSON={json}; Orbit={orbit_query_result}"
            );
            Err(json)
        }
        (json, orbit_query_result) => panic!(
            "frontend acceptance differs for {orbit_query}: JSON={json:?}; Orbit={orbit_query_result:?}"
        ),
    }
}
