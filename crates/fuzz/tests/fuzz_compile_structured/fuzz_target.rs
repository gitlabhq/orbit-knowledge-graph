use bolero::check;
use compiler::{Frontend, SecurityContext, compile};
use orbit_fuzz::generators::FuzzQuery;
use query_data_model::{Catalog, ClickHouse};
use std::sync::{Arc, OnceLock};

fn data_model() -> &'static Arc<Catalog<ClickHouse>> {
    static DATA_MODEL: OnceLock<Arc<Catalog<ClickHouse>>> = OnceLock::new();
    DATA_MODEL
        .get_or_init(|| Arc::new(Catalog::<ClickHouse>::embedded().expect("load embedded model")))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("create security context"))
}

fn main() {
    check!()
        .with_type::<FuzzQuery>()
        .for_each(|query: &FuzzQuery| {
            let _ = compile(&query.json, Frontend::JsonDsl, data_model(), ctx());
        });
}
