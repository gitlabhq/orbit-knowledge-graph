use bolero::check;
use compiler::compile;
use compiler::orbit_authorization::Grants;
use query_data_model::{Catalog, ClickHouse};
use std::str::from_utf8;
use std::sync::{Arc, OnceLock};

fn data_model() -> &'static Arc<Catalog<ClickHouse>> {
    static DATA_MODEL: OnceLock<Arc<Catalog<ClickHouse>>> = OnceLock::new();
    DATA_MODEL
        .get_or_init(|| Arc::new(Catalog::<ClickHouse>::embedded().expect("load embedded model")))
}

fn ctx() -> &'static Grants {
    static CTX: OnceLock<Grants> = OnceLock::new();
    CTX.get_or_init(|| compiler::uniform_grants(data_model().model(), false, &["1/"]))
}

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Err(error) = compile(s, data_model(), ctx())
        {
            assert!(error.is_client_safe(), "{s:?}\n{error}");
        }
    });
}
