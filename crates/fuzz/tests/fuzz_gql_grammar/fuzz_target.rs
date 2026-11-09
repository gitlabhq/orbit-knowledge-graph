use std::sync::LazyLock;

use bolero::check;
use compiler::Frontend;
use orbit_fuzz::grammar::{Bytes, Grammar};
use query_data_model::{Catalog, ClickHouse};

static MODEL: LazyLock<Catalog<ClickHouse>> =
    LazyLock::new(|| Catalog::<ClickHouse>::embedded().expect("embedded packages load"));

fn main() {
    let catalog = MODEL.model().logical();
    check!().for_each(|input: &[u8]| {
        let Some(text) = Grammar::orbit().derive("Query", &mut Bytes(input)) else {
            return;
        };
        if let Err(error) = compiler::surface::parse(&text, Frontend::Gql, catalog) {
            assert!(error.is_client_safe(), "{text:?}\n{error}");
            assert!(
                !error.to_string().contains("Orbit query syntax"),
                "{text:?}\n{error}"
            );
        }
    });
}
