use bolero::check;
use compiler::{Frontend, SecurityContext, compile};
use query_data_model::ClickHouseDataModel;
use std::str::from_utf8;
use std::sync::{Arc, OnceLock};

fn data_model() -> &'static Arc<ClickHouseDataModel> {
    static DATA_MODEL: OnceLock<Arc<ClickHouseDataModel>> = OnceLock::new();
    DATA_MODEL
        .get_or_init(|| Arc::new(ClickHouseDataModel::embedded().expect("load embedded model")))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("test ctx"))
}

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Err(error) = compile(s, Frontend::Gql, data_model(), ctx())
        {
            assert!(error.is_client_safe(), "{s:?}\n{error}");
        }
    });
}
