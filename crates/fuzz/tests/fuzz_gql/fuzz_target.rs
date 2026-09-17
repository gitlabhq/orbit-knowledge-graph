use bolero::check;
use compiler::{Frontend, Ontology, SecurityContext, compile};
use std::str::from_utf8;
use std::sync::{Arc, OnceLock};

fn ontology() -> &'static Arc<Ontology> {
    static ONTOLOGY: OnceLock<Arc<Ontology>> = OnceLock::new();
    ONTOLOGY.get_or_init(|| Arc::new(Ontology::load_embedded().expect("load embedded ontology")))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("test ctx"))
}

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input)
            && let Err(error) = compile(s, Frontend::Gql, ontology(), ctx())
        {
            assert!(error.is_client_safe(), "{s:?}\n{error}");
        }
    });
}
