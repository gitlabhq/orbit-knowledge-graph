use bolero::check;
use compiler::{Ontology, SecurityContext, compile};
use std::str::from_utf8;
use std::sync::OnceLock;

fn ontology() -> &'static Ontology {
    static ONTOLOGY: OnceLock<Ontology> = OnceLock::new();
    ONTOLOGY.get_or_init(|| Ontology::load_embedded().expect("load embedded ontology"))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("create security context"))
}

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(s) = from_utf8(input) {
            let _ = compile(s, ontology(), ctx());
        }
    });
}
