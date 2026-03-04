#![no_main]

use libfuzzer_sys::fuzz_target;
use once_cell::sync::Lazy;
use ontology::Ontology;
use query_engine::{compile, SecurityContext};

static ONTOLOGY: Lazy<Ontology> = Lazy::new(|| Ontology::load_embedded().unwrap());
static CTX: Lazy<SecurityContext> =
    Lazy::new(|| SecurityContext::new(1, vec!["1/".into()]).unwrap());

fuzz_target!(|data: &str| {
    let _ = compile(data, &ONTOLOGY, &CTX);
});
