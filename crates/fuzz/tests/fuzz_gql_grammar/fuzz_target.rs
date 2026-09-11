use bolero::check;
use compiler::passes::frontend::gql;
use orbit_fuzz::grammar::{Bytes, Grammar};

fn main() {
    check!().for_each(|input: &[u8]| {
        let Some(text) = Grammar::orbit().derive("Query", &mut Bytes(input)) else {
            return;
        };
        if let Err(error) = gql::parse(&text) {
            assert!(error.is_client_safe(), "{text:?}\n{error}");
            assert!(
                !error.to_string().contains("Orbit query syntax"),
                "{text:?}\n{error}"
            );
        }
    });
}
