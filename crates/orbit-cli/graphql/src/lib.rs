#[cynic::schema("gitlab")]
pub mod schema {}

pub mod queries {
    include!(concat!(env!("OUT_DIR"), "/queries.rs"));
}
