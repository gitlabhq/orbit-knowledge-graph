#[cynic::schema("gitlab")]
pub mod schema {}

pub mod queries {
    include!(concat!(env!("OUT_DIR"), "/queries.rs"));
}

pub const ORBIT_SINCE: [(u64, u64, u64); 3] = [(19, 4, 7), (19, 5, 5), (19, 6, 0)];
