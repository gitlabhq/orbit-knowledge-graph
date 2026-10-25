pub mod orbit;

#[cynic::schema("gitlab")]
pub mod schema {}

#[derive(cynic::QueryFragment, Debug)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}
