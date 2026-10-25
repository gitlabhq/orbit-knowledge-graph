pub mod orbit;

use crate::schema;

#[derive(cynic::QueryFragment, Debug)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}
