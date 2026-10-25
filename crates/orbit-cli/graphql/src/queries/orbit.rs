use super::PageInfo;
use crate::schema;

#[derive(cynic::QueryVariables, Debug, Default)]
pub struct OrbitSchemaNodesVariables {
    pub after: Option<String>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Query", variables = "OrbitSchemaNodesVariables")]
pub struct OrbitSchemaNodes {
    #[directives(gl_introduced(version: "19.6.0"))]
    pub orbit: Option<Orbit>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(variables = "OrbitSchemaNodesVariables")]
pub struct Orbit {
    pub schema: Option<OrbitSchema>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(variables = "OrbitSchemaNodesVariables")]
pub struct OrbitSchema {
    #[arguments(after: $after)]
    pub nodes: OrbitSchemaNodeConnection,
}

#[derive(cynic::QueryFragment, Debug)]
pub struct OrbitSchemaNodeConnection {
    pub page_info: PageInfo,
    #[cynic(flatten)]
    pub nodes: Vec<OrbitSchemaNode>,
}

#[derive(cynic::QueryFragment, Debug)]
pub struct OrbitSchemaNode {
    pub name: String,
    pub description: Option<String>,
    pub domain: OrbitSchemaDomain,
}

#[derive(cynic::QueryFragment, Debug)]
pub struct OrbitSchemaDomain {
    pub name: String,
}
