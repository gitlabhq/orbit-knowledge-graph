use crate::{PageInfo, schema};

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

#[cfg(test)]
mod tests {
    use cynic::{GraphQlResponse, QueryBuilder};

    use super::*;

    #[test]
    fn orbit_schema_nodes_sends_the_checked_in_query() {
        let operation = OrbitSchemaNodes::build(OrbitSchemaNodesVariables {
            after: Some("MzM".into()),
        });

        assert_eq!(
            operation.query,
            include_str!("../fixtures/orbit_schema_nodes.graphql")
        );
        assert_eq!(
            serde_json::to_value(&operation.variables).unwrap(),
            serde_json::json!({ "after": "MzM" })
        );
    }

    #[test]
    fn orbit_schema_nodes_decodes_a_gitlab_response() {
        let response: GraphQlResponse<OrbitSchemaNodes> =
            serde_json::from_str(include_str!("../fixtures/orbit_schema_nodes.json")).unwrap();

        let connection = response.data.unwrap().orbit.unwrap().schema.unwrap().nodes;
        let kinds: Vec<_> = connection
            .nodes
            .iter()
            .map(|node| (node.name.as_str(), node.domain.name.as_str()))
            .collect();

        assert!(response.errors.is_none());
        assert_eq!(
            kinds,
            [("AgentSession", "agents"), ("Branch", "source_code")]
        );
        assert!(!connection.page_info.has_next_page);
        assert_eq!(connection.page_info.end_cursor.as_deref(), Some("MzM"));
    }
}
