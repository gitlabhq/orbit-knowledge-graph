use cynic::QueryBuilder;
use orbit_cli_graphql::queries::ontology::{
    NodeKind, OrbitSchema, OrbitSchemaEdge as EdgeKind, PageInfo, SchemaPage, SchemaPageVariables,
};
use serde::Serialize;

use super::client::OrbitClient;
use super::error::{EXIT_UNAVAILABLE, RemoteError, build_graphql_error};
use super::{pretty_value, write_stdout};

#[derive(Serialize, Default)]
struct Ontology {
    nodes: Vec<NodeKind>,
    edges: Vec<EdgeKind>,
}

#[derive(Default)]
struct PageCursor {
    after: Option<String>,
    done: bool,
}

pub(crate) async fn run_ontology(nodes: Vec<String>) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let mut ontology = read_ontology(&client, !nodes.is_empty()).await?;
    ontology.keep_properties_of(&nodes);
    write_stdout(&pretty_value(&ontology))
}

async fn read_ontology(
    client: &OrbitClient,
    with_properties: bool,
) -> Result<Ontology, RemoteError> {
    let mut ontology = Ontology::default();
    let (mut nodes, mut edges) = (PageCursor::default(), PageCursor::default());
    while !(nodes.done && edges.done) {
        let schema = read_schema_page(
            client,
            SchemaPageVariables {
                nodes_after: nodes.after.as_deref(),
                edges_after: edges.after.as_deref(),
                read_nodes: !nodes.done,
                read_edges: !edges.done,
                with_properties,
            },
        )
        .await?;
        nodes.read(
            schema.nodes.map(|page| (page.page_info, page.nodes)),
            &mut ontology.nodes,
        );
        edges.read(
            schema.edges.map(|page| (page.page_info, page.nodes)),
            &mut ontology.edges,
        );
    }
    Ok(ontology)
}

async fn read_schema_page(
    client: &OrbitClient,
    variables: SchemaPageVariables<'_>,
) -> Result<OrbitSchema, RemoteError> {
    let response = client.graphql(&SchemaPage::build(variables)).await?;
    if let Some(errors) = response.errors.filter(|errors| !errors.is_empty()) {
        return Err(build_graphql_error("Orbit schema query failed", &errors));
    }
    response
        .data
        .and_then(|data| data.orbit)
        .and_then(|orbit| orbit.schema)
        .ok_or_else(|| {
            RemoteError::new(
                EXIT_UNAVAILABLE,
                "Orbit schema not available\n\n\
                 This GitLab version has no Orbit GraphQL API, or Orbit is not available\n\
                 for your user.",
            )
        })
}

impl Ontology {
    fn keep_properties_of(&mut self, names: &[String]) {
        self.nodes
            .iter_mut()
            .filter(|node| !names.contains(&node.name))
            .for_each(|node| node.properties = None);
    }
}

impl PageCursor {
    fn read<T>(&mut self, page: Option<(PageInfo, Option<Vec<Option<T>>>)>, kinds: &mut Vec<T>) {
        let Some((page_info, page_kinds)) = page else {
            self.done = true;
            return;
        };
        kinds.extend(page_kinds.into_iter().flatten().flatten());
        match page_info {
            PageInfo {
                has_next_page: true,
                end_cursor: Some(cursor),
            } => self.after = Some(cursor),
            _ => self.done = true,
        }
    }
}
