use std::collections::HashSet;

use cynic::QueryBuilder;
use orbit_cli_graphql::queries::ontology::{
    NodeKind, OrbitSchema, OrbitSchemaEdge as EdgeKind, PageInfo, SchemaPage, SchemaPageVariables,
};
use serde::Serialize;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, EXIT_UNAVAILABLE, RemoteError, build_graphql_error};
use super::legacy;
use super::{pretty_value, write_stdout};

const EXPAND_ALL: &str = "*";

#[derive(Serialize, Default)]
struct Ontology {
    nodes: Vec<NodeKind>,
    edges: Vec<EdgeKind>,
}

#[derive(Default)]
struct PageCursor {
    after: Option<String>,
    seen: HashSet<String>,
    done: bool,
}

pub(crate) async fn run_ontology(nodes: Vec<String>) -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let Some(mut ontology) = read_ontology(&client, !nodes.is_empty()).await? else {
        return legacy::run_ontology(&client, &nodes).await;
    };
    ontology.keep_properties_of(&nodes);
    write_stdout(&pretty_value(&ontology))
}

async fn read_ontology(
    client: &OrbitClient,
    with_properties: bool,
) -> Result<Option<Ontology>, RemoteError> {
    let mut ontology = Ontology::default();
    let (mut nodes, mut edges) = (PageCursor::default(), PageCursor::default());
    while !(nodes.done && edges.done) {
        let Some(schema) = read_schema_page(
            client,
            SchemaPageVariables {
                nodes_after: nodes.after.as_deref(),
                edges_after: edges.after.as_deref(),
                read_nodes: !nodes.done,
                read_edges: !edges.done,
                with_properties,
            },
        )
        .await?
        else {
            return Ok(None);
        };
        nodes.read(
            schema.nodes.map(|page| (page.page_info, page.nodes)),
            &mut ontology.nodes,
        )?;
        edges.read(
            schema.edges.map(|page| (page.page_info, page.nodes)),
            &mut ontology.edges,
        )?;
    }
    Ok(Some(ontology))
}

async fn read_schema_page(
    client: &OrbitClient,
    variables: SchemaPageVariables<'_>,
) -> Result<Option<OrbitSchema>, RemoteError> {
    let Some(response) = client.graphql(&SchemaPage::build(variables)).await? else {
        return Ok(None);
    };
    if let Some(errors) = response.errors.filter(|errors| !errors.is_empty()) {
        return Err(build_graphql_error("Orbit schema query failed", &errors));
    }
    let Some(orbit) = response.data.and_then(|data| data.orbit) else {
        return Ok(None);
    };
    orbit.schema.map(Some).ok_or_else(|| {
        RemoteError::new(
            EXIT_UNAVAILABLE,
            "Orbit schema not available\n\n\
             The Orbit GraphQL API returned no schema for your user.",
        )
    })
}

impl Ontology {
    fn keep_properties_of(&mut self, names: &[String]) {
        if names.iter().any(|name| name == EXPAND_ALL) {
            return;
        }
        self.nodes
            .iter_mut()
            .filter(|node| !names.contains(&node.name))
            .for_each(|node| node.properties = None);
    }
}

impl PageCursor {
    fn read<T>(
        &mut self,
        page: Option<(PageInfo, Option<Vec<Option<T>>>)>,
        kinds: &mut Vec<T>,
    ) -> Result<(), RemoteError> {
        let Some((page_info, page_kinds)) = page else {
            self.done = true;
            return Ok(());
        };
        kinds.extend(page_kinds.into_iter().flatten().flatten());
        match page_info {
            PageInfo {
                has_next_page: true,
                end_cursor: Some(cursor),
            } => self.advance(cursor),
            _ => {
                self.done = true;
                Ok(())
            }
        }
    }

    fn advance(&mut self, cursor: String) -> Result<(), RemoteError> {
        if !self.seen.insert(cursor.clone()) {
            return Err(RemoteError::new(
                EXIT_GENERIC,
                format!(
                    "Orbit schema paging did not advance\n\n\
                     The server sent the page cursor `{cursor}` again."
                ),
            ));
        }
        self.after = Some(cursor);
        Ok(())
    }
}
