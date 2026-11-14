use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use query_data_model::{Catalog, ClickHouse, DataType, Packages};

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub enum_values: Option<BTreeMap<i64, String>>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub name: String,
    pub table: String,
    pub fields: Vec<Field>,
    pub sort_key: Vec<String>,
}

/// The entities, tables, and relationship variants the synthetic data covers.
#[derive(Debug, Clone)]
pub struct Graph {
    nodes: Vec<Node>,
    edges: BTreeSet<(String, String, String)>,
    edge_table: String,
}

pub fn load_catalog(path: impl AsRef<Path>) -> Result<Catalog<ClickHouse>> {
    let path = path.as_ref();
    let packages = Packages::read_dir(path, query_data_model::Tier::Platform)
        .with_context(|| format!("failed to read model packages from {}", path.display()))?;
    Ok(Catalog::<ClickHouse>::from_model(
        &packages.deployment("server")?,
    )?)
}

impl Graph {
    #[cfg(test)]
    pub fn new(nodes: Vec<Node>, edge_table: &str) -> Self {
        Self {
            nodes,
            edges: BTreeSet::new(),
            edge_table: edge_table.to_string(),
        }
    }

    #[cfg(test)]
    pub fn embedded() -> Result<Self> {
        Self::from_catalog(&Catalog::<ClickHouse>::embedded("server")?)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_catalog(&load_catalog(path)?)
    }

    pub fn from_catalog(catalog: &Catalog<ClickHouse>) -> Result<Self> {
        let graph = catalog.graph();
        let nodes = graph
            .entities()
            .filter_map(|entity| {
                Some(Node {
                    name: entity.name.clone(),
                    table: catalog.table_of(entity.id)?.to_string(),
                    fields: entity
                        .properties
                        .iter()
                        .map(|property| {
                            let property = graph.property(*property);
                            Field {
                                name: property.name.clone(),
                                data_type: property.data_type,
                                nullable: property.nullable,
                                enum_values: property.enum_values.clone(),
                            }
                        })
                        .collect(),
                    sort_key: catalog.sort_key(entity.id).unwrap_or_default().to_vec(),
                })
            })
            .collect();
        let edges = graph
            .variants()
            .map(|variant| {
                (
                    graph.relationship(variant.relationship).name.clone(),
                    graph.entity(variant.source).name.clone(),
                    graph.entity(variant.target).name.clone(),
                )
            })
            .collect();
        let edge_table = catalog
            .shared_edge_table()
            .context("the data model shares no edge table")?
            .to_string();
        Ok(Self {
            nodes,
            edges,
            edge_table,
        })
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    pub fn node(&self, name: &str) -> Option<&Node> {
        self.nodes.iter().find(|node| node.name == name)
    }

    pub fn table_name(&self, name: &str) -> Result<&str> {
        self.node(name)
            .map(|node| node.table.as_str())
            .ok_or_else(|| anyhow!("unknown node label \"{name}\""))
    }

    pub fn has_edge(&self, relationship: &str, source: &str, target: &str) -> bool {
        self.edges.contains(&(
            relationship.to_string(),
            source.to_string(),
            target.to_string(),
        ))
    }

    pub fn edge_type_count(&self) -> usize {
        self.edges
            .iter()
            .map(|(relationship, _, _)| relationship)
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn edge_table(&self) -> &str {
        &self.edge_table
    }
}
