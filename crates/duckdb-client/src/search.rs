use std::collections::HashMap;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use serde_json::{Map, Value};

use crate::{DuckDbClient, i64_column, sql_lit, string_column};
use orbit_search::RecallFilter;
use orbit_search::corpus::{EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex, search_corpus_exts};

pub const GLOB_CHARS: [char; 3] = ['*', '?', '['];

#[derive(Debug, Clone, PartialEq)]
pub struct NodeValue {
    pub entity_type: String,
    pub id: i64,
    pub properties: Map<String, Value>,
}

pub struct NodeHydrator {
    entity_type: String,
    table: String,
    columns: HashMap<String, String>,
    properties: Vec<String>,
}

impl NodeHydrator {
    pub fn new(ontology: &Ontology, entity_type: &str) -> Result<Self> {
        let node = ontology
            .get_node(entity_type)
            .with_context(|| format!("ontology node {entity_type:?} does not exist"))?;
        let columns: HashMap<String, String> = node
            .fields
            .iter()
            .filter_map(|field| Some((field.name.clone(), field.column_name()?.to_string())))
            .collect();
        let properties = if node.default_columns.is_empty() {
            columns
                .keys()
                .filter(|name| *name != "id")
                .cloned()
                .collect()
        } else {
            node.default_columns
                .iter()
                .filter(|name| *name != "id" && columns.contains_key(*name))
                .cloned()
                .collect()
        };
        anyhow::ensure!(
            columns.contains_key("id"),
            "ontology node has no database-backed id property"
        );
        Ok(Self {
            entity_type: node.name.clone(),
            table: node.destination_table.clone(),
            columns,
            properties,
        })
    }

    pub fn embedded(entity_type: &str) -> Result<Self> {
        let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
        Self::new(&ontology, entity_type)
    }

    pub fn column(&self, property: &str) -> Result<&str> {
        self.columns
            .get(property)
            .map(String::as_str)
            .with_context(|| {
                format!(
                    "{property:?} is not a database property of {}",
                    self.entity_type
                )
            })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    fn projection(&self) -> String {
        let properties = self
            .properties
            .iter()
            .map(|property| format!("{property} := n.{}", self.columns[property]))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "n.{} AS id, to_json(struct_pack({properties})) AS properties",
            self.columns["id"]
        )
    }

    pub fn query(
        &self,
        client: &DuckDbClient,
        filters: &[(&str, Value)],
        ids: Option<&[i64]>,
    ) -> Result<Vec<NodeValue>> {
        if ids.is_some_and(<[i64]>::is_empty) {
            return Ok(Vec::new());
        }
        let mut predicates: Vec<String> = filters
            .iter()
            .enumerate()
            .map(|(index, (property, _))| {
                self.column(property)
                    .map(|column| format!("n.{column} = ?{}", index + 1))
            })
            .collect::<Result<_>>()?;
        if let Some(ids) = ids {
            predicates.push(format!("n.{} IN ({})", self.columns["id"], id_list(ids)));
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        let params: Vec<Value> = filters.iter().map(|(_, value)| value.clone()).collect();
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {} FROM {} n{}",
                self.projection(),
                self.table,
                where_clause
            ),
            &params,
        )?;
        let nodes = self.nodes_from_batches(&batches)?;
        let Some(ids) = ids else {
            return Ok(nodes);
        };
        let mut by_id: HashMap<i64, NodeValue> =
            nodes.into_iter().map(|node| (node.id, node)).collect();
        Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
    }

    fn nodes_from_batches(&self, batches: &[RecordBatch]) -> Result<Vec<NodeValue>> {
        i64_column(batches, "id")
            .into_iter()
            .zip(string_column(batches, "properties"))
            .map(|(id, properties)| {
                Ok(NodeValue {
                    entity_type: self.entity_type.clone(),
                    id,
                    properties: serde_json::from_str(&properties)?,
                })
            })
            .collect()
    }
}

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
    node: NodeHydrator,
    paths: Vec<String>,
}

impl DuckDbSearch {
    pub fn scoped(
        client: DuckDbClient,
        project_id: i64,
        commit_sha: &str,
        paths: &[String],
    ) -> Result<Self> {
        Ok(Self {
            client,
            pid: project_id,
            sha: commit_sha.to_string(),
            node: NodeHydrator::embedded("Definition")?,
            paths: paths.to_vec(),
        })
    }

    pub fn client(&self) -> &DuckDbClient {
        &self.client
    }

    pub fn list_corpus(&self, filter: &RecallFilter) -> Result<Vec<NodeValue>> {
        self.client.execute(
            &corpus_table_sql(self.pid, &sql_lit(&self.sha), &self.paths, &self.node)?,
            &[],
        )?;
        let batches = query(
            &self.client,
            &format!(
                "SELECT id
FROM search_corpus
WHERE TRUE
{}
ORDER BY file_path, start_line, end_line DESC, fqn",
                kind_scope("definition_type", &filter.kinds)
            ),
        )?;
        let ids = i64_column(&batches, "id");
        self.node.query(
            &self.client,
            &[
                ("project_id", self.pid.into()),
                ("commit_sha", self.sha.clone().into()),
            ],
            Some(&ids),
        )
    }
}

fn id_list(ids: &[i64]) -> String {
    ids.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn query(client: &DuckDbClient, sql: &str) -> Result<Vec<RecordBatch>> {
    client.query_arrow(sql).with_context(|| {
        let preview: String = sql.chars().take(120).collect();
        let suffix = if sql.chars().count() > 120 { "…" } else { "" };
        format!("query failed: {preview}{suffix}")
    })
}

fn corpus_table_sql(pid: i64, sha: &str, paths: &[String], node: &NodeHydrator) -> Result<String> {
    let id = node.column("id")?;
    let fqn = node.column("fqn")?;
    let kind = node.column("definition_type")?;
    let file = node.column("file_path")?;
    let start = node.column("start_line")?;
    let end = node.column("end_line")?;
    Ok(format!(
        "CREATE OR REPLACE TEMP TABLE search_corpus AS
SELECT d.{id} AS id, d.{name} AS name, d.{fqn} AS fqn, d.{kind} AS definition_type,
       d.{file} AS file_path, d.{start} AS start_line, d.{end} AS end_line
FROM {table} d
WHERE d.{project_id} = {pid} AND d.{commit_sha} = {sha}
  AND regexp_matches(d.{file}, {source_only})
  AND NOT regexp_matches(d.{name}, '^[0-9]+$')
  AND d.{fqn} NOT LIKE '%@%'
{exclude}{paths}",
        table = node.table,
        project_id = node.column("project_id")?,
        commit_sha = node.column("commit_sha")?,
        name = node.column("name")?,
        source_only = sql_lit(&ext_regex(&search_corpus_exts())),
        exclude = if paths.is_empty() {
            exclusions(&format!("d.{file}"))
        } else {
            String::new()
        },
        paths = path_scope(&format!("d.{file}"), paths, false),
    ))
}

pub fn kind_scope(col: &str, kinds: &[String]) -> String {
    if kinds.is_empty() {
        return String::new();
    }
    let list = kinds
        .iter()
        .map(|k| sql_lit(&k.to_lowercase()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("  AND lower({col}) IN ({list})\n")
}

pub fn path_scope(col: &str, paths: &[String], include_excluded: bool) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let alternatives = paths
        .iter()
        .map(|p| {
            let p = p.trim_end_matches('/');
            let literal = format!(
                "{col} = {} OR starts_with({col}, {})",
                sql_lit(p),
                sql_lit(&format!("{p}/"))
            );
            let scope = match p.contains(GLOB_CHARS) {
                true => format!(
                    "{literal} OR {col} GLOB {} OR {col} GLOB {}",
                    sql_lit(p),
                    sql_lit(&format!("{p}/*"))
                ),
                false => literal,
            };
            if include_excluded {
                return format!("({scope})");
            }
            let opted_in = format!(
                "{} OR {}",
                excluded_path_predicate(&sql_lit(p)),
                excluded_path_predicate(&sql_lit(&format!("{p}/")))
            );
            format!(
                "(({scope}) AND ({opted_in} OR NOT {}))",
                excluded_path_predicate(col)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("  AND ({alternatives})\n")
}

fn exclusions(col: &str) -> String {
    format!("  AND NOT {}\n", excluded_path_predicate(col))
}

pub fn excluded_path_predicate(col: &str) -> String {
    let likes = EXCLUDE_LIKE
        .iter()
        .map(|pat| format!("{col} LIKE {}", sql_lit(pat)));
    let regexes = EXCLUDE_REGEX
        .iter()
        .map(|re| format!("regexp_matches({col}, {})", sql_lit(re)));
    format!(
        "({})",
        likes.chain(regexes).collect::<Vec<_>>().join(" OR ")
    )
}
