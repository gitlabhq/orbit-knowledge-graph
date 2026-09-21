use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use serde_json::{Map, Value};

use crate::{
    DuckDbClient, bool_column, f64_column, i64_column, scalar_i64, sql_lit, string_column,
};
use orbit_search::corpus::{EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex, search_corpus_exts};
use orbit_search::{GrepMatch, GrepOutcome, RecallFilter, query_alternatives};

pub const FTS_STEMMER: &str = "english";

pub const DEF_DOC_PREFIX: &str = "gl_def_doc_";

const DEF_SOURCE_TABLE: &str = "search_def_source";

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

pub fn def_doc_table(project_id: i64) -> String {
    format!("{DEF_DOC_PREFIX}{project_id}")
}

pub fn def_doc_sql(doc_table: &str, ontology: &Ontology) -> Result<String> {
    let node = NodeHydrator::new(ontology, "Definition")?;
    Ok(format!(
        "CREATE OR REPLACE TABLE {doc_table} AS
SELECT DISTINCT {commit_sha} AS commit_sha, {id} AS def_id,
       fts_doc(def_name({fqn})) AS name,
       fts_doc({fqn} || ' ' || {file_path}) AS context,
       '' AS source
FROM {table} WHERE {project_id} = ?1 AND {commit_sha} = ?2",
        commit_sha = node.column("commit_sha")?,
        id = node.column("id")?,
        fqn = node.column("fqn")?,
        file_path = node.column("file_path")?,
        table = node.table,
        project_id = node.column("project_id")?,
    ))
}

pub fn populate_def_doc_sources(
    client: &DuckDbClient,
    doc_table: &str,
    ontology: &Ontology,
    repository_root: &Path,
    project_id: i64,
    commit_sha: &str,
) -> Result<()> {
    let node = NodeHydrator::new(ontology, "Definition")?;
    let batches = client.query_arrow_json(
        &format!(
            "SELECT {id} AS def_id, {file_path} AS file_path,
       {start_byte} AS start_byte, {end_byte} AS end_byte
FROM {table}
WHERE {project_id} = ?1 AND {commit_sha} = ?2
  AND regexp_matches({file_path}, {source_only})
QUALIFY row_number() OVER (
  PARTITION BY {id} ORDER BY {file_path}, {start_byte}, {end_byte}
) = 1
ORDER BY {file_path}, {id}",
            id = node.column("id")?,
            file_path = node.column("file_path")?,
            start_byte = node.column("start_byte")?,
            end_byte = node.column("end_byte")?,
            source_only = sql_lit(&ext_regex(&search_corpus_exts())),
            table = node.table(),
            project_id = node.column("project_id")?,
            commit_sha = node.column("commit_sha")?,
        ),
        &[project_id.into(), commit_sha.into()],
    )?;
    let ids = i64_column(&batches, "def_id");
    let paths = string_column(&batches, "file_path");
    let starts = i64_column(&batches, "start_byte");
    let ends = i64_column(&batches, "end_byte");
    anyhow::ensure!(
        ids.len() == paths.len() && ids.len() == starts.len() && ids.len() == ends.len(),
        "definition source metadata columns have unequal lengths"
    );
    if ids.is_empty() {
        return Ok(());
    }

    let mut current_path = String::new();
    let mut content = String::new();
    let mut sources = StringBuilder::new();
    for index in 0..ids.len() {
        if current_path != paths[index] {
            current_path.clone_from(&paths[index]);
            let path = repository_root.join(&current_path);
            content = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                eprintln!(
                    "warning: skipping body search for {}: {error}",
                    path.display()
                );
                String::new()
            });
        }
        let source = match (usize::try_from(starts[index]), usize::try_from(ends[index])) {
            (Ok(start), Ok(end)) if start < content.len() => content
                .get(start..end.min(content.len()))
                .unwrap_or_default(),
            _ => "",
        };
        sources.append_value(source);
    }

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("def_id", DataType::Int64, false),
            Field::new("source", DataType::Utf8, false),
        ])),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(sources.finish())],
    )?;
    client.execute(
        &format!("CREATE OR REPLACE TEMP TABLE {DEF_SOURCE_TABLE} (def_id BIGINT, source VARCHAR)"),
        &[],
    )?;
    client.insert_batch(DEF_SOURCE_TABLE, &batch)?;
    client.execute(
        &format!(
            "UPDATE {doc_table} AS d
SET source = s.source
FROM {DEF_SOURCE_TABLE} AS s
WHERE d.def_id = s.def_id"
        ),
        &[],
    )?;
    client.execute(&format!("DROP TABLE {DEF_SOURCE_TABLE}"), &[])?;
    Ok(())
}

pub fn create_fts_index_sql(doc_table: &str) -> String {
    format!(
        "PRAGMA create_fts_index('{doc_table}', 'def_id', 'name', 'context', 'source', stemmer='{FTS_STEMMER}', stopwords='none', overwrite=1)"
    )
}

pub struct DuckDbSearch {
    client: DuckDbClient,
    pid: i64,
    sha: String,
    node: NodeHydrator,
}

impl DuckDbSearch {
    pub fn scoped(
        client: DuckDbClient,
        project_id: i64,
        commit_sha: &str,
        paths: &[String],
    ) -> Result<Self> {
        let sha = sql_lit(commit_sha);
        let node = NodeHydrator::embedded("Definition")?;
        client.load_extension("fts")?;
        ensure_search_index(&client, project_id, &sha)?;
        client.execute(&corpus_table_sql(project_id, &sha, paths, &node)?, &[])?;
        Ok(Self {
            client,
            pid: project_id,
            sha: commit_sha.to_string(),
            node,
        })
    }

    pub fn client(&self) -> &DuckDbClient {
        &self.client
    }

    pub fn grep(
        &self,
        query: &str,
        limit: usize,
        filter: &RecallFilter,
    ) -> Result<(GrepOutcome, Vec<NodeValue>)> {
        anyhow::ensure!(limit > 0, "search limit must be positive");
        let alternatives = query_alternatives(query).map_err(anyhow::Error::msg)?;
        let params: Vec<Value> = alternatives.iter().cloned().map(Value::String).collect();
        let batches = self.client.query_arrow_json(
            &recall_sql(self.pid, &self.sha, &alternatives, limit, filter),
            &params,
        )?;
        let total = i64_column(&batches, "total")[0] as usize;
        let exact_indices: Vec<usize> =
            serde_json::from_str(&string_column(&batches, "exact_alternatives")[0])?;
        let ids = i64_column(&batches, "id");
        let scores = f64_column(&batches, "score");
        let exact_names = bool_column(&batches, "exact_name");
        let name_matches = bool_column(&batches, "name_match");
        let body_offsets = i64_column(&batches, "body_offset");
        let body_texts = string_column(&batches, "body_text");
        let mentions = i64_column(&batches, "mentions");
        let matches = (0..if total == 0 { 0 } else { ids.len() })
            .map(|i| GrepMatch {
                id: ids[i],
                score: scores[i],
                exact_name: exact_names[i],
                name_match: name_matches[i],
                body_offset: usize::try_from(body_offsets[i])
                    .ok()
                    .filter(|&offset| offset > 0),
                body_text: body_texts[i].clone(),
                mentions: usize::try_from(mentions[i]).unwrap_or(0),
            })
            .collect();
        let outcome = GrepOutcome {
            exact_alternatives: exact_indices
                .iter()
                .map(|&i| alternatives[i].clone())
                .collect(),
            alternatives,
            matches,
            total,
        };
        let ids: Vec<i64> = outcome.matches.iter().map(|hit| hit.id).collect();
        let nodes = self.node.query(
            &self.client,
            &[
                ("project_id", self.pid.into()),
                ("commit_sha", self.sha.clone().into()),
            ],
            Some(&ids),
        )?;
        Ok((outcome, nodes))
    }

    pub fn list_corpus(&self, filter: &RecallFilter) -> Result<Vec<NodeValue>> {
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

fn ensure_search_index(client: &DuckDbClient, project_id: i64, sha: &str) -> Result<()> {
    let doc_table = def_doc_table(project_id);
    let table_exists = scalar_i64(&client.query_arrow(&format!(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_tables()
  WHERE table_name = {}",
        sql_lit(&doc_table)
    ))?) > 0;
    let has_source = table_exists
        && scalar_i64(&client.query_arrow(&format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_columns()
             WHERE table_name = {} AND column_name = 'source'",
            sql_lit(&doc_table)
        ))?) > 0;
    let indexed = has_source
        && scalar_i64(&client.query_arrow(&format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (
  SELECT 1 FROM {doc_table}
  WHERE commit_sha = {sha}
  LIMIT 1
)"
        ))?) > 0;
    if !indexed {
        anyhow::bail!(
            "local graph has no search index for this commit; \
             re-index the repository (`orbit index <path>`)"
        );
    }
    Ok(())
}

fn recall_sql(
    pid: i64,
    sha: &str,
    alternatives: &[String],
    limit: usize,
    filter: &RecallFilter,
) -> String {
    let doc_table = def_doc_table(pid);
    let sha = sql_lit(sha);
    let scored = alternatives.iter().enumerate().map(|(i, _)| {
        let param = i + 1;
        let query = format!("array_to_string(list_filter(fts_main_{doc_table}.tokenize(?{param}), token -> token <> ''), ' ')");
        let phrase = format!("regexp_matches(?{param}, '\\s')");
        let term = format!("lower(?{param})");
        format!(
            "SELECT id, alternative, exact_name, score, name_match, body_offset, mentions,
       trim(lines[body_offset]) AS body_text FROM (
  SELECT c.id, {i} AS alternative, lower(c.name) = {term} AS exact_name,
       CASE WHEN {phrase} OR contains(lower(d.context || ' ' || d.source), {term})
            THEN fts_main_{doc_table}.match_bm25(c.id, {query}, fields := 'name,context,source', conjunctive := true) END AS score,
       ({phrase} OR contains(lower(d.context), {term}))
       AND fts_main_{doc_table}.match_bm25(c.id, {query}, fields := 'name,context', conjunctive := true) IS NOT NULL AS name_match,
       string_split(d.source, chr(10)) AS lines,
       CASE WHEN NOT {phrase} THEN CAST(list_position(
            list_transform(string_split(d.source, chr(10)), line -> contains(lower(line), {term})), true) AS BIGINT) END AS body_offset,
       CASE WHEN NOT {phrase} THEN CAST((length(lower(d.source)) - length(replace(lower(d.source), {term}, ''))) // length(?{param}) AS BIGINT) END AS mentions
  FROM search_corpus c JOIN {doc_table} d ON d.def_id = c.id AND d.commit_sha = {sha}
  WHERE TRUE
{})", kind_scope("definition_type", &filter.kinds))
    }).collect::<Vec<_>>().join("\nUNION ALL\n");
    format!(
        "WITH scored AS ({scored}),
hits AS (
  SELECT id, max(score) AS score, bool_or(exact_name) AS exact_name,
         bool_or(name_match) AS name_match,
         arg_max(body_offset, COALESCE(score, -1e9)) AS body_offset,
         arg_max(body_text, COALESCE(score, -1e9)) AS body_text,
         arg_max(mentions, COALESCE(score, -1e9)) AS mentions
  FROM scored GROUP BY id HAVING count(score) > 0
),
limited AS (
  SELECT * FROM hits ORDER BY exact_name DESC, name_match DESC, score DESC, id LIMIT {limit}
),
stats AS (SELECT count(*) AS total FROM hits),
exact AS (
  SELECT COALESCE(to_json(list(DISTINCT alternative ORDER BY alternative)
         FILTER (WHERE exact_name)), '[]') AS exact_alternatives FROM scored
)
SELECT COALESCE(id, 0) AS id, COALESCE(score, 0.0) AS score,
       COALESCE(exact_name, false) AS exact_name, COALESCE(name_match, false) AS name_match,
       COALESCE(body_offset, 0) AS body_offset, COALESCE(body_text, '') AS body_text,
       COALESCE(mentions, 0) AS mentions, total, exact_alternatives
FROM stats CROSS JOIN exact LEFT JOIN limited ON TRUE
ORDER BY exact_name DESC, name_match DESC, score DESC, id"
    )
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
            let scope = if p.contains(GLOB_CHARS) {
                format!("{col} GLOB {}", sql_lit(p))
            } else {
                format!(
                    "{col} = {} OR {col} GLOB {}",
                    sql_lit(p),
                    sql_lit(&format!("{p}/*"))
                )
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
