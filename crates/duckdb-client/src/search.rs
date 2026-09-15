use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use serde_json::{Map, Value};

use crate::{DuckDbClient, f64_column, i64_column, scalar_i64, sql_lit, string_column};
use orbit_search::corpus::{EXCLUDE_LIKE, EXCLUDE_REGEX, ext_regex, search_corpus_exts};
use orbit_search::grep::{GrepError, GrepSource, grep};
use orbit_search::{
    ANCHOR_SIM, EXACT_NAME_SIM, GrepOutcome, RecallFilter, SearchCandidate, SearchVocab, TermRecall,
};

pub const CONTEXT_SIM_CAP: f64 = 0.99;
pub const NAME_SIM_FLOOR: f64 = ANCHOR_SIM;
pub const NAME_SIM_CEIL: f64 = 0.9999;

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
        table = &node.table,
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
QUALIFY row_number() OVER (
  PARTITION BY {id} ORDER BY {file_path}, {start_byte}, {end_byte}
) = 1
ORDER BY {file_path}, {id}",
            id = node.column("id")?,
            file_path = node.column("file_path")?,
            start_byte = node.column("start_byte")?,
            end_byte = node.column("end_byte")?,
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
            content = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read definition source {}", path.display()))?;
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
        vocab: &SearchVocab,
        filter: &RecallFilter,
    ) -> Result<(GrepOutcome, Vec<NodeValue>)> {
        let outcome = grep(self, query, limit, vocab, filter).map_err(|e| match e {
            GrepError::Source(e) => e,
            e => anyhow::anyhow!("{e}"),
        })?;
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

impl GrepSource for DuckDbSearch {
    type Error = anyhow::Error;

    fn stem(&self, words: &[String]) -> Result<Vec<String>> {
        if words.is_empty() {
            return Ok(Vec::new());
        }
        let values = words
            .iter()
            .enumerate()
            .map(|(i, w)| format!("({i}, {})", sql_lit(&w.to_lowercase())))
            .collect::<Vec<_>>()
            .join(", ");
        let batches = query(
            &self.client,
            &format!(
                "SELECT stem(w, '{FTS_STEMMER}') AS s FROM (VALUES {values}) t(i, w) ORDER BY i"
            ),
        )?;
        Ok(string_column(&batches, "s"))
    }

    fn recall(&self, terms: &[String], filter: &RecallFilter) -> Result<Vec<TermRecall>> {
        let sql = recall_sql(self.pid, &sql_lit(&self.sha), filter);
        terms
            .iter()
            .map(|term| {
                let batches = self
                    .client
                    .query_arrow_json(&sql, &[serde_json::Value::String(term.clone())])
                    .with_context(|| format!("fts recall failed for term {term:?}"))?;
                let ids = i64_column(&batches, "id");
                let sims = f64_column(&batches, "sim");
                let dfs = i64_column(&batches, "df");
                let totals = i64_column(&batches, "total");
                let mut recall = TermRecall {
                    hits: Vec::new(),
                    matched: 0,
                    corpus: 0,
                };
                for i in 0..ids.len() {
                    recall.matched = dfs[i] as u64;
                    recall.corpus = totals[i] as u64;
                    if ids[i] != 0 {
                        recall.hits.push((ids[i], sims[i]));
                    }
                }
                Ok(recall)
            })
            .collect()
    }

    fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<SearchCandidate>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = corpus_rows_sql(
            &format!(
                "cand AS (
  SELECT d.{id} AS id, d.{fqn} AS fqn, d.{file_path} AS file_path
  FROM {table} d
  WHERE d.{project_id} = {pid} AND d.{commit_sha} = {sha} AND d.{id} IN ({list})
)",
                id = self.node.column("id")?,
                fqn = self.node.column("fqn")?,
                file_path = self.node.column("file_path")?,
                table = &self.node.table,
                project_id = self.node.column("project_id")?,
                pid = self.pid,
                commit_sha = self.node.column("commit_sha")?,
                sha = sql_lit(&self.sha),
                list = id_list(ids),
            ),
            self.pid,
            &sql_lit(&self.sha),
        );
        Ok(rows_from_batches(&query(&self.client, &sql)?))
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

fn recall_sql(pid: i64, sha: &str, filter: &RecallFilter) -> String {
    let doc_table = def_doc_table(pid);
    let corpus = format!(
        "SELECT id FROM search_corpus WHERE TRUE\n{}",
        kind_scope("definition_type", &filter.kinds)
    );
    format!(
        "WITH scored AS (
  SELECT def_id AS id,
         fts_main_{doc_table}.match_bm25(def_id, ?1, fields := 'name,context,source') AS score
  FROM {doc_table}
  WHERE commit_sha = {sha}
    AND def_id IN ({corpus})
),
hits AS (
  SELECT s.id, s.score,
         regexp_replace(lower(d.name), '[^0-9a-z]+', ' ', 'g') = regexp_replace(lower(?1), '[^0-9a-z]+', ' ', 'g') AS exact_hit,
         list_contains(
           list_transform(string_split_regex(lower(d.name), '[^0-9a-z]+'), t -> stem(t, '{FTS_STEMMER}')),
           stem(lower(?1), '{FTS_STEMMER}')) AS token_hit
  FROM scored s
  JOIN {doc_table} d ON d.def_id = s.id AND d.commit_sha = {sha}
  WHERE s.score IS NOT NULL
  ORDER BY s.score DESC, s.id
),
df AS (SELECT COUNT(*) AS df FROM scored WHERE score IS NOT NULL),
mx AS (SELECT MAX(score) AS m FROM hits),
corpus_n AS (SELECT GREATEST(COUNT(*), 1) AS total FROM ({corpus}))
SELECT COALESCE(h.id, 0) AS id,
       COALESCE(CASE WHEN h.exact_hit THEN {EXACT_NAME_SIM}
                     WHEN h.token_hit THEN {NAME_SIM_FLOOR} + ({NAME_SIM_CEIL} - {NAME_SIM_FLOOR}) * h.score / mx.m
                     ELSE LEAST(h.score / mx.m, {CONTEXT_SIM_CAP}) END, 0.0) AS sim,
       CAST(df.df AS BIGINT) AS df,
       CAST(corpus_n.total AS BIGINT) AS total
FROM df
CROSS JOIN corpus_n
CROSS JOIN mx
LEFT JOIN hits h ON TRUE
ORDER BY sim DESC, id"
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
SELECT d.{id} AS id, d.{fqn} AS fqn, d.{kind} AS definition_type,
       d.{file} AS file_path, d.{start} AS start_line, d.{end} AS end_line
FROM {table} d
WHERE d.{project_id} = {pid} AND d.{commit_sha} = {sha}
  AND regexp_matches(d.{file}, {source_only})
  AND NOT regexp_matches(d.{name}, '^[0-9]+$')
  AND d.{fqn} NOT LIKE '%@%'
{exclude}{paths}",
        table = &node.table,
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

fn corpus_rows_sql(cand_ctes: &str, pid: i64, sha: &str) -> String {
    let doc_table = def_doc_table(pid);
    format!(
        "WITH {cand_ctes},
deg AS (
  SELECT id, COUNT(*) AS degree FROM (
    SELECT source_id AS id FROM gl_edge WHERE source_id IN (SELECT id FROM cand)
    UNION ALL
    SELECT target_id FROM gl_edge WHERE target_id IN (SELECT id FROM cand)
  ) GROUP BY 1
),
lens AS (
  SELECT def_id, CAST(len(string_split(context, ' ')) AS BIGINT) AS grams
  FROM {doc_table}
  WHERE commit_sha = {sha}
    AND def_id IN (SELECT id FROM cand)
)
SELECT c.id, c.fqn, c.file_path, COALESCE(deg.degree, 0) AS degree,
       COALESCE(lens.grams, 0) AS grams
FROM cand c
LEFT JOIN deg ON deg.id = c.id
LEFT JOIN lens ON lens.def_id = c.id"
    )
}

fn rows_from_batches(batches: &[RecordBatch]) -> Vec<SearchCandidate> {
    let ids = i64_column(batches, "id");
    let fqns = string_column(batches, "fqn");
    let files = string_column(batches, "file_path");
    let degrees = i64_column(batches, "degree");
    let grams = i64_column(batches, "grams");
    (0..ids.len())
        .map(|i| SearchCandidate {
            id: ids[i],
            label: fqns[i].clone(),
            parent_group: definition_parent(&fqns[i]),
            diversity_group: files[i].clone(),
            degree: degrees[i] as u64,
            document_length: grams[i] as u64,
        })
        .collect()
}

fn definition_parent(fqn: &str) -> String {
    fqn.rfind("::")
        .or_else(|| fqn.rfind('.'))
        .map_or(fqn, |index| &fqn[..index])
        .to_string()
}
