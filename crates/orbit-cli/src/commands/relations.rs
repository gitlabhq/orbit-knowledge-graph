use std::fmt::Write as _;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use duckdb_client::search::{NodeHydrator, NodeValue, excluded_path_predicate};
use duckdb_client::{DuckDbClient, bool_column, i64_column, sql_lit, string_column};

use crate::commands::{context, setup::spec};

const FILE_CONNECTION_LIMIT: usize = 10;
use crate::workspace;

fn labels_cte(definition: &NodeHydrator) -> Result<String> {
    Ok(format!(
        "labels AS (
  SELECT {id} AS id, {fqn} AS label,
         {file} || ':' || CAST({start} AS VARCHAR) AS loc, {file} AS path,
         'Definition:' || CAST({id} AS VARCHAR) AS reference, {kind} AS def_kind
  FROM {table} WHERE {project} = ?1 AND {commit} = ?2
  UNION ALL
  SELECT id, path, '', path, 'File:' || CAST(id AS VARCHAR), ''
  FROM gl_file WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT id, path, '', path, '', '' FROM gl_directory WHERE project_id = ?1 AND commit_sha = ?2
  UNION ALL
  SELECT i.id, concat_ws(' from ', NULLIF(i.name, ''), NULLIF(i.import_path, '')),
         COALESCE(d.{file} || ':' || CAST(d.{start} AS VARCHAR), ''),
         COALESCE(d.{file}, i.file_path),
         COALESCE('Definition:' || CAST(d.{id} AS VARCHAR), 'external/unresolved'), ''
  FROM gl_import i
  LEFT JOIN gl_edge e ON e.source_id = i.id AND e.relationship_kind = 'RESOLVES_TO'
  LEFT JOIN {table} d ON d.{id} = e.target_id AND d.{project} = ?1 AND d.{commit} = ?2
  WHERE i.project_id = ?1 AND i.commit_sha = ?2
), import_files AS (
  SELECT i.id, f.id AS file_id FROM gl_import i
  JOIN gl_file f ON f.path = i.file_path AND f.project_id = ?1 AND f.commit_sha = ?2
  WHERE i.project_id = ?1 AND i.commit_sha = ?2
)",
        id = definition.column("id")?,
        fqn = definition.column("fqn")?,
        file = definition.column("file_path")?,
        start = definition.column("start_line")?,
        kind = definition.column("kind")?,
        table = definition.table(),
        project = definition.column("project_id")?,
        commit = definition.column("commit_sha")?,
    ))
}

struct Row {
    target_id: i64,
    kind: String,
    dir: String,
    label: String,
    reference: String,
    loc: String,
    def_kind: String,
    via: String,
    hidden: bool,
    direct: bool,
}

fn rows_from(batches: &[RecordBatch], direct: bool) -> Vec<Row> {
    let target_ids = i64_column(batches, "target_id");
    let kinds = string_column(batches, "kind");
    let dirs = string_column(batches, "dir");
    let labels = string_column(batches, "label");
    let references = string_column(batches, "reference");
    let locs = string_column(batches, "loc");
    let def_kinds = string_column(batches, "def_kind");
    let vias = string_column(batches, "via");
    let hidden = bool_column(batches, "hidden");
    (0..kinds.len())
        .map(|i| Row {
            target_id: target_ids[i],
            kind: kinds[i].clone(),
            dir: dirs[i].clone(),
            label: labels[i].clone(),
            reference: references[i].clone(),
            loc: locs[i].clone(),
            def_kind: def_kinds[i].clone(),
            via: vias[i].clone(),
            hidden: hidden[i],
            direct,
        })
        .collect()
}

pub(crate) fn render(
    client: &DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    nodes: &[NodeValue],
    file_members: &[context::SourceRange],
) -> Result<String> {
    if nodes.is_empty() {
        return Ok(String::new());
    }
    let labels_cte = labels_cte(hydrator)?;
    let definition_id = hydrator.column("id")?;
    let definition_file = hydrator.column("file_path")?;
    let definition_table = hydrator.table();
    let hidden_expr = format!("COALESCE({}, FALSE)", excluded_path_predicate("l.path"));
    let targets = nodes
        .iter()
        .enumerate()
        .map(|(order, node)| {
            let path = node
                .properties
                .get("path")
                .and_then(serde_json::Value::as_str);
            format!(
                "({order}, {}, {})",
                node.id,
                path.map(sql_lit).unwrap_or_else(|| "NULL".into())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let params = [git.project_id.into(), git.commit_sha.clone().into()];
    let edges = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id, path) AS (VALUES {targets})
SELECT DISTINCT t.ord, t.target_id, e.relationship_kind AS kind,
       CASE WHEN e.source_id = t.target_id THEN '-->' ELSE '<--' END AS dir,
       l.label, l.reference, l.loc, l.def_kind, '' AS via, {hidden_expr} AS hidden
FROM targets t
JOIN gl_edge e ON e.source_id = t.target_id OR e.target_id = t.target_id
LEFT JOIN import_files i ON i.id = e.source_id
JOIN labels l ON l.id = CASE WHEN e.source_id = t.target_id THEN e.target_id ELSE COALESCE(i.file_id, e.source_id) END
WHERE e.relationship_kind <> 'DEFINES'
  AND (t.path IS NULL OR l.path <> t.path OR l.reference = 'external/unresolved')
ORDER BY t.ord, kind, dir DESC, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let via = client.query_arrow_json(
        &format!(
            "WITH {labels_cte}, targets(ord, target_id, path) AS (VALUES {targets}),
members AS (
  SELECT t.ord, t.target_id, t.path, e.target_id AS id FROM targets t
  JOIN gl_edge e ON e.source_id = t.target_id AND e.relationship_kind = 'DEFINES'
  WHERE t.path IS NULL
  UNION ALL
  SELECT t.ord, t.target_id, t.path, d.{definition_id} FROM targets t
  JOIN {definition_table} d ON d.{definition_file} = t.path
  WHERE d.project_id = ?1 AND d.commit_sha = ?2
)
SELECT members.target_id, e.relationship_kind AS kind,
       CASE WHEN e.source_id = members.id THEN '-->' ELSE '<--' END AS dir,
       l.label, l.reference, l.loc, l.def_kind,
       string_agg(DISTINCT def_name(m.label), ', ' ORDER BY def_name(m.label)) AS via,
       {hidden_expr} AS hidden
FROM gl_edge e
JOIN members ON members.id = e.target_id OR (members.path IS NOT NULL AND members.id = e.source_id)
JOIN labels m ON m.id = members.id
LEFT JOIN import_files i ON i.id = e.source_id
JOIN labels l ON l.id = CASE WHEN e.source_id = members.id THEN e.target_id ELSE COALESCE(i.file_id, e.source_id) END
WHERE e.relationship_kind <> 'DEFINES'
  AND l.id <> members.target_id
  AND (members.path IS NULL OR l.path <> members.path OR l.reference = 'external/unresolved')
  AND l.id NOT IN (
    SELECT own.id FROM members own WHERE own.target_id = members.target_id
  )
GROUP BY members.ord, members.target_id, kind, dir, l.label, l.reference, l.loc, l.def_kind, l.path
ORDER BY members.ord, kind, dir DESC, l.path, l.label, l.loc, l.reference"
        ),
        &params,
    )?;
    let edges = rows_from(&edges, true);
    let via = rows_from(&via, false);
    let mut out = String::new();
    for node in nodes {
        if !out.is_empty() {
            out.push('\n');
        }
        let is_file = node.entity_type == "File";
        if is_file {
            let path = node.properties["path"].as_str().unwrap();
            let members: Vec<_> = file_members
                .iter()
                .filter(|member| member.file == path)
                .cloned()
                .collect();
            writeln!(
                out,
                "{}  [{}]  ({} definitions)",
                path,
                node.properties["language"].as_str().unwrap_or(""),
                members.len()
            )?;
            if let Some(reason) = node.properties["skip_reason"]
                .as_str()
                .filter(|reason| !reason.is_empty())
            {
                writeln!(out, "Indexing reason: {reason}")?;
            }
            if members.is_empty() {
                writeln!(out, "No indexed definitions.")?;
            }
            for member in &members {
                writeln!(
                    out,
                    "  {}  [{}]  L{}-{}",
                    member.fqn, member.kind, member.start, member.end
                )?;
            }
            if let Some(member) = members.first() {
                writeln!(
                    out,
                    "\nFor a definition's source and connections, read its lines.\nnext: {} context {path}:{}-{}",
                    spec::launcher(),
                    member.start,
                    member.end
                )?;
            }
        } else {
            let range = context::source_range(node)?;
            writeln!(
                out,
                "{}  [{}]  {}:{}-{}",
                range.fqn, range.kind, range.file, range.start, range.end
            )?;
        }
        let links: Vec<_> = edges
            .iter()
            .filter(|row| row.target_id == node.id && !row.hidden)
            .collect();
        let used_via: Vec<_> = via
            .iter()
            .filter(|row| row.target_id == node.id && !row.hidden)
            .collect();
        let tests: Vec<_> = edges
            .iter()
            .chain(&via)
            .filter(|row| row.target_id == node.id && row.hidden)
            .collect();
        if links.is_empty() && used_via.is_empty() && tests.is_empty() {
            writeln!(out, "\nNo indexed connections.")?;
            continue;
        }
        for (title, rows) in [
            ("Connections", links),
            (
                if is_file {
                    "Connections via definitions"
                } else {
                    "Used via members"
                },
                used_via,
            ),
            ("Test, fixture, or generated connections", tests),
        ] {
            if rows.is_empty() {
                continue;
            }
            writeln!(out, "\n{title} ({} indexed):", rows.len())?;
            let mut prev_path = String::new();
            let limit = if is_file {
                FILE_CONNECTION_LIMIT
            } else {
                rows.len()
            };
            let (rows, fields) = fold_fields(&rows);
            let limit = limit.min(rows.len());
            for line in &fields {
                writeln!(out, "{line}")?;
            }
            for row in rows.iter().take(limit) {
                let via = if row.via.is_empty() {
                    String::new()
                } else {
                    let names: Vec<_> = row.via.split(", ").collect();
                    if is_file && names.len() > 3 {
                        format!(
                            "  via {} (+{} names)",
                            names[..3].join(", "),
                            names.len() - 3
                        )
                    } else {
                        format!("  via {}", row.via)
                    }
                };
                let external = match row.reference.as_str() {
                    "external/unresolved" => "  external",
                    _ => "",
                };
                writeln!(
                    out,
                    "  {} {}  [{}]{external}{via}{}",
                    row.dir,
                    row.label,
                    row.kind.to_lowercase(),
                    loc_suffix(&row.loc, &mut prev_path)
                )?;
            }
            let omitted = rows.len().saturating_sub(limit);
            if omitted > 0 {
                let omitted_rows = &rows[limit..];
                if omitted_rows.iter().any(|row| row.direct) {
                    writeln!(
                        out,
                        "  … {omitted} connections omitted; list all file edges: {} workspace sql \"SELECT * FROM gl_edge WHERE source_id = {} OR target_id = {}\"",
                        spec::launcher(),
                        node.id,
                        node.id
                    )?;
                    if omitted_rows.iter().any(|row| !row.direct) {
                        writeln!(
                            out,
                            "  For omitted definition connections, read a definition's lines from the map."
                        )?;
                    }
                } else {
                    writeln!(
                        out,
                        "  … {omitted} connections omitted; read a definition's lines from the map for its complete connections."
                    )?;
                }
            }
        }
    }
    Ok(out)
}

const FIELD_KINDS: &[&str] = &["Field", "Property", "Attribute"];
const FOLDED_FIELD_NAMES: usize = 4;

fn fold_fields<'a>(rows: &[&'a Row]) -> (Vec<&'a Row>, Vec<String>) {
    let mut owners: Vec<(String, Vec<&'a Row>)> = Vec::new();
    let mut kept = Vec::new();
    for row in rows {
        let owner = row
            .label
            .rsplit_once("::")
            .or_else(|| row.label.rsplit_once('.'))
            .map(|(owner, _)| owner.to_string());
        match (
            row.dir.as_str(),
            FIELD_KINDS.contains(&row.def_kind.as_str()),
            owner,
        ) {
            ("-->", true, Some(owner)) => {
                match owners.iter_mut().find(|(name, _)| *name == owner) {
                    Some((_, fields)) => fields.push(row),
                    None => owners.push((owner, vec![row])),
                }
            }
            _ => kept.push(*row),
        }
    }
    let mut lines = Vec::new();
    for (owner, fields) in owners {
        if fields.len() == 1 {
            kept.push(fields[0]);
            continue;
        }
        let names: Vec<&str> = fields
            .iter()
            .map(|row| row.label.rsplit(['.', ':']).next().unwrap_or(&row.label))
            .collect();
        let more = match names.len().saturating_sub(FOLDED_FIELD_NAMES) {
            0 => String::new(),
            more => format!(", +{more}"),
        };
        let loc = fields[0].loc.as_str();
        lines.push(format!(
            "  --> {owner}: {} fields ({}{more})  [{}]{}",
            names.len(),
            names[..names.len().min(FOLDED_FIELD_NAMES)].join(", "),
            fields[0].kind.to_lowercase(),
            match loc.is_empty() {
                true => String::new(),
                false => format!("  ({loc})"),
            }
        ));
    }
    (kept, lines)
}

fn loc_suffix(loc: &str, prev_path: &mut String) -> String {
    if loc.is_empty() {
        return String::new();
    }
    let (path, line) = loc.rsplit_once(':').unwrap_or((loc, ""));
    let suffix = if path == prev_path {
        format!("  (:{line})")
    } else {
        format!("  ({loc})")
    };
    *prev_path = path.to_string();
    suffix
}
