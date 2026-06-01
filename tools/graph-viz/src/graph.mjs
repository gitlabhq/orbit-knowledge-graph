// Pure graph-building logic for the Orbit code-graph adapter.
//
// Kept free of I/O (no `orbit`, no filesystem) so it can be unit-tested in
// isolation. The adapter script wires these functions to `orbit sql` output
// and the ontology on disk.

/**
 * Node table -> ontology node_type. The local DuckDB code graph writes exactly
 * these four node tables (see crates/duckdb-client/src/converter.rs).
 */
export const NODE_TABLES = [
  { table: 'gl_directory', kind: 'Directory', labelColumn: 'path' },
  { table: 'gl_file', kind: 'File', labelColumn: 'path' },
  { table: 'gl_definition', kind: 'Definition', labelColumn: 'name' },
  { table: 'gl_imported_symbol', kind: 'ImportedSymbol', labelColumn: 'identifier_name' },
];

export const EDGE_TABLE = 'gl_edge';

/** Edge kinds present in the local code graph, in legend order. */
export const EDGE_KINDS = ['CONTAINS', 'DEFINES', 'CALLS', 'IMPORTS', 'EXTENDS'];

/**
 * Build the `{ nodes, links }` graph from raw rows, applying a degree-based
 * top-N cap.
 *
 * - `nodeRows`: array of `{ kind, labelColumn, rows }` where `rows` are the raw
 *   objects from `orbit sql` (ids already stringified).
 * - `edgeRows`: raw rows from `gl_edge` (source_id/target_id stringified).
 * - `cap`: maximum number of nodes to keep (`0`/falsy = no cap).
 *
 * Strategy (documented in README): force-directed layout + degree top-N. We
 * rank nodes by total degree (in + out across all edge kinds), keep the top
 * `cap`, then keep only edges whose endpoints both survive. This keeps the
 * densely-connected core of the graph — the part worth looking at — and drops
 * leaf noise first.
 */
export function buildGraph(nodeRows, edgeRows, cap = 0) {
  const nodes = new Map();

  for (const { kind, labelColumn, rows } of nodeRows) {
    for (const row of rows) {
      const id = String(row.id);
      const label =
        firstNonEmpty(row[labelColumn], row.name, row.path, row.identifier_name, row.import_path) ||
        id;
      nodes.set(id, {
        id,
        kind,
        label,
        // Carry a few descriptive fields through for the inspector panel.
        fqn: row.fqn ?? null,
        definitionType: row.definition_type ?? null,
        filePath: row.file_path ?? row.path ?? null,
        language: row.language ?? null,
        degree: 0,
      });
    }
  }

  // Normalise edges to string ids and drop dangling references.
  const edges = [];
  const degree = new Map();
  for (const e of edgeRows) {
    const source = String(e.source_id);
    const target = String(e.target_id);
    const kind = e.relationship_kind;
    if (source === target) continue; // drop self-loops (e.g. root dir -> itself)
    if (!nodes.has(source) || !nodes.has(target)) continue;
    edges.push({ source, target, kind });
    degree.set(source, (degree.get(source) ?? 0) + 1);
    degree.set(target, (degree.get(target) ?? 0) + 1);
  }
  for (const [id, d] of degree) nodes.get(id).degree = d;

  let keptIds = new Set(nodes.keys());
  let capped = false;
  if (cap && nodes.size > cap) {
    capped = true;
    const ranked = [...nodes.values()].sort(
      (a, b) => b.degree - a.degree || a.id.localeCompare(b.id),
    );
    keptIds = new Set(ranked.slice(0, cap).map((n) => n.id));
  }

  const keptNodes = [...nodes.values()].filter((n) => keptIds.has(n.id));
  const keptLinks = edges.filter((e) => keptIds.has(e.source) && keptIds.has(e.target));

  return {
    nodes: keptNodes,
    links: keptLinks,
    meta: {
      totalNodes: nodes.size,
      totalLinks: edges.length,
      keptNodes: keptNodes.length,
      keptLinks: keptLinks.length,
      capped,
      cap: cap || null,
      edgeKindCounts: countBy(keptLinks, (l) => l.kind),
      nodeKindCounts: countBy(keptNodes, (n) => n.kind),
    },
  };
}

/**
 * Parse `style:` blocks out of the ontology node YAML files into a
 * `{ <NodeType>: { color, size } }` map. `nodeYamlFiles` is an array of
 * `{ kind, yaml }` where `yaml` is a parsed YAML object (see adapter for the
 * loader). Only node types present in the local code graph are emitted.
 */
export function buildStyles(nodeYamlObjects) {
  const styles = {};
  for (const { kind, yaml } of nodeYamlObjects) {
    const style = yaml?.style ?? {};
    styles[kind] = {
      color: style.color ?? '#22D3EE',
      size: typeof style.size === 'number' ? style.size : 24,
    };
  }
  return styles;
}

function firstNonEmpty(...values) {
  for (const v of values) {
    if (v !== null && v !== undefined && String(v).trim() !== '') return String(v);
  }
  return null;
}

function countBy(items, keyFn) {
  const out = {};
  for (const item of items) {
    const k = keyFn(item);
    out[k] = (out[k] ?? 0) + 1;
  }
  return out;
}
