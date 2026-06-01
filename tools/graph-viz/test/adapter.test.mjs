import { test } from 'node:test';
import assert from 'node:assert/strict';

import { buildGraph, buildStyles, NODE_TABLES, EDGE_KINDS } from '../src/graph.mjs';

function sampleNodes() {
  return [
    { kind: 'Directory', labelColumn: 'path', rows: [{ id: 1, path: 'src' }] },
    {
      kind: 'File',
      labelColumn: 'path',
      rows: [
        { id: 2, path: 'src/a.rs', name: 'a.rs', language: 'rust' },
        { id: 3, path: 'src/b.rs', name: 'b.rs', language: 'rust' },
      ],
    },
    {
      kind: 'Definition',
      labelColumn: 'name',
      rows: [
        { id: 4, name: 'foo', fqn: 'a::foo', definition_type: 'Function', file_path: 'src/a.rs' },
        { id: 5, name: 'bar', fqn: 'b::bar', definition_type: 'Function', file_path: 'src/b.rs' },
      ],
    },
    {
      kind: 'ImportedSymbol',
      labelColumn: 'identifier_name',
      rows: [{ id: 6, identifier_name: null, import_path: 'std::fs' }],
    },
  ];
}

function sampleEdges() {
  return [
    { source_id: 1, target_id: 2, relationship_kind: 'CONTAINS' },
    { source_id: 1, target_id: 3, relationship_kind: 'CONTAINS' },
    { source_id: 2, target_id: 4, relationship_kind: 'DEFINES' },
    { source_id: 3, target_id: 5, relationship_kind: 'DEFINES' },
    { source_id: 4, target_id: 5, relationship_kind: 'CALLS' },
    { source_id: 3, target_id: 6, relationship_kind: 'IMPORTS' },
  ];
}

test('buildGraph emits {nodes, links} of the expected shape', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 0);
  assert.equal(g.nodes.length, 6);
  assert.equal(g.links.length, 6);

  const node = g.nodes.find((n) => n.id === '4');
  assert.deepEqual(
    { id: node.id, kind: node.kind, label: node.label, fqn: node.fqn },
    { id: '4', kind: 'Definition', label: 'foo', fqn: 'a::foo' },
  );

  for (const link of g.links) {
    assert.equal(typeof link.source, 'string');
    assert.equal(typeof link.target, 'string');
    assert.ok(EDGE_KINDS.includes(link.kind));
  }
});

test('ids are stringified to survive i64 precision', () => {
  const big = '3864473129654815802';
  const g = buildGraph(
    [{ kind: 'Directory', labelColumn: 'path', rows: [{ id: big, path: '.' }] }],
    [],
    0,
  );
  assert.equal(g.nodes[0].id, big);
  assert.equal(typeof g.nodes[0].id, 'string');
});

test('label falls back through name/path/import_path when label column is empty', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 0);
  const imported = g.nodes.find((n) => n.id === '6');
  assert.equal(imported.label, 'std::fs'); // identifier_name was null
});

test('degree is computed across all edge kinds', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 0);
  const file3 = g.nodes.find((n) => n.id === '3');
  // file 3: CONTAINS(in), DEFINES(out), IMPORTS(out) = degree 3
  assert.equal(file3.degree, 3);
});

test('degree top-N cap keeps the highest-degree nodes and prunes dangling links', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 3);
  assert.equal(g.nodes.length, 3);
  assert.ok(g.meta.capped);
  assert.equal(g.meta.totalNodes, 6);
  assert.equal(g.meta.cap, 3);

  const keptIds = new Set(g.nodes.map((n) => n.id));
  for (const link of g.links) {
    assert.ok(keptIds.has(link.source));
    assert.ok(keptIds.has(link.target));
  }
});

test('cap of 0 keeps every node', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 0);
  assert.equal(g.nodes.length, 6);
  assert.equal(g.meta.capped, false);
});

test('self-loops are dropped', () => {
  const g = buildGraph(
    [{ kind: 'Directory', labelColumn: 'path', rows: [{ id: 1, path: '.' }] }],
    [{ source_id: 1, target_id: 1, relationship_kind: 'CONTAINS' }],
    0,
  );
  assert.equal(g.links.length, 0);
});

test('dangling edges (endpoint missing from node set) are dropped', () => {
  const g = buildGraph(
    [{ kind: 'File', labelColumn: 'path', rows: [{ id: 1, path: 'a' }] }],
    [{ source_id: 1, target_id: 999, relationship_kind: 'DEFINES' }],
    0,
  );
  assert.equal(g.links.length, 0);
});

test('meta carries node/edge kind counts', () => {
  const g = buildGraph(sampleNodes(), sampleEdges(), 0);
  assert.equal(g.meta.edgeKindCounts.CONTAINS, 2);
  assert.equal(g.meta.edgeKindCounts.CALLS, 1);
  assert.equal(g.meta.nodeKindCounts.Definition, 2);
});

test('buildStyles parses color/size from ontology style blocks', () => {
  const styles = buildStyles([
    { kind: 'Directory', yaml: { node_type: 'Directory', style: { size: 26, color: '#06B6D4' } } },
    { kind: 'File', yaml: { node_type: 'File', style: { size: 24, color: '#22D3EE' } } },
  ]);
  assert.deepEqual(styles.Directory, { color: '#06B6D4', size: 26 });
  assert.deepEqual(styles.File, { color: '#22D3EE', size: 24 });
});

test('buildStyles falls back when style block is missing', () => {
  const styles = buildStyles([{ kind: 'File', yaml: { node_type: 'File' } }]);
  assert.equal(typeof styles.File.color, 'string');
  assert.equal(typeof styles.File.size, 'number');
});

test('NODE_TABLES covers exactly the four local code-graph node tables', () => {
  assert.deepEqual(
    NODE_TABLES.map((n) => n.table).sort(),
    ['gl_definition', 'gl_directory', 'gl_file', 'gl_imported_symbol'],
  );
});
