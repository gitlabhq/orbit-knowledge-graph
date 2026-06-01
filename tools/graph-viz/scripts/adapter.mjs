#!/usr/bin/env node
// Orbit code-graph -> graph.json + styles.json adapter (Milestone 1).
//
// Runs `orbit sql --format json` once per node table plus the edge table,
// builds a force-directed graph with a degree-based top-N cap, and parses the
// ontology `style:` blocks into a color/size palette. Output is written to the
// viz's data directory so the static renderer can fetch it.
//
// Usage:
//   node scripts/adapter.mjs \
//     --orbit /path/to/orbit \
//     --db /path/to/graph.duckdb \
//     --ontology /path/to/config/ontology \
//     --out public/data \
//     --cap 1500

import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';

import { NODE_TABLES, EDGE_TABLE, buildGraph, buildStyles } from '../src/graph.mjs';
import { layoutGraph } from './layout.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '../../..');

function parseArgs(argv) {
  const args = {
    orbit: process.env.ORBIT_BIN || join(repoRoot, 'target/debug/orbit'),
    db: process.env.ORBIT_DB || '',
    ontology: join(repoRoot, 'config/ontology'),
    out: join(__dirname, '../public/data'),
    cap: 1500,
    layout: true,
    'layout-ticks': 300,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i].replace(/^--/, '');
    const val = argv[i + 1];
    if (key === 'cap' || key === 'layout-ticks') {
      args[key] = Number(val);
      i += 1;
    } else if (key === 'no-layout') {
      args.layout = false;
    } else if (key === 'layout') {
      args.layout = val !== 'false';
      i += 1;
    } else if (key in args) {
      args[key] = val;
      i += 1;
    }
  }
  return args;
}

function orbitSqlJson(orbit, db, sql) {
  const out = execFileSync(orbit, ['sql', sql, '--format', 'json', '--db', db], {
    encoding: 'utf8',
    maxBuffer: 512 * 1024 * 1024,
    env: process.env,
  });
  const trimmed = out.trim();
  return trimmed ? JSON.parse(trimmed) : [];
}

function loadNodeYaml(ontologyDir) {
  const nodesDir = join(ontologyDir, 'nodes');
  const wanted = new Map(NODE_TABLES.map((n) => [n.kind, n]));
  const byKind = new Map();
  for (const file of walkYaml(nodesDir)) {
    const text = readFileSync(file, 'utf8');
    const yaml = parseYaml(text);
    const kind = yaml?.node_type;
    if (kind && wanted.has(kind)) byKind.set(kind, { kind, yaml });
  }
  return NODE_TABLES.filter((n) => byKind.has(n.kind)).map((n) => byKind.get(n.kind));
}

function walkYaml(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...walkYaml(full));
    else if (entry.endsWith('.yaml') || entry.endsWith('.yml')) out.push(full);
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv);
  if (!args.db) {
    console.error(
      'error: --db <path/to/graph.duckdb> is required (or set ORBIT_DB).\n' +
        'Index a repo first: orbit index <repo>',
    );
    process.exit(1);
  }

  console.error(`[adapter] orbit=${args.orbit}`);
  console.error(`[adapter] db=${args.db}`);
  console.error(`[adapter] cap=${args.cap || 'none'}`);

  // IDs are i64 and exceed JS safe-integer range, so cast to VARCHAR and keep
  // them as strings end-to-end.
  const nodeRows = NODE_TABLES.map(({ table, kind, labelColumn }) => {
    const rows = orbitSqlJson(
      args.orbit,
      args.db,
      `SELECT * REPLACE (CAST(id AS VARCHAR) AS id) FROM ${table}`,
    );
    console.error(`[adapter] ${table}: ${rows.length} rows`);
    return { kind, labelColumn, rows };
  });

  const edgeRows = orbitSqlJson(
    args.orbit,
    args.db,
    `SELECT CAST(source_id AS VARCHAR) AS source_id, ` +
      `CAST(target_id AS VARCHAR) AS target_id, relationship_kind FROM ${EDGE_TABLE}`,
  );
  console.error(`[adapter] ${EDGE_TABLE}: ${edgeRows.length} rows`);

  const graph = buildGraph(nodeRows, edgeRows, args.cap);
  const styles = buildStyles(loadNodeYaml(args.ontology));

  if (args.layout) {
    const t0 = Date.now();
    layoutGraph(graph.nodes, graph.links, { ticks: args['layout-ticks'] });
    graph.meta.layout = { precomputed: true, ticks: args['layout-ticks'] };
    console.error(
      `[adapter] precomputed layout: ${graph.nodes.length} nodes, ` +
        `${args['layout-ticks']} ticks in ${Date.now() - t0}ms`,
    );
  } else {
    graph.meta.layout = { precomputed: false };
  }

  mkdirSync(args.out, { recursive: true });
  writeFileSync(join(args.out, 'graph.json'), JSON.stringify(graph));
  writeFileSync(join(args.out, 'styles.json'), JSON.stringify(styles, null, 2));

  console.error(`[adapter] styles: ${Object.keys(styles).join(', ')}`);
  console.error(
    `[adapter] wrote ${graph.meta.keptNodes}/${graph.meta.totalNodes} nodes, ` +
      `${graph.meta.keptLinks}/${graph.meta.totalLinks} links to ${args.out}`,
  );
  console.error(`[adapter] edge kinds: ${JSON.stringify(graph.meta.edgeKindCounts)}`);
}

main();
