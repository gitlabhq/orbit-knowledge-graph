#!/usr/bin/env node
// Minimal static file server for the graph-viz demo. No dependencies — just
// serves `public/` (and the bundled `node_modules` deps the page imports).
//
// Usage: node scripts/serve.mjs [--port 8080]

import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '..');
const publicDir = join(root, 'public');

const port = Number(argValue('--port') || process.env.PORT || 8080);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.map': 'application/json; charset=utf-8',
};

const server = createServer(async (req, res) => {
  try {
    const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
    const filePath = await resolveFile(urlPath);
    if (!filePath) {
      res.writeHead(404).end('Not found');
      return;
    }
    const body = await readFile(filePath);
    res.writeHead(200, {
      'Content-Type': MIME[extname(filePath)] || 'application/octet-stream',
      'Cache-Control': 'no-store',
    });
    res.end(body);
  } catch (err) {
    res.writeHead(500).end(String(err));
  }
});

async function resolveFile(urlPath) {
  const rel = urlPath === '/' ? '/index.html' : urlPath;
  // node_modules: bundled ESM deps. test/: committed fixtures for the smoke test.
  const fromRoot = rel.startsWith('/node_modules/') || rel.startsWith('/test/');
  const base = fromRoot ? root : publicDir;
  const candidate = normalize(join(base, rel));
  if (!candidate.startsWith(root)) return null; // path traversal guard
  try {
    const info = await stat(candidate);
    if (info.isFile()) return candidate;
  } catch {
    /* fall through */
  }
  return null;
}

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 ? process.argv[idx + 1] : undefined;
}

server.listen(port, () => {
  console.log(`graph-viz serving http://localhost:${port}/`);
});
