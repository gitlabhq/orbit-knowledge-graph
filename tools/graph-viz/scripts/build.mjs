#!/usr/bin/env node
// Bundle the renderer (src/main.js + 3d-force-graph + three + UnrealBloomPass)
// into a single browser module at public/bundle.js so a single three copy is
// shared across all imports.

import { build } from 'esbuild';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');

await build({
  entryPoints: [join(root, 'src/main.js')],
  bundle: true,
  format: 'esm',
  target: 'es2020',
  minify: true,
  sourcemap: true,
  outfile: join(root, 'public/bundle.js'),
  logLevel: 'info',
});
