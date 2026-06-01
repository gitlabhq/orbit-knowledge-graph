# graph-viz — 3D code-graph visualizer (TRON / JARVIS)

A browser-based 3D visualizer for the **local Orbit code graph** (the DuckDB
that `orbit index` produces). Black void, neon edges with `UnrealBloomPass`
glow, glassy node labels, ontology-driven colors. Built on
[`3d-force-graph`](https://github.com/vasturiano/3d-force-graph) (Three.js +
d3-force-3d).

This covers Milestones 1–3 of the design (data adapter, renderer,
interactions). The remote Orbit API source (Milestone 4) is a separate
follow-up.

## What it does

- **M1 — Data adapter** (`scripts/adapter.mjs`): runs `orbit sql --format json`
  against the four local node tables (`gl_directory`, `gl_file`,
  `gl_definition`, `gl_imported_symbol`) plus `gl_edge`, and emits
  `graph.json` (`{ nodes, links, meta }`). Generates `styles.json` by parsing
  the `style:` blocks from `config/ontology/nodes/**/*.yaml`.
- **M2 — Renderer** (`src/main.js`): force-directed 3D layout, bloom glow,
  ontology-driven color/size, glassy sprite labels, idle camera drift.
- **M3 — Interactions**: click a node to expand its neighbors, search to
  fly to a symbol, "who calls this" ripple over `CALLS` edges, and an
  edge-type legend with filter toggles.

## Design choices

- **Layout:** 3D force-directed (`d3-force-3d`). Hierarchical shells were a
  stretch goal and are not implemented here.
- **Scale cap:** **degree-based top-N** (default `--cap 1500`). Nodes are
  ranked by total degree (in + out across all edge kinds); the top N are kept
  and edges are pruned to the surviving set. This keeps the densely-connected
  core — the part worth looking at — and drops leaf noise first. The renderer
  additionally seeds the opening view with the ~400 highest-degree nodes and
  expands the rest on click, so the first frame is legible rather than a wall
  of points. IDs are i64 and exceed JavaScript's safe-integer range, so they
  are carried as strings end to end.

## Demo in under 5 minutes

From the repo root. The DuckDB client needs `libduckdb.so`, which the debug
build drops in `target/debug/deps`, so export `LD_LIBRARY_PATH` for the
`orbit` invocations.

```bash
# 0. From the repo root, with mise tools installed (`mise install`).
export LD_LIBRARY_PATH="$PWD/target/debug/deps:$LD_LIBRARY_PATH"

# 1. Build orbit.
cargo build --bin orbit -p orbit-local

# 2. Index a repo into an isolated workspace (this repo itself works well).
export ORBIT_DATA_DIR="$PWD/tools/graph-viz/tmp/orbit-data"
mkdir -p "$ORBIT_DATA_DIR"
./target/debug/orbit index "$PWD"

# 3. Install viz deps and build the browser bundle.
cd tools/graph-viz
npm install
npm run build

# 4. Run the adapter against the indexed DuckDB.
node scripts/adapter.mjs \
  --orbit "../../target/debug/orbit" \
  --db "$ORBIT_DATA_DIR/graph.duckdb" \
  --cap 1500

# 5. Serve and open.
npm run serve   # http://localhost:8080/
```

Open <http://localhost:8080/> in a browser. Click nodes to expand, type in the
search box to fly to a symbol, select a definition and hit **Who calls this**
to fire the ripple, and toggle edge types in the bottom-left legend.

## Adapter reference

```text
node scripts/adapter.mjs [options]
  --orbit <path>      path to the orbit binary (default: target/debug/orbit)
  --db <path>         path to graph.duckdb (or set ORBIT_DB) [required]
  --ontology <path>   ontology dir (default: config/ontology)
  --out <path>        output dir (default: public/data)
  --cap <n>           degree top-N node cap, 0 = no cap (default: 1500)
```

Output: `public/data/graph.json` and `public/data/styles.json`.

## Tests

```bash
npm run test:unit   # adapter logic: JSON shape, cap, style parsing (node --test)
npm run test:e2e    # Playwright smoke test against the committed fixture
npm test            # both
```

The Playwright smoke test loads a committed fixture (`test/fixtures/graph.json`,
exercising all five edge kinds) so it runs without orbit or DuckDB. It asserts
the scene initializes, the WebGL canvas renders a non-black frame, search and
the inspector work, and the ripple fires — with no console errors. Screenshots
land in `tmp/screenshots/`. Point it at a real adapter-generated graph with
`VIZ_DATA=data npx playwright test`.

The browser needs software WebGL in headless CI; the Playwright config launches
chromium with `--use-gl=angle --use-angle=swiftshader`.

## Layout

```text
tools/graph-viz/
  scripts/adapter.mjs   M1 adapter (orbit sql -> graph.json + styles.json)
  scripts/build.mjs     esbuild bundle of the renderer
  scripts/serve.mjs     dependency-free static server
  src/graph.mjs         pure graph-building + style parsing (unit-tested)
  src/main.js           renderer + interactions (bundled to public/bundle.js)
  public/index.html     the page (HUD, legend, inspector)
  public/styles.css     TRON HUD styling
  test/adapter.test.mjs node:test unit tests
  test/e2e/             Playwright smoke test
  test/fixtures/        committed mini graph for CI
```

Generated artifacts (`public/bundle.js`, `public/data/`, `tmp/`,
`node_modules/`) are gitignored.
