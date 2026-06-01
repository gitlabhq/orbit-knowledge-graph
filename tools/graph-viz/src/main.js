// TRON / JARVIS 3D code-graph renderer (Milestones 2 + 3).
//
// Imports are bare and resolved by esbuild at build time (see scripts/build.mjs)
// into a single browser bundle, so three is shared between 3d-force-graph and
// the UnrealBloomPass.

import ForceGraph3D from '3d-force-graph';
import * as THREE from 'three';
import { UnrealBloomPass } from 'three/examples/jsm/postprocessing/UnrealBloomPass.js';

// Default data dir is `data/` (written by the adapter). A `?data=<path>`
// override lets the Playwright smoke test point at a committed fixture so CI
// can run without orbit/DuckDB.
const DATA_BASE = new URLSearchParams(window.location.search).get('data') || 'data';
const EDGE_KINDS = ['CONTAINS', 'DEFINES', 'CALLS', 'IMPORTS', 'EXTENDS'];

// Per-edge-kind colors, tuned for the teal/cyan TRON palette. CALLS pops amber
// so the "who calls this" ripple reads clearly against the cyan structure.
const EDGE_COLORS = {
  CONTAINS: '#0e7490',
  DEFINES: '#22d3ee',
  CALLS: '#fbbf24',
  IMPORTS: '#5eead4',
  EXTENDS: '#a78bfa',
};

const state = {
  graph: null,
  styles: {},
  fullData: { nodes: [], links: [] },
  nodeById: new Map(),
  adjacency: new Map(),
  callers: new Map(), // target -> [source] over CALLS edges
  visibleNodeIds: new Set(),
  edgeFilter: new Set(EDGE_KINDS),
  selectedId: null,
};

async function loadJson(name) {
  const res = await fetch(`${DATA_BASE}/${name}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`failed to load ${name}: ${res.status}`);
  return res.json();
}

function indexData(data) {
  state.nodeById = new Map(data.nodes.map((n) => [n.id, n]));
  state.adjacency = new Map(data.nodes.map((n) => [n.id, new Set()]));
  state.callers = new Map();
  for (const link of data.links) {
    const s = typeof link.source === 'object' ? link.source.id : link.source;
    const t = typeof link.target === 'object' ? link.target.id : link.target;
    state.adjacency.get(s)?.add(t);
    state.adjacency.get(t)?.add(s);
    if (link.kind === 'CALLS') {
      if (!state.callers.has(t)) state.callers.set(t, []);
      state.callers.get(t).push(s);
    }
  }
}

function styleFor(kind) {
  return state.styles[kind] || { color: '#22d3ee', size: 24 };
}

function nodeColor(node) {
  return styleFor(node.kind).color;
}

// Glassy text-sprite label rendered on a translucent rounded panel.
function makeLabelSprite(node) {
  const text = node.label.length > 42 ? `${node.label.slice(0, 41)}…` : node.label;
  const fontSize = 64;
  const padding = 24;
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.font = `600 ${fontSize}px "Inter", "Segoe UI", system-ui, sans-serif`;
  const textWidth = ctx.measureText(text).width;
  canvas.width = textWidth + padding * 2;
  canvas.height = fontSize + padding * 2;

  const c = canvas.getContext('2d');
  const color = nodeColor(node);
  roundRect(c, 0, 0, canvas.width, canvas.height, 28);
  c.fillStyle = 'rgba(8, 15, 22, 0.55)';
  c.fill();
  c.lineWidth = 3;
  c.strokeStyle = hexToRgba(color, 0.85);
  c.stroke();

  c.font = `600 ${fontSize}px "Inter", "Segoe UI", system-ui, sans-serif`;
  c.textBaseline = 'middle';
  c.textAlign = 'center';
  c.shadowColor = color;
  c.shadowBlur = 18;
  c.fillStyle = '#e8feff';
  c.fillText(text, canvas.width / 2, canvas.height / 2);

  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  const material = new THREE.SpriteMaterial({ map: texture, transparent: true, depthWrite: false });
  const sprite = new THREE.Sprite(material);
  const scale = 0.12;
  sprite.scale.set(canvas.width * scale, canvas.height * scale, 1);
  return sprite;
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function hexToRgba(hex, alpha) {
  const v = hex.replace('#', '');
  const r = parseInt(v.slice(0, 2), 16);
  const g = parseInt(v.slice(2, 4), 16);
  const b = parseInt(v.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// Glowing node: an emissive sphere whose size comes from the ontology style and
// scales gently with degree so hubs read as bigger.
function makeNodeObject(node) {
  const { color, size } = styleFor(node.kind);
  const radius = (size / 6) * (1 + Math.min(node.degree || 0, 40) / 80);
  const geometry = new THREE.SphereGeometry(radius, 16, 16);
  const material = new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.95 });
  const sphere = new THREE.Mesh(geometry, material);

  const glowGeom = new THREE.SphereGeometry(radius * 1.6, 16, 16);
  const glowMat = new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.18 });
  sphere.add(new THREE.Mesh(glowGeom, glowMat));
  return sphere;
}

function visibleLink(link) {
  return state.edgeFilter.has(link.kind);
}

function applyData(Graph) {
  const nodes = state.fullData.nodes.filter((n) => state.visibleNodeIds.has(n.id));
  const visIds = new Set(nodes.map((n) => n.id));
  const links = state.fullData.links.filter((l) => {
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    return visIds.has(s) && visIds.has(t) && state.edgeFilter.has(l.kind);
  });
  Graph.graphData({ nodes, links });
}

function buildGraph(container) {
  const Graph = new ForceGraph3D(container, { controlType: 'orbit' })
    .backgroundColor('#03060a')
    .showNavInfo(false)
    .nodeThreeObject(makeNodeObject)
    .nodeLabel(() => '') // custom sprites used instead of the default tooltip
    .linkColor((l) => EDGE_COLORS[l.kind] || '#22d3ee')
    .linkOpacity(0.4)
    .linkWidth((l) => (l.kind === 'CALLS' ? 0.6 : 0.3))
    .linkDirectionalParticles(0)
    .linkDirectionalParticleWidth(2)
    .onNodeClick((node) => expandNode(Graph, node))
    .onBackgroundClick(() => selectNode(Graph, null));

  Graph.d3Force('charge').strength(-120);
  Graph.d3Force('link').distance(40);

  // UnrealBloomPass for the neon glow.
  const bloom = new UnrealBloomPass(new THREE.Vector2(window.innerWidth, window.innerHeight), 1.6, 0.6, 0.05);
  Graph.postProcessingComposer().addPass(bloom);

  return Graph;
}

// Camera drift: slowly orbit the scene while idle for the "living HUD" feel.
function startCameraDrift(Graph) {
  let angle = 0;
  let userInteracted = false;
  const controls = Graph.controls();
  controls.addEventListener('start', () => {
    userInteracted = true;
  });
  const distance = 600;
  setInterval(() => {
    if (userInteracted) return;
    angle += Math.PI / 1800;
    Graph.cameraPosition({
      x: distance * Math.sin(angle),
      z: distance * Math.cos(angle),
    });
  }, 40);
}

function expandNode(Graph, node) {
  selectNode(Graph, node);
  const neighbors = state.adjacency.get(node.id);
  if (neighbors) {
    let added = 0;
    for (const id of neighbors) {
      if (!state.visibleNodeIds.has(id)) {
        state.visibleNodeIds.add(id);
        added += 1;
      }
    }
    if (added > 0) applyData(Graph);
  }
  flyTo(Graph, node);
}

function selectNode(Graph, node) {
  state.selectedId = node ? node.id : null;
  updateInspector(node);
}

function flyTo(Graph, node) {
  const distance = 120;
  const distRatio = 1 + distance / Math.hypot(node.x || 1, node.y || 1, node.z || 1);
  Graph.cameraPosition(
    { x: (node.x || 0) * distRatio, y: (node.y || 0) * distRatio, z: (node.z || 0) * distRatio },
    node,
    1200,
  );
}

// "Who calls this" ripple: animate directional particles backwards over CALLS
// edges that terminate at the selected node.
function rippleCallers(Graph, targetId) {
  const calls = state.fullData.links.filter((l) => {
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    return l.kind === 'CALLS' && t === targetId;
  });
  if (calls.length === 0) return 0;
  // Ensure callers are visible so the ripple has somewhere to travel.
  for (const l of calls) {
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    state.visibleNodeIds.add(s);
  }
  if (!state.edgeFilter.has('CALLS')) {
    state.edgeFilter.add('CALLS');
    syncLegend();
  }
  applyData(Graph);
  Graph.linkDirectionalParticles((l) => {
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    return l.kind === 'CALLS' && t === targetId ? 4 : 0;
  }).linkDirectionalParticleSpeed(0.012);
  setTimeout(() => Graph.linkDirectionalParticles(0), 6000);
  return calls.length;
}

// ── UI wiring ───────────────────────────────────────────────────────────────

function updateInspector(node) {
  const el = document.getElementById('inspector');
  if (!node) {
    el.classList.add('hidden');
    return;
  }
  el.classList.remove('hidden');
  const rows = [
    ['kind', node.kind],
    ['label', node.label],
    ['fqn', node.fqn],
    ['type', node.definitionType],
    ['file', node.filePath],
    ['language', node.language],
    ['degree', node.degree],
  ].filter(([, v]) => v !== null && v !== undefined && v !== '');
  el.querySelector('.inspector-body').innerHTML = rows
    .map(([k, v]) => `<div class="row"><span class="k">${k}</span><span class="v">${escapeHtml(String(v))}</span></div>`)
    .join('');
  const callerCount = state.callers.get(node.id)?.length || 0;
  el.querySelector('.who-calls').textContent = callerCount
    ? `Who calls this (${callerCount}) →`
    : 'No callers in graph';
  el.querySelector('.who-calls').dataset.disabled = callerCount ? 'false' : 'true';
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function buildLegend() {
  const legend = document.getElementById('legend-items');
  legend.innerHTML = EDGE_KINDS.map(
    (kind) => `
    <label class="legend-item" data-kind="${kind}">
      <input type="checkbox" checked data-kind="${kind}" />
      <span class="swatch" style="background:${EDGE_COLORS[kind]}; box-shadow:0 0 8px ${EDGE_COLORS[kind]}"></span>
      <span class="legend-label">${kind}</span>
    </label>`,
  ).join('');
}

function syncLegend() {
  document.querySelectorAll('#legend-items input[type=checkbox]').forEach((cb) => {
    cb.checked = state.edgeFilter.has(cb.dataset.kind);
  });
}

function searchNodes(query) {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  return state.fullData.nodes
    .filter((n) => n.label.toLowerCase().includes(q) || (n.fqn || '').toLowerCase().includes(q))
    .slice(0, 12);
}

async function main() {
  const container = document.getElementById('graph');
  const status = document.getElementById('status');
  try {
    const [graph, styles] = await Promise.all([loadJson('graph.json'), loadJson('styles.json')]);
    state.styles = styles;
    state.fullData = { nodes: graph.nodes, links: graph.links.map((l) => ({ ...l })) };
    indexData(state.fullData);

    // Seed view: start from the highest-degree nodes so the demo opens on the
    // dense, interesting core rather than an empty void; the rest expand on click.
    const seedCount = Math.min(state.fullData.nodes.length, 400);
    const seeds = [...state.fullData.nodes]
      .sort((a, b) => (b.degree || 0) - (a.degree || 0))
      .slice(0, seedCount);
    state.visibleNodeIds = new Set(seeds.map((n) => n.id));

    const Graph = buildGraph(container);
    window.__graph = Graph; // exposed for the Playwright smoke test
    window.__selectForTest = (id) => {
      const node = state.nodeById.get(String(id));
      if (node) {
        if (!state.visibleNodeIds.has(node.id)) {
          state.visibleNodeIds.add(node.id);
          applyData(Graph);
        }
        selectNode(Graph, node);
      }
    };
    applyData(Graph);
    startCameraDrift(Graph);
    buildLegend();
    wireControls(Graph);

    status.textContent = `${graph.meta.keptNodes} nodes · ${graph.meta.keptLinks} links` +
      (graph.meta.capped ? ` (capped from ${graph.meta.totalNodes})` : '');
    document.getElementById('loading').classList.add('hidden');
    window.__graphReady = true;
  } catch (err) {
    status.textContent = `error: ${err.message}`;
    document.getElementById('loading').textContent = `Failed to load graph: ${err.message}`;
    window.__graphError = String(err);
    throw err;
  }
}

function wireControls(Graph) {
  const searchInput = document.getElementById('search');
  const results = document.getElementById('search-results');
  searchInput.addEventListener('input', () => {
    const matches = searchNodes(searchInput.value);
    results.innerHTML = matches
      .map((n) => `<li data-id="${n.id}"><span class="dot" style="background:${nodeColor(n)}"></span>${escapeHtml(n.label)} <em>${n.kind}</em></li>`)
      .join('');
  });
  results.addEventListener('click', (e) => {
    const li = e.target.closest('li');
    if (!li) return;
    const node = state.nodeById.get(li.dataset.id);
    if (!node) return;
    if (!state.visibleNodeIds.has(node.id)) {
      state.visibleNodeIds.add(node.id);
      applyData(Graph);
    }
    selectNode(Graph, node);
    // The force layout needs a tick for positions; fly after a short delay.
    setTimeout(() => flyTo(Graph, node), 200);
    results.innerHTML = '';
    searchInput.value = node.label;
  });

  document.getElementById('legend-items').addEventListener('change', (e) => {
    const kind = e.target.dataset.kind;
    if (!kind) return;
    if (e.target.checked) state.edgeFilter.add(kind);
    else state.edgeFilter.delete(kind);
    applyData(Graph);
  });

  document.getElementById('inspector').addEventListener('click', (e) => {
    if (e.target.classList.contains('who-calls') && e.target.dataset.disabled === 'false') {
      rippleCallers(Graph, state.selectedId);
    }
  });
}

main();
