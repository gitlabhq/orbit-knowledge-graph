"use strict";

(function () {
  const NS = "http://www.w3.org/2000/svg";
  const H = 92, VGAP = 116, PAD = 18, LANE = 26, CHAR = 6.7;
  let W = 0, GAP = 232;
  const NAMES = { clickhouse: "ClickHouse", duckdb: "DuckDB lake", orbit_code: "Orbit Next", live: "Live (Data Extraction)", combine: "Combine" };
  const KINDS = { relational: "Relational", remote: "Remote", combine: "In process" };
  const STATES = { waiting: "Waiting", running: "Running", done: "Finished", failed: "Failed" };
  const ARROW = { keys: 2, rows: 1.5 };
  const HEAD = { keys: 5, rows: 6 };

  const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
  const kind = (backend) => (backend === "combine" ? "combine" : backend === "clickhouse" || backend === "duckdb" ? "relational" : "remote");
  const fit = (text, width) => { const max = Math.floor(width / CHAR); return text.length > max ? text.slice(0, max - 1) + "…" : text; };

  function el(name, attrs, parent) {
    const node = document.createElementNS(NS, name);
    for (const [key, value] of Object.entries(attrs || {})) node.setAttribute(key, value);
    if (parent) parent.appendChild(node);
    return node;
  }

  function text(parent, x, y, value, cls, anchor) {
    const node = el("text", { x, y, class: cls || "", "text-anchor": anchor || "start" }, parent);
    node.textContent = value;
    return node;
  }

  function badge(parent, x, y, value, cls) {
    const group = el("g", { class: `chip ${cls}` }, parent);
    const label = text(group, x + 7, y + 4, value);
    const width = label.getComputedTextLength() + 14;
    group.insertBefore(el("rect", { x, y: y - 8, width, height: 16, rx: 8 }), label);
    return width;
  }

  const lines = (stage) => {
    const k = kind(stage.backend);
    return {
      kind: `Stage ${stage.id} · ${KINDS[k]}`,
      title: NAMES[stage.backend] || stage.backend,
      meta: k === "combine" ? "combines rows · policy · paging" : `connection ${stage.connection}`,
    };
  };

  function cardWidth(svg, stages) {
    const probe = el("g", { class: "stage-card", visibility: "hidden" }, svg);
    const width = (value, cls) => text(probe, 0, 0, value, cls).getComputedTextLength();
    const pill = text(el("g", { class: "chip pill" }, probe), 0, 0, STATES.done).getComputedTextLength() + 14;
    let widest = 0;
    for (const stage of stages) {
      const line = lines(stage);
      const stats = width(`${plural(Math.max(stage.rows ?? 0, 10000), "row")} · ${Math.max(stage.elapsed ?? 0, 9999.9).toFixed(1)} ms`, "stats");
      widest = Math.max(widest, width(line.kind, "kind"), width(line.title, "title"), width(line.meta, "meta"), pill + 16 + stats);
    }
    svg.removeChild(probe);
    return Math.ceil(26 + widest + 16);
  }

  function layout(svg, stages, edges) {
    const data = stages.filter((s) => kind(s.backend) !== "combine").sort((a, b) => a.id - b.id);
    const combines = stages.filter((s) => kind(s.backend) === "combine");
    GAP = data.length > 5 ? 140 : data.length > 3 ? 164 : 232;
    W = cardWidth(svg, stages);
    const column = Object.fromEntries(data.map((s, i) => [s.id, i]));
    const skips = edges
      .filter((e) => e.kind === "keys" && column[e.to] - column[e.from] > 1)
      .sort((x, y) => column[x.to] - column[x.from] - (column[y.to] - column[y.from]));
    const lanes = new Map(skips.map((e, lane) => [e, lane]));
    const top = PAD + (skips.length ? 18 + skips.length * LANE : 0);
    const row = Math.max(1, data.length) * W + Math.max(0, data.length - 1) * GAP;
    const positions = {};
    data.forEach((s, i) => { positions[s.id] = { x: PAD + i * (W + GAP), y: top }; });
    combines.forEach((s, i) => { positions[s.id] = { x: PAD + (row - W) / 2 + i * (W + GAP), y: top + H + VGAP }; });
    const width = 2 * PAD + Math.max(row, combines.length * W + Math.max(0, combines.length - 1) * GAP);
    return { positions, lanes, width, height: top + H + (combines.length ? VGAP + H : 0) + PAD };
  }

  function markers(defs) {
    for (const k of ["keys", "rows"]) for (const state of ["pending", "flowing", "empty", "active"]) {
      const marker = el("marker", { id: `arrow-${k}-${state}`, class: `arrow arrow-${k} ${state}`, viewBox: "0 0 10 10", refX: 0, refY: 5, markerUnits: "strokeWidth", markerWidth: HEAD[k], markerHeight: HEAD[k], orient: "auto" }, defs);
      el("path", { d: "M 0 0 L 10 5 L 0 10 z" }, marker);
    }
  }

  function edgeState(model, e) {
    const target = model.stages.find((s) => s.id === e.to);
    if (target && target.status === "running") return "active";
    if (e.count === undefined) return "pending";
    return e.count === 0 ? "empty" : "flowing";
  }

  function keyEdge(svg, p, e, state, lane) {
    const a = p[e.from], b = p[e.to], tip = ARROW.keys * HEAD.keys;
    const group = el("g", { class: `edge edge-keys ${state}` }, svg);
    el("title", {}, group).textContent = e.title;
    let d, lx, ly, room;
    if (lane !== undefined) {
      const shift = Math.min(lane * LANE, W / 2 - 18), x1 = a.x + W / 2 - shift, x2 = b.x + W / 2 + shift, y = a.y - 16 - lane * LANE;
      d = `M ${x1} ${a.y} V ${y + 10} Q ${x1} ${y} ${x1 + 10} ${y} H ${x2 - 10} Q ${x2} ${y} ${x2} ${y + 10} V ${b.y - tip}`;
      lx = x1 + 16; ly = y - 10; room = x2 - x1 - 40;
    } else {
      const y = a.y + H / 2;
      d = `M ${a.x + W} ${y} H ${b.x - tip}`;
      lx = a.x + W + 10; ly = y - 14; room = GAP - 20;
    }
    el("path", { d, "marker-end": `url(#arrow-keys-${state})` }, group);
    const used = e.count === undefined ? 0 : badge(group, lx, ly, String(e.count), "count") + 6;
    text(group, lx + used, ly + 4, fit(e.label, room - used), "edge-label");
  }

  // Each row edge enters the Combine card at its own point and turns on its own lane; the edge
  // farther from its entry takes the lower lane, so no two horizontal runs meet or cross.
  function routes(rows, p) {
    const out = new Map();
    for (const target of new Set(rows.map((e) => e.to))) {
      const into = rows.filter((e) => e.to === target).sort((x, y) => p[x.from].x - p[y.from].x);
      const b = p[target];
      const entries = into.map((e, slot) => ({ e, sx: p[e.from].x + W / 2, ex: b.x + 28 + ((W - 56) * (slot + 0.5)) / into.length }));
      entries.sort((x, y) => Math.abs(x.sx - x.ex) - Math.abs(y.sx - y.ex));
      entries.forEach((entry, rank) => out.set(entry.e, { ex: entry.ex, lane: 38 + rank * ((VGAP - 58) / Math.max(1, entries.length - 1)) }));
    }
    return out;
  }

  function rowEdge(svg, p, e, state, route) {
    const a = p[e.from], b = p[e.to], tip = ARROW.rows * HEAD.rows, r = 8;
    const group = el("g", { class: `edge edge-rows ${state}` }, svg);
    el("title", {}, group).textContent = e.title;
    const sx = a.x + W / 2, sy = a.y + H, ex = route.ex, ey = b.y - tip;
    const mid = sy + route.lane, dir = Math.sign(ex - sx);
    const d = Math.abs(ex - sx) < 2 * r
      ? `M ${sx} ${sy} C ${sx} ${mid}, ${ex} ${mid}, ${ex} ${ey}`
      : `M ${sx} ${sy} V ${mid - r} Q ${sx} ${mid} ${sx + dir * r} ${mid} H ${ex - dir * r} Q ${ex} ${mid} ${ex} ${mid + r} V ${ey}`;
    el("path", { d, "marker-end": `url(#arrow-rows-${state})` }, group);
    const used = e.count === undefined ? 0 : badge(group, sx + 8, sy + 18, String(e.count), "count") + 6;
    text(group, sx + 8 + used, sy + 22, "rows", "edge-label");
  }

  function card(svg, stage, p, selected, onSelect) {
    const k = kind(stage.backend);
    const group = el("g", { class: ["stage-card", k, stage.status, stage.id === selected ? "selected" : ""].join(" "), transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: "button", "data-stage": stage.id }, svg);
    el("rect", { class: "card", width: W, height: H, rx: 12 }, group);
    el("rect", { class: "accent", x: 10, y: 12, width: 4, height: H - 24, rx: 2 }, group);
    const line = lines(stage);
    text(group, 26, 25, line.kind, "kind");
    text(group, 26, 44, line.title, "title");
    text(group, 26, 60, line.meta, "meta");
    badge(group, 26, 77, STATES[stage.status] || stage.status, "pill");
    if (stage.status === "done") text(group, W - 14, 81, `${plural(stage.rows ?? 0, "row")} · ${stage.elapsed.toFixed(1)} ms`, "stats", "end");
    group.addEventListener("click", () => onSelect(stage.id));
    group.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect(stage.id); } });
  }

  function draw(svg, model, selected, onSelect) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    if (!model.stages.length) {
      svg.setAttribute("viewBox", "0 0 1200 120");
      svg.style.maxWidth = "";
      svg.style.minWidth = "";
      text(svg, 600, 65, "Run a query to see its stages, the key sets between them and their rows.", "dag-empty", "middle");
      return;
    }
    const { positions, lanes: skips, width, height } = layout(svg, model.stages, model.edges);
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.style.maxWidth = `${Math.round(width * 1.12)}px`;
    svg.style.minWidth = `${Math.round(width * 0.62)}px`;
    markers(el("defs", {}, svg));
    const rows = model.edges.filter((e) => e.kind === "rows");
    for (const e of model.edges.filter((e) => e.kind === "keys")) keyEdge(svg, positions, e, edgeState(model, e), skips.get(e));
    const lanes = routes(rows, positions);
    for (const e of rows) rowEdge(svg, positions, e, edgeState(model, e), lanes.get(e));
    for (const stage of model.stages) card(svg, stage, positions[stage.id], selected, onSelect);
  }

  window.Dag = { draw, kind, plural, NAMES, KINDS, STATES };
})();
