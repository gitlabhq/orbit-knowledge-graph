"use strict";

(function () {
  const NS = "http://www.w3.org/2000/svg";
  const H = 92, PAD = 20, LABEL_H = 20, PORT = 16;
  const NAMES = { clickhouse: "ClickHouse", duckdb: "DuckDB lake", orbit_code: "Orbit Next", live: "Live (Data Extraction)", combine: "Combine" };
  const KINDS = { relational: "Relational", remote: "Remote", combine: "In process" };
  const STATES = { waiting: "Waiting", running: "Running", done: "Finished", failed: "Failed" };
  const ARROW = { keys: 2, rows: 1.5 };
  const HEAD = { keys: 5, rows: 6 };

  const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
  const kind = (backend) => (backend === "combine" ? "combine" : backend === "clickhouse" || backend === "duckdb" ? "relational" : "remote");

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

  // Card and label sizes come from the rendered fonts, with room reserved for typical counts and
  // timings, so the layout only moves when a run outgrows them.
  function measure(svg, model) {
    const probe = el("g", { class: "stage-card", visibility: "hidden" }, svg);
    const width = (value, cls, parent) => text(parent || probe, 0, 0, value, cls).getComputedTextLength();
    const pill = width(STATES.done, "", el("g", { class: "chip pill" }, probe)) + 14;
    const cards = {};
    for (const stage of model.stages) {
      const line = lines(stage);
      const stats = width(`${plural(Math.max(stage.rows ?? 0, 99), "row")} · ${Math.max(stage.elapsed ?? 0, 99.9).toFixed(1)} ms`, "stats");
      cards[stage.id] = Math.ceil(26 + Math.max(width(line.kind, "kind"), width(line.title, "title"), width(line.meta, "meta"), pill + 16 + stats) + 16);
    }
    const edges = el("g", { class: "edge edge-keys" }, probe);
    const count = width("000", "", el("g", { class: "chip count" }, edges)) + 14;
    const labels = {};
    for (const e of model.edges) labels[e.id] = Math.ceil(count + 6 + width(e.label, "edge-label", edges) + 12);
    svg.removeChild(probe);
    return { cards, labels };
  }

  function layout(model, size) {
    const g = new dagre.graphlib.Graph({ multigraph: true });
    g.setGraph({ rankdir: "LR", nodesep: 24, ranksep: 24, edgesep: 18, marginx: PAD, marginy: PAD });
    g.setDefaultEdgeLabel(() => ({}));
    for (const s of model.stages) g.setNode(String(s.id), { width: size.cards[s.id], height: H });
    for (const e of model.edges) {
      g.setEdge(String(e.from), String(e.to), { width: size.labels[e.id], height: LABEL_H, labelpos: "c", weight: e.kind === "keys" ? 2 : 1 }, e.id);
    }
    dagre.layout(g);
    const nodes = {};
    for (const s of model.stages) {
      const n = g.node(String(s.id));
      nodes[s.id] = { x: n.x - n.width / 2, y: n.y - H / 2, w: n.width };
    }
    const routes = {};
    for (const e of model.edges) {
      const laid = g.edge(String(e.from), String(e.to), e.id);
      routes[e.id] = { via: laid.points.slice(1, -1), label: { x: laid.x, y: laid.y } };
    }
    ports(model.edges, nodes, routes);
    return { nodes, routes, width: Math.ceil(g.graph().width), height: Math.ceil(g.graph().height) };
  }

  // Each edge gets its own point on the source's right side and the target's left side, ordered
  // by where the edge goes, so edges sharing a card fan out instead of crossing at it.
  function ports(edges, nodes, routes) {
    const spread = (list, node, side, ys) => {
      list.sort((x, y) => ys(x) - ys(y));
      list.forEach((e, i) => {
        const y = nodes[node].y + H / 2 + (i - (list.length - 1) / 2) * PORT;
        routes[e.id][side] = { x: side === "start" ? nodes[node].x + nodes[node].w : nodes[node].x, y };
      });
    };
    const near = (e, end) => { const via = routes[e.id].via; return via.length ? via[end ? via.length - 1 : 0].y : null; };
    for (const id of Object.keys(nodes).map(Number)) {
      spread(edges.filter((e) => e.from === id), id, "start", (e) => near(e, false) ?? nodes[e.to].y);
      spread(edges.filter((e) => e.to === id), id, "end", (e) => near(e, true) ?? nodes[e.from].y);
    }
  }

  function curve(points) {
    let d = `M ${points[0].x} ${points[0].y}`;
    for (let i = 1; i < points.length; i++) {
      const p = points[i - 1], q = points[i], mid = (p.x + q.x) / 2;
      d += ` C ${mid} ${p.y}, ${mid} ${q.y}, ${q.x} ${q.y}`;
    }
    return d;
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

  const since = (view, at) => (view.now - at) / view.speed;
  const eased = (t) => 1 - Math.pow(1 - Math.min(1, Math.max(0, t)), 3);

  // An edge carries data from its event until the stage it feeds finishes.
  function inFlight(model, e, view) {
    const target = model.stages.find((s) => s.id === e.to);
    return e.count > 0 && e.at <= view.now && !(target && target.finishedAt <= view.now);
  }

  function edge(svg, e, route, state, view, model) {
    const group = el("g", { class: `edge edge-${e.kind} ${state}`, "data-edge": e.id }, svg);
    el("title", {}, group).textContent = e.title;
    const tip = ARROW[e.kind] * HEAD[e.kind];
    const end = { x: route.end.x - tip, y: route.end.y };
    const path = el("path", { d: curve([route.start, ...route.via, end]), "marker-end": `url(#arrow-${e.kind}-${state})` }, group);
    if (view.flow) {
      const reveal = el("clipPath", { id: `reveal-${e.id}` }, group);
      const xs = [route.start.x, route.end.x];
      el("rect", { x: xs[0] - 4, y: -1e4, width: 0, height: 2e4, "data-reveal": xs[1] - xs[0] + 8, "data-at": e.at }, reveal);
      group.setAttribute("clip-path", `url(#reveal-${e.id})`);
      if (inFlight(model, e, view)) {
        const dots = el("g", { class: `particles particles-${e.kind}`, "data-length": path.getTotalLength() }, group);
        const n = Math.min(8, 1 + Math.round(Math.log2(e.count + 1) * 1.5));
        for (let i = 0; i < n; i++) {
          el(e.kind === "keys" ? "circle" : "rect", e.kind === "keys" ? { r: 3.6 } : { width: 7, height: 7, rx: 1.5, x: -3.5, y: -3.5 }, dots);
        }
        dots.pathElement = path;
      }
    }
    return group;
  }

  function label(group, e, at, view) {
    const holder = el("g", { class: "edge-tag" }, group);
    const back = el("rect", { class: "tag-back", y: at.y - LABEL_H / 2, height: LABEL_H, rx: LABEL_H / 2 }, holder);
    const used = e.count === undefined ? 0 : badge(holder, 0, at.y, String(e.count), "count") + 6;
    if (view.flow && e.count !== undefined) ticker(holder.querySelector(".count text"), e.count, e.at, (n) => String(n));
    const words = text(holder, used, at.y + 4, e.label, "edge-label");
    const width = used + words.getComputedTextLength() + 12;
    const left = at.x - width / 2;
    holder.setAttribute("transform", `translate(${left + 6} 0)`);
    back.setAttribute("x", -6);
    back.setAttribute("width", width);
  }

  function ticker(node, count, at, format) {
    node.setAttribute("data-tick", count);
    node.setAttribute("data-at", at);
    node.format = format;
  }

  function card(svg, stage, node, selected, onSelect, view) {
    const k = kind(stage.backend), w = node.w;
    const group = el("g", { class: ["stage-card", k, stage.status, stage.id === selected ? "selected" : ""].join(" "), transform: `translate(${node.x} ${node.y})`, tabindex: 0, role: "button", "data-stage": stage.id }, svg);
    el("rect", { class: "card", width: w, height: H, rx: 12 }, group);
    el("rect", { class: "accent", x: 10, y: 12, width: 4, height: H - 24, rx: 2 }, group);
    const line = lines(stage);
    text(group, 26, 25, line.kind, "kind");
    text(group, 26, 44, line.title, "title");
    text(group, 26, 60, line.meta, "meta");
    badge(group, 26, 77, STATES[stage.status] || stage.status, "pill");
    const timing = stage.status === "done" ? ` · ${stage.elapsed.toFixed(1)} ms` : "";
    if (view.flow && stage.rows !== undefined) {
      const stats = text(group, w - 14, 81, `${plural(stage.rows, "row")}${timing}`, "stats", "end");
      ticker(stats, stage.rows, stage.rowsAt, (n) => `${plural(n, "row")}${timing}`);
    } else if (stage.status === "done") {
      text(group, w - 14, 81, `${plural(stage.rows ?? 0, "row")}${timing}`, "stats", "end");
    }
    if (view.flow && stage.status === "running") {
      el("rect", { class: "progress-track", x: 12, y: H - 5, width: w - 24, height: 2, rx: 1 }, group);
      el("rect", { class: "progress-bar", x: 12, y: H - 5, width: 0, height: 2, rx: 1, "data-track": w - 24, "data-full": stage.rowsAt !== undefined ? 1 : 0 }, group);
    }
    group.addEventListener("click", () => onSelect(stage.id));
    group.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect(stage.id); } });
  }

  let cached = { key: "", laid: null };

  function laidOut(svg, model) {
    const size = measure(svg, model);
    const key = JSON.stringify([model.stages.map((s) => s.id), model.edges.map((e) => [e.id, e.from, e.to]), size]);
    if (cached.key !== key) cached = { key, laid: layout(model, size) };
    return cached.laid;
  }

  function draw(svg, model, selected, onSelect, view) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    svg.classList.toggle("flow", view.flow);
    if (!model.stages.length) {
      svg.setAttribute("viewBox", "0 0 1200 120");
      svg.style.maxWidth = "";
      svg.style.minWidth = "";
      text(svg, 600, 65, "Run a query to see its stages, the key sets between them and their rows.", "dag-empty", "middle");
      return;
    }
    const { nodes, routes, width, height } = laidOut(svg, model);
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.style.maxWidth = `${width}px`;
    svg.style.minWidth = `${Math.round(width * 0.7)}px`;
    markers(el("defs", {}, svg));
    const shown = model.edges.filter((e) => !view.flow || e.at <= view.now);
    const groups = shown.map((e) => [e, edge(svg, e, routes[e.id], edgeState(model, e), view, model)]);
    for (const [e, group] of groups) label(group, e, routes[e.id].label, view);
    for (const stage of model.stages) card(svg, stage, nodes[stage.id], selected, onSelect, view);
    frame(svg, view);
  }

  const DRAW_MS = 450, TICK_MS = 650, SWEEP_MS = 1100;
  const SPEED = { keys: 150, rows: 100 };

  // Moves what Flow animates without rebuilding the canvas; returns whether anything still moves.
  function frame(svg, view) {
    let moving = false;
    for (const rect of svg.querySelectorAll("[data-reveal]")) {
      const p = eased(since(view, Number(rect.dataset.at)) / DRAW_MS);
      rect.setAttribute("width", Number(rect.dataset.reveal) * p);
      moving ||= p < 1;
    }
    for (const node of svg.querySelectorAll("[data-tick]")) {
      const p = eased(since(view, Number(node.dataset.at)) / TICK_MS);
      node.textContent = node.format(Math.round(Number(node.dataset.tick) * p));
      moving ||= p < 1;
    }
    for (const bar of svg.querySelectorAll(".progress-bar")) {
      const track = Number(bar.dataset.track);
      if (bar.dataset.full === "1") { bar.setAttribute("x", 12); bar.setAttribute("width", track); continue; }
      const phase = (view.wall % SWEEP_MS) / SWEEP_MS, span = track * 0.3;
      bar.setAttribute("x", 12 + (track + span) * phase - span);
      bar.setAttribute("width", span);
      moving = true;
    }
    for (const dots of svg.querySelectorAll(".particles")) {
      const length = Number(dots.dataset.length), kindOf = dots.classList.contains("particles-keys") ? "keys" : "rows";
      const shapes = [...dots.children];
      shapes.forEach((shape, i) => {
        const at = dots.pathElement.getPointAtLength(((view.wall / 1000) * SPEED[kindOf] + (i * length) / shapes.length) % length);
        if (shape.tagName === "circle") { shape.setAttribute("cx", at.x); shape.setAttribute("cy", at.y); }
        else shape.setAttribute("transform", `translate(${at.x} ${at.y})`);
      });
      moving = true;
    }
    return moving;
  }

  window.Dag = { draw, frame, inFlight, kind, plural, NAMES, KINDS, STATES };
})();
