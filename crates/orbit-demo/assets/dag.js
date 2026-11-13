"use strict";

(function () {
  const NS = "http://www.w3.org/2000/svg";
  const W = 236, H = 96, GAP_X = 236, GAP_Y = 48, ARC = 44;
  const NAMES = { clickhouse: "ClickHouse", duckdb: "DuckDB", orbit_code: "Orbit Next", live: "Slack (live)", engine: "Query engine" };
  const KINDS = { relational: "Relational", remote: "Remote", engine: "Engine" };
  const STATES = { waiting: "Waiting", running: "Running", done: "Finished", failed: "Failed" };

  const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

  function kind(backend) {
    if (backend === "engine") return "engine";
    return backend === "clickhouse" || backend === "duckdb" ? "relational" : "remote";
  }

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

  function pill(parent, x, y, value, cls, anchor, limit) {
    const group = el("g", { class: cls }, parent);
    const short = limit && value.length > limit ? value.slice(0, limit - 1) + "…" : value;
    if (short !== value) el("title", {}, group).textContent = value;
    const label = text(group, x, y, short, "", anchor || "middle");
    const box = label.getBBox();
    group.insertBefore(el("rect", { x: box.x - 8, y: box.y - 3, width: box.width + 16, height: box.height + 6, rx: (box.height + 6) / 2 }), label);
    return group;
  }

  // Breadthfirst's `maximal` option throws in headless cytoscape 3.34, so ranks are longest paths.
  function layout(stages, edges) {
    const cy = cytoscape({
      headless: true,
      styleEnabled: true,
      style: [{ selector: "node", style: { width: H, height: W } }],
      elements: stages.map((s) => ({ data: { id: "s" + s.id } }))
        .concat(edges.map((e) => ({ data: { id: e.id, source: "s" + e.from, target: "s" + e.to } }))),
    });
    cy.layout({ name: "breadthfirst", directed: true, avoidOverlap: true, spacingFactor: 1.1, animate: false }).run();
    const rank = {};
    const rankOf = (n) => rank[n.id()] ?? (rank[n.id()] = Math.max(-1, ...n.incomers("node").map(rankOf)) + 1);
    cy.nodes().forEach(rankOf);
    const columns = {};
    cy.nodes().sort((a, b) => a.position().x - b.position().x).forEach((n) => {
      (columns[rank[n.id()]] = columns[rank[n.id()]] || []).push(n.id());
    });
    cy.destroy();
    const ranks = Object.keys(columns).length;
    const tallest = Math.max(...Object.values(columns).map((c) => c.length));
    const skips = edges.some((e) => rank["s" + e.to] - rank["s" + e.from] > 1);
    const pad = Math.max(12, 32 - 6 * ranks);
    const top = pad + (skips ? ARC : 0);
    const positions = {};
    for (const [r, ids] of Object.entries(columns)) {
      const offset = ((tallest - ids.length) * (H + GAP_Y)) / 2;
      ids.forEach((id, i) => { positions[id] = { x: pad + Number(r) * (W + GAP_X), y: top + offset + i * (H + GAP_Y) }; });
    }
    return { positions, width: 2 * pad + ranks * (W + GAP_X) - GAP_X, height: top + pad + tallest * (H + GAP_Y) - GAP_Y };
  }

  function arrows(defs) {
    for (const [id, cls] of [["arrow", "dag-arrow"], ["arrow-flowing", "dag-arrow dag-arrow-flowing"]]) {
      const marker = el("marker", { id, class: cls, viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" }, defs);
      el("path", { d: "M 0 0 L 10 5 L 0 10 z" }, marker);
    }
  }

  function edge(svg, model, positions, e, slots) {
    const a = positions["s" + e.from], b = positions["s" + e.to];
    const group = el("g", { class: "dag-edge " + (e.state || "") }, svg);
    let d, lx, ly;
    if (b.x - a.x > W + GAP_X) {
      const top = Math.min(a.y, b.y) - ARC + 8;
      const x1 = a.x + W - 36, x2 = b.x + 36;
      d = `M ${x1} ${a.y} C ${x1} ${top}, ${x2} ${top}, ${x2} ${b.y}`;
      lx = (x1 + x2) / 2; ly = top + 12;
    } else {
      const inputs = model.edges.filter((x) => x.to === e.to && positions["s" + x.from].x + W + GAP_X >= b.x).length;
      const slot = (slots[e.to] = (slots[e.to] || 0) + 1);
      const x1 = a.x + W, y1 = a.y + H / 2;
      const x2 = b.x, y2 = b.y + (H * slot) / (inputs + 1);
      const mid = (x1 + x2) / 2;
      d = `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`;
      lx = mid; ly = (y1 + y2) / 2 + 4;
    }
    el("path", { d, "marker-end": e.state === "flowing" ? "url(#arrow-flowing)" : "url(#arrow)" }, group);
    pill(group, lx, ly, e.label, "label", "middle", 30);
  }

  function card(svg, stage, p, selected, onSelect) {
    const cls = ["stage-card", kind(stage.backend), stage.status, stage.id === selected ? "selected" : ""].join(" ");
    const group = el("g", { class: cls, transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: "button", "data-stage": stage.id }, svg);
    el("rect", { class: "card", width: W, height: H, rx: 12 }, group);
    el("rect", { class: "accent", x: 10, y: 12, width: 4, height: H - 24, rx: 2 }, group);
    text(group, 26, 26, `Stage ${stage.id} · ${KINDS[kind(stage.backend)]}${stage.hydration ? " · hydration" : ""}`, "kind");
    text(group, 26, 46, Dag.NAMES[stage.backend] || stage.backend, "title");
    text(group, 26, 63, stage.connection ? `connection ${stage.connection}` : "runs in the query engine", "meta");
    pill(group, 34, 84, STATES[stage.status] || stage.status, "pill", "start");
    if (stage.status === "done") text(group, W - 14, 84, `${plural(stage.rows ?? 0, "row")} · ${stage.elapsed.toFixed(1)} ms`, "stats", "end");
    group.addEventListener("click", () => onSelect(stage.id));
    group.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect(stage.id); } });
  }

  function draw(svg, model, selected, onSelect) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    if (!model.stages.length) {
      svg.setAttribute("viewBox", "0 0 1200 120");
      svg.style.maxWidth = "";
      text(svg, 600, 65, "Run a query to see its stages, the key sets between them and their rows.", "dag-empty", "middle");
      return;
    }
    const { positions, width, height } = layout(model.stages, model.edges);
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.style.maxWidth = `${Math.round(width * 1.2)}px`;
    arrows(el("defs", {}, svg));
    const slots = {};
    for (const e of model.edges) edge(svg, model, positions, e, slots);
    for (const stage of model.stages) card(svg, stage, positions["s" + stage.id], selected, onSelect);
  }

  window.Dag = { draw, kind, plural, NAMES, STATES };
})();
