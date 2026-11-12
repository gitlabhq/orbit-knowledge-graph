"use strict";

(function () {
  const NS = "http://www.w3.org/2000/svg";
  const W = 220, H = 78, GAP_X = 250, MARGIN = 30, TOP = 60;
  const NAMES = { clickhouse: "ClickHouse", duckdb: "DuckDB", orbit_code: "Orbit Next", live: "Slack (live)", engine: "Query engine" };

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

  function text(parent, x, y, value, cls) {
    const node = el("text", { x, y, class: cls || "" }, parent);
    node.textContent = value;
    return node;
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
    const tallest = Math.max(...Object.values(columns).map((c) => c.length));
    const out = {};
    for (const [r, ids] of Object.entries(columns)) {
      const offset = ((tallest - ids.length) * (H + 60)) / 2;
      ids.forEach((id, i) => { out[id] = { x: MARGIN + Number(r) * (W + GAP_X), y: MARGIN + TOP + offset + i * (H + 60) }; });
    }
    const ranks = Object.keys(columns).length;
    cy.destroy();
    return { positions: out, width: MARGIN * 2 + ranks * (W + GAP_X) - GAP_X, height: MARGIN * 2 + TOP + tallest * (H + 60) - 60 };
  }

  function draw(svg, model, selected, onSelect) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    if (!model.stages.length) return;
    const { positions, width, height } = layout(model.stages, model.edges);
    svg.setAttribute("viewBox", `0 0 ${Math.max(width, 3 * (W + GAP_X))} ${Math.max(height, 300)}`);
    const defs = el("defs", {}, svg);
    const marker = el("marker", { id: "arrow", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 8, markerHeight: 8, orient: "auto-start-reverse" }, defs);
    el("path", { d: "M 0 0 L 10 5 L 0 10 z" }, marker);
    const fanIn = {};
    for (const edge of model.edges) {
      const a = positions["s" + edge.from], b = positions["s" + edge.to];
      const slot = (fanIn[edge.to] = (fanIn[edge.to] || 0) + 1);
      const inputs = model.edges.filter((e) => e.to === edge.to).length;
      const skips = b.x - a.x > W + GAP_X;
      const group = el("g", { class: "edge " + (edge.state || "") }, svg);
      let d, lx, ly;
      if (skips) {
        const top = Math.min(a.y, b.y) - 46;
        const x1 = a.x + W - 30, x2 = b.x + 30;
        d = `M ${x1} ${a.y} C ${x1} ${top}, ${x2} ${top}, ${x2} ${b.y}`;
        lx = (x1 + x2) / 2; ly = top + 16;
      } else {
        const x1 = a.x + W, y1 = a.y + H / 2;
        const x2 = b.x, y2 = b.y + (H * slot) / (inputs + 1);
        const mid = (x1 + x2) / 2;
        d = `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`;
        lx = mid; ly = (y1 + y2) / 2 + 4;
      }
      el("path", { d, "marker-end": "url(#arrow)" }, group);
      const label = text(group, lx, ly, edge.label);
      label.setAttribute("text-anchor", "middle");
      const box = label.getBBox();
      const bg = el("rect", { x: box.x - 5, y: box.y - 3, width: box.width + 10, height: box.height + 6 });
      group.insertBefore(bg, label);
    }
    for (const stage of model.stages) {
      const p = positions["s" + stage.id];
      const cls = ["stage", kind(stage.backend), stage.status, stage.id === selected ? "selected" : ""].join(" ");
      const group = el("g", { class: cls, transform: `translate(${p.x} ${p.y})`, tabindex: 0, "data-stage": stage.id }, svg);
      el("rect", { width: W, height: H }, group);
      text(group, 12, 21, `Stage ${stage.id}: ${NAMES[stage.backend] || stage.backend}`, "title");
      text(group, 12, 40, stage.connection ? `connection ${stage.connection}` : "in process", "sub");
      text(group, 12, 62, stage.summary || stage.status);
      group.addEventListener("click", () => onSelect(stage.id));
    }
  }

  window.Dag = { draw, kind, NAMES };
})();
