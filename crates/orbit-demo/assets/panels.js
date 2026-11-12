"use strict";

(function () {
  const TABS = ["Plans", "Request", "Security", "Keys and timing", "Rows"];

  function node(tag, text, cls) {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = text;
    if (cls) el.className = cls;
    return el;
  }

  function heading(text) { return node("h2", text); }

  function pre(lines, added) {
    const block = node("pre");
    for (const line of lines || []) {
      const span = node("span", line + "\n", added && added.has(line) ? "added" : "");
      block.appendChild(span);
    }
    if (!lines || !lines.length) block.textContent = "(none)";
    return block;
  }

  function table(rows) {
    const wrap = node("div", undefined, "table");
    if (!rows || !rows.length) { wrap.appendChild(node("p", "No rows.", "muted")); return wrap; }
    const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const t = node("table");
    const head = t.insertRow();
    for (const c of columns) head.appendChild(node("th", c));
    for (const row of rows) {
      const tr = t.insertRow();
      for (const c of columns) tr.insertCell().textContent = row[c] === null || row[c] === undefined ? "" : typeof row[c] === "object" ? JSON.stringify(row[c]) : String(row[c]);
    }
    wrap.appendChild(t);
    return wrap;
  }

  function request(r) {
    const parts = [];
    if (r.kind === "sql") {
      parts.push(heading("ClickHouse SQL"), node("pre", r.sql));
      parts.push(heading("Parameters"), table(r.params.map(([name, value]) => ({ name, value }))));
    } else if (r.kind === "engine") {
      parts.push(heading("Engine plan"), pre(r.plan));
    } else {
      const { kind, ...body } = r;
      parts.push(heading(kind === "code" ? "Orbit Next structural search request" : "Slack extraction request"), node("pre", JSON.stringify(body, null, 2)));
    }
    return parts;
  }

  // Rewrites renumber plan ids, so lines compare with their ids masked.
  const shape = (line) => line.replace(/PlanId\(\d+\)/g, "PlanId");

  function added(s) {
    const planned = new Set((s.planned || []).map(shape));
    return new Set((s.emitted || []).filter((line) => !planned.has(shape(line))));
  }

  function body(run, s, tab) {
    const sub = run.subplans[s.id] || {};
    switch (tab) {
      case "Plans":
        return [heading("Logical sub-plan"), pre(sub.plan), heading("Planned"), pre(s.planned), heading("Emitted (green: added by the security and policy passes)"), pre(s.emitted, added(s))];
      case "Request":
        return s.rendered.length ? s.rendered.flatMap(request) : [node("p", "This stage rendered no request.", "muted")];
      case "Security": {
        const lookups = s.rendered.filter((r) => r.kind === "sql" && s.backend !== "clickhouse");
        const parts = [heading("Operations the emitted plan adds"), pre([...added(s)])];
        parts.push(heading("Row checks on this stage's remote rows"), table(sub.row_checks || []));
        if (lookups.length) parts.push(heading("Scope lookups on the relational connection"), ...lookups.flatMap(request));
        parts.push(heading("Caller scope answers"), table(run.grants));
        return parts;
      }
      case "Keys and timing": {
        const kv = node("dl", undefined, "kv");
        const facts = [["status", s.status], ["rows", s.rows ?? ""], ["elapsed", s.elapsed === undefined ? "" : `${s.elapsed.toFixed(2)} ms`], ["error", s.error || ""]];
        for (const [k, v] of facts) { kv.appendChild(node("dt", k)); kv.appendChild(node("dd", String(v), k === "error" ? "error-text" : "")); }
        const keys = s.keys.map((k) => ({ from: `stage ${k.from}`, binding: k.binding, count: k.count, sample: JSON.stringify(k.sample) }));
        return [kv, heading("Key sets this stage read"), table(keys)];
      }
      default:
        return s.samples.length
          ? s.samples.flatMap((x) => [heading(`${x.request}: ${x.count} rows before the stage's checks`), table(x.rows)])
          : s.id === run.stages[run.stages.length - 1]?.id ? [heading("Engine output"), table(run.rows)] : [node("p", "No executor call.", "muted")];
    }
  }

  function render(run) {
    const grants = document.getElementById("grants");
    grants.replaceChildren(run.grants.length ? table(run.grants) : node("span", "Run a query to sign in.", "muted"));
    document.getElementById("rows").replaceChildren(run.error ? node("pre", run.error, "error-text") : table(run.rows || []));
    document.getElementById("count").textContent = run.rows ? `(${run.rows.length})` : "";
    const title = document.getElementById("title");
    const tabs = document.getElementById("tabs");
    const detail = document.getElementById("detail");
    const s = run.stages.find((x) => x.id === run.selected);
    if (!s) {
      title.textContent = "Logical plan";
      tabs.replaceChildren();
      detail.replaceChildren(pre(run.logical));
      return;
    }
    run.tab = run.tab || TABS[0];
    title.textContent = `Stage ${s.id}: ${Dag.NAMES[s.backend] || s.backend}${s.hydration ? " hydration" : ""}`;
    const back = node("button", "Logical plan");
    back.addEventListener("click", () => { run.selected = null; document.dispatchEvent(new Event("orbit:render")); });
    tabs.replaceChildren(back, ...TABS.map((t) => {
      const b = node("button", t, t === run.tab ? "active" : "");
      b.addEventListener("click", () => { run.tab = t; document.dispatchEvent(new Event("orbit:render")); });
      return b;
    }));
    detail.replaceChildren(...body(run, s, run.tab));
  }

  window.Panels = { render };
})();
