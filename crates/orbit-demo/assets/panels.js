"use strict";

(function () {
  const TABS = ["Plans", "Request", "Security", "Keys and timing", "Rows"];
  const TONES = { waiting: "neutral", running: "info", done: "success", failed: "danger" };
  const KIND_TONES = { relational: "info", remote: "review", engine: "warning" };

  function node(tag, text, cls) {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = text;
    if (cls) el.className = cls;
    return el;
  }

  const heading = (text) => node("h3", text);
  const badge = (text, tone) => { const b = node("span", undefined, `badge badge-${tone}`); b.append(node("span", undefined, "badge-dot"), text); return b; };
  const empty = (title, body) => { const box = node("div", undefined, "empty-state"); box.append(node("p", title, "empty-state-title")); if (body) box.append(node("p", body, "empty-state-body")); return box; };

  // Rewrites renumber plan ids, so lines compare with their ids masked.
  const shape = (line) => line.replace(/PlanId\(\d+\)/g, "PlanId");

  function sql(text) {
    const block = node("pre", undefined, "code code-sql");
    Highlight.render(block, Highlight.sql(text));
    return block;
  }

  function code(lines, added) {
    const block = node("pre", undefined, "code");
    if (!lines || !lines.length) { block.textContent = "(none)"; return block; }
    for (const line of lines) block.append(node("span", line, added && added.has(line) ? "added" : "line"));
    return block;
  }

  function cell(value) {
    if (value === null || value === undefined) return "";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  function table(rows, title) {
    if (!rows || !rows.length) return empty(title || "No rows", "The stage returned nothing for this caller.");
    const wrap = node("div", undefined, "table-scroll");
    const t = node("table", undefined, "table");
    const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const head = t.createTHead().insertRow();
    for (const c of columns) head.append(node("th", c, c.startsWith("_orbit") ? "hidden-column" : ""));
    const body = t.createTBody();
    for (const row of rows) {
      const tr = body.insertRow();
      for (const c of columns) { const td = tr.insertCell(); td.textContent = cell(row[c]); if (c.startsWith("_orbit")) td.className = "hidden-column"; }
    }
    wrap.append(t);
    return wrap;
  }

  function request(r) {
    if (r.kind === "sql") return [heading("ClickHouse SQL"), sql(r.sql), heading("Parameters"), table(r.params.map(([name, value]) => ({ name, value })))];
    if (r.kind === "engine") return [heading("Join plan: join the stages' rows, apply the response policy, page the result"), code(r.plan)];
    const { kind, ...body } = r;
    return [heading(kind === "code" ? "Orbit Next structural search request" : "Slack extraction request"), code(JSON.stringify(body, null, 2).split("\n"))];
  }

  function added(s) {
    const planned = new Set((s.planned || []).map(shape));
    return new Set((s.emitted || []).filter((line) => !planned.has(shape(line))));
  }

  function fact(label, value) {
    const box = node("div", undefined, "fact");
    box.append(node("span", label, "fact-label"), node("span", value, "fact-value"));
    return box;
  }

  function body(run, s, tab) {
    const sub = run.subplans[s.id] || {};
    switch (tab) {
      case "Plans":
        return [heading("Logical sub-plan"), code(sub.plan), heading("Planned"), code(s.planned), heading("Emitted, with what the security and policy passes added"), code(s.emitted, added(s))];
      case "Request":
        return s.rendered.length ? s.rendered.flatMap(request) : [empty("No request", "This stage rendered no request.")];
      case "Security": {
        const lookups = s.rendered.filter((r) => r.kind === "sql" && s.backend !== "clickhouse");
        const checks = s.checks.map((c) => ({ provider: c.provider, permission: c.permission, resource: c.resource, field: c.field, asked: c.asked, allowed: c.allowed }));
        const parts = [heading("Operations the emitted plan adds"), code([...added(s)])];
        parts.push(heading("Row checks the stage's remote rows went through"), checks.length ? table(checks) : empty("No row checks", "No provider was asked about this stage's rows."));
        if (lookups.length) parts.push(heading("Scope lookup on the relational connection"), ...lookups.flatMap(request));
        return parts;
      }
      case "Keys and timing": {
        const facts = node("div", undefined, "facts");
        facts.append(fact("Status", Dag.STATES[s.status] || s.status), fact("Rows", String(s.rows ?? "–")), fact("Elapsed", s.elapsed === undefined ? "–" : `${s.elapsed.toFixed(2)} ms`));
        const parts = [facts];
        if (s.error) { const alert = node("div", s.error, "alert alert-danger"); parts.push(alert); }
        const keys = s.keys.map((k) => ({ from: `stage ${k.from}`, binding: k.binding, count: k.count, sample: k.sample }));
        parts.push(heading("Key sets this stage read"), keys.length ? table(keys) : empty("No key sets", "This stage reads no keys from earlier stages."));
        const flows = run.edges.filter((e) => e.kind === "rows" && (e.from === s.id || e.to === s.id))
          .map((e) => ({ from: `stage ${e.from}`, to: `stage ${e.to}`, rows: e.count ?? "", columns: (e.columns || []).join(", ") }));
        if (flows.length) parts.push(heading(Dag.kind(s.backend) === "engine" ? "Rows this join reads" : "Rows this stage hands to the join"), table(flows));
        return parts;
      }
      default:
        if (s.samples.length) return s.samples.flatMap((x) => [heading(`${x.request}: ${x.count} rows before the stage's checks`), table(x.rows)]);
        return s.id === run.stages[run.stages.length - 1]?.id ? [heading("Join output"), table(run.rows)] : [empty("No executor call", "This stage read nothing.")];
    }
  }

  function grants(run) {
    const list = document.getElementById("grants");
    if (!run.grants.length) { list.replaceChildren(node("span", "Run a query to sign in.", "grants-empty")); return; }
    list.replaceChildren(...run.grants.map((g) => {
      const item = node("div", undefined, "grant");
      const kind = g.answer.startsWith("All") ? "all" : g.answer.startsWith("Subtrees") ? "subtrees" : g.answer.startsWith("Keys") ? "exact keys" : g.answer;
      item.append(node("dt", `${g.scope} · ${g.permission} · ${kind}`));
      const values = node("dd");
      for (const [, value] of g.answer.matchAll(/"([^"]*)"/g)) values.append(node("code", value, "inline-code"));
      item.append(values);
      return item;
    }));
  }

  function tabs(run, s) {
    const nav = document.getElementById("tabs");
    nav.replaceChildren(...TABS.map((t) => {
      const b = node("button", t, `tab${t === run.tab ? " tab-active" : ""}`);
      b.type = "button";
      const count = t === "Rows" ? s.rows : t === "Security" ? s.checks.length || undefined : undefined;
      if (count !== undefined) b.append(node("span", String(count), "tab-count"));
      b.addEventListener("click", () => { run.tab = t; document.dispatchEvent(new Event("orbit:render")); });
      return b;
    }));
    const back = node("button", "Back to the logical plan", "button button-sm button-tertiary tabs-trailing");
    back.type = "button";
    back.addEventListener("click", () => { run.selected = null; document.dispatchEvent(new Event("orbit:render")); });
    nav.append(back);
  }

  function render(run) {
    grants(run);
    const rows = document.getElementById("rows");
    if (run.error) rows.replaceChildren(node("div", run.error, "alert alert-danger"));
    else if (run.rows) rows.replaceChildren(table(run.rows, "No rows for this caller"));
    else rows.replaceChildren(empty("No results yet", "Pick an example or write a query, then run it."));
    document.getElementById("count").textContent = run.rows ? String(run.rows.length) : "";
    const title = document.getElementById("title"), meta = document.getElementById("stage-meta");
    const detail = document.getElementById("detail");
    const s = run.stages.find((x) => x.id === run.selected);
    if (!s) {
      title.textContent = "Logical plan";
      meta.replaceChildren(run.stages.length ? node("span", "Select a stage for its plans, request, security and rows.", "inline-status") : "");
      document.getElementById("tabs").replaceChildren();
      detail.replaceChildren(run.logical.length ? code(run.logical) : empty("No plan yet", "The logical plan appears when a query runs."));
      return;
    }
    run.tab = run.tab || TABS[0];
    title.textContent = `Stage ${s.id} · ${Dag.NAMES[s.backend] || s.backend}`;
    const kind = Dag.kind(s.backend);
    meta.replaceChildren(badge(Dag.KINDS[kind], KIND_TONES[kind]), badge(Dag.STATES[s.status] || s.status, TONES[s.status]));
    tabs(run, s);
    detail.replaceChildren(...body(run, s, run.tab));
  }

  window.Panels = { render };
})();
