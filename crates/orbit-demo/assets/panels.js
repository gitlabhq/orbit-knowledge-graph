"use strict";

(function () {
  const TABS = ["Plans", "Request", "Security", "Keys and timing", "Rows", "Package YAML"];
  const TONES = { waiting: "neutral", running: "info", done: "success", failed: "danger" };
  const KIND_TONES = { relational: "info", remote: "review", combine: "warning" };

  function node(tag, text, cls) {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = text;
    if (cls) el.className = cls;
    return el;
  }

  const rerender = () => document.dispatchEvent(new Event("orbit:render"));
  let names;
  const heading = (text) => node("h3", text);
  const badge = (text, tone) => { const b = node("span", undefined, `badge badge-${tone}`); b.append(node("span", undefined, "badge-dot"), text); return b; };
  const empty = (title, body) => { const box = node("div", undefined, "empty-state"); box.append(node("p", title, "empty-state-title")); if (body) box.append(node("p", body, "empty-state-body")); return box; };
  const isCombine = (s) => Dag.kind(s.backend) === "combine";

  function button(text, cls, onClick) {
    const b = node("button", text, cls);
    b.type = "button";
    b.addEventListener("click", () => { onClick(); rerender(); });
    return b;
  }

  // Rewrites renumber plan ids, so lines compare with their ids masked.
  const shape = (line) => line.replace(/PlanId\(\d+\)/g, "PlanId");

  function highlighted(cls, tokens) {
    const block = node("pre", undefined, `code ${cls}`);
    Highlight.render(block, tokens);
    return block;
  }

  const sql = (text) => highlighted("code-sql", Highlight.sql(text));

  function lines(cls, rows, paint) {
    const block = node("pre", undefined, `code ${cls}`);
    if (!rows.length) { block.textContent = "(none)"; return block; }
    for (const [row, mark] of rows) {
      const span = node("span", undefined, mark || "line");
      paint(span, row);
      block.append(span);
    }
    return block;
  }

  const plan = (rows, added) => lines("code-plan", (rows || []).map((row) => [row, added && added.has(row) && "added"]),
    (span, row) => Highlight.render(span, Highlight.plan(row)));

  const yaml = (text, marks) => lines("code-yaml", numbered(text, marks), (span, tokens) => Highlight.render(span, tokens));

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
      for (const c of columns) {
        const td = tr.insertCell(), text = cell(row[c]);
        td.textContent = text;
        td.className = [c.startsWith("_orbit") && "hidden-column", text.includes(" ") ? text.length > 32 && "prose" : "word"].filter(Boolean).join(" ");
      }
    }
    wrap.append(t);
    return wrap;
  }

  function request(r, s) {
    if (r.kind === "sql") return [heading(`${Dag.NAMES[s.backend] || s.backend} SQL`), sql(r.sql), heading("Parameters"), table(r.params.map(([name, value]) => ({ name, value })))];
    if (r.kind === "combine") return [heading("Combine plan: combine the stages' rows, apply the response policy, page the result"), plan(r.plan)];
    const { kind, ...body } = r;
    return [heading(kind === "code" ? "Orbit Next structural search request" : "Slack extraction request"), yaml(JSON.stringify(body, null, 2))];
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

  const ENTITIES = /Entities (\w+): (\w+)/g;
  const TRAVERSALS = /\b(?:Follow|Neighbors|Shortest) (\w+) (<?)-\[([A-Z0-9_|]+)\]-(>?) (\w+)(?:: (\w+))?/g;

  const entities = (text) => Object.fromEntries([
    ...[...text.matchAll(ENTITIES)].map(([, binding, entity]) => [binding, entity]),
    ...[...text.matchAll(TRAVERSALS)].filter((step) => step[6]).map((step) => [step[5], step[6]]),
  ]);

  const variant = (source, target) => new RegExp(`\\b${source || "\\w+"} -> ${target || "\\w+"}\\b`);

  function marks(plans, query) {
    const text = plans.join("\n"), found = new Set(), bound = entities(text), variants = new Map();
    for (const entity of Object.values(bound)) found.add(entity);
    const read = (binding, property) => { if (bound[binding]) found.add(`${bound[binding]}.${property}`); };
    for (const [, binding, property] of text.matchAll(/\b(\w+)\.(\w+)/g)) read(binding, property);
    for (const [, binding, list] of text.matchAll(/\b(\w+)\{([\w, ]+)\}/g)) for (const property of list.split(", ")) read(binding, property);
    const own = [...text.matchAll(TRAVERSALS)].flatMap(([, from, , , , to]) => [from, to]);
    const reads = new Set([...Object.keys(bound), ...Object.values(bound), ...own]);
    const all = query.join("\n"), entity = entities(all);
    for (const [, from, incoming, rels, outgoing, to] of all.matchAll(TRAVERSALS)) {
      const [a, b] = [entity[from], entity[to]];
      if (![from, to, a, b].some((name) => reads.has(name))) continue;
      const ends = incoming ? [variant(b, a)] : outgoing ? [variant(a, b)] : [variant(a, b), variant(b, a)];
      for (const rel of rels.split("|")) { found.add(rel); variants.set(rel, [...(variants.get(rel) || []), ...ends]); }
    }
    return { found, variants };
  }

  const KEY = /^(\s*)(?:-\s+)?([^\s:#'"{}[\],][^:#]*?)\s*:(?:\s|$)/;

  function numbered(text, { found = new Set(), variants = new Map() } = {}) {
    const stack = [];
    const raw = text.replace(/\n$/, "").split("\n");
    return Highlight.yaml(raw.join("\n"), names).map((tokens, i) => {
      const m = tokens.length === 1 && tokens[0][0] === "string" ? null : KEY.exec(raw[i]);
      if (!m) return [tokens];
      const [, indent, key] = m;
      while (stack.length && stack[stack.length - 1].indent >= indent.length) stack.pop();
      const entity = stack.findLast((outer) => found.has(outer.key))?.key;
      const rel = stack.findLast((outer) => variants.has(outer.key))?.key;
      stack.push({ indent: indent.length, key });
      const traversed = rel && variants.get(rel).some((pattern) => pattern.test(raw[i]));
      const named = found.has(key) || traversed || found.has(`${entity}.${key}`);
      return [tokens, named && "line named"];
    });
  }

  const files = new Map();

  function packageFiles(name) {
    if (!files.has(name)) {
      files.set(name, null);
      fetch(`/api/packages/${encodeURIComponent(name)}`)
        .then((response) => (response.ok ? response.json() : []))
        .catch(() => [])
        .then((list) => { files.set(name, list); rerender(); });
    }
    return files.get(name);
  }

  function packages(run, s) {
    const names = (run.subplans[s.id] || {}).packages || [];
    if (!names.length) return [empty("No packages", "This stage reads nothing a package declares.")];
    const view = s.yaml ||= { package: names[0], path: null };
    const picker = node("div", undefined, "segmented");
    picker.setAttribute("role", "group");
    picker.setAttribute("aria-label", "Package");
    picker.append(...names.map((name) => button(name, `segmented-item${name === view.package ? " segmented-active" : ""}`, () => { view.package = name; view.path = null; })));
    const parts = [heading(isCombine(s) ? "Packages of every stage Combine reads" : "Packages that declare what this stage reads"), picker];
    const list = packageFiles(view.package);
    if (!list) return [...parts, empty("Loading the package", view.package)];
    if (!list.length) return [...parts, empty("No package files", `The server holds no YAML for ${view.package}.`)];
    const shown = list.find((f) => f.path === view.path) || list.find((f) => f.path === `storage/${s.backend}.yaml`) || list.find((f) => f.path === "graph.yaml") || list[0];
    const nav = node("nav", undefined, "tabs");
    nav.setAttribute("aria-label", "Package file");
    nav.append(...list.map((f) => button(f.path, `tab${f === shown ? " tab-active" : ""}`, () => { view.path = f.path; })));
    const subplans = Object.values(run.subplans).flatMap((sub) => sub.plan || []);
    const plans = isCombine(s) ? subplans : run.subplans[s.id].plan || [];
    return [...parts, nav, yaml(shown.text, marks(plans, [...run.logical, ...subplans]))];
  }

  function body(run, s, tab) {
    const sub = run.subplans[s.id] || {};
    switch (tab) {
      case "Plans": {
        const parts = sub.plan ? [heading("Logical sub-plan"), plan(sub.plan)] : [];
        return [...parts, heading("Planned"), plan(s.planned), heading("Emitted, with what the security and policy passes added"), plan(s.emitted, added(s))];
      }
      case "Request":
        return s.rendered.length ? s.rendered.flatMap((r) => request(r, s)) : [empty("No request", "This stage rendered no request.")];
      case "Security": {
        const checks = s.checks.map((c) => ({ provider: c.provider, permission: c.permission, resource: c.resource, asked: c.asked, allowed: c.allowed }));
        return [heading("Operations the emitted plan adds"), plan([...added(s)]),
          heading("Row checks the stage's remote rows went through"), checks.length ? table(checks) : empty("No row checks", "No provider was asked about this stage's rows.")];
      }
      case "Keys and timing": {
        const facts = node("div", undefined, "facts");
        facts.append(fact("Status", Dag.STATES[s.status] || s.status), fact("Rows", String(s.rows ?? "–")), fact("Elapsed", s.elapsed === undefined ? "–" : `${s.elapsed.toFixed(2)} ms`));
        const parts = [facts];
        if (s.error) parts.push(node("div", s.error, "alert alert-danger"));
        if (!isCombine(s)) {
          const keys = s.keys.map((k) => ({ from: `stage ${k.from}`, binding: k.binding, count: k.count, sample: k.sample }));
          parts.push(heading("Key sets this stage read"), keys.length ? table(keys) : empty("No key sets", "This stage reads no keys from earlier stages."));
        }
        const flows = run.edges.filter((e) => e.kind === "rows" && (e.from === s.id || e.to === s.id))
          .map((e) => ({ from: `stage ${e.from}`, to: `stage ${e.to}`, rows: e.count ?? "", columns: (e.columns || []).join(", ") }));
        if (flows.length) parts.push(heading(isCombine(s) ? "Rows Combine reads" : "Rows this stage hands to Combine"), table(flows));
        return parts;
      }
      case "Package YAML":
        return packages(run, s);
      default:
        if (s.samples.length) return s.samples.flatMap((x) => [heading(`${x.request}: ${x.count} rows before the stage's checks`), table(x.rows)]);
        return s.id === run.stages[run.stages.length - 1]?.id ? [heading("Combine output"), table(run.rows)] : [empty("No executor call", "This stage read nothing.")];
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
      const b = button(t, `tab${t === run.tab ? " tab-active" : ""}`, () => { run.tab = t; });
      const count = t === "Rows" ? s.rows : t === "Security" ? s.checks.length || undefined : undefined;
      if (count !== undefined) b.append(node("span", String(count), "tab-count"));
      return b;
    }));
    nav.append(button("Back to the logical plan", "button button-sm button-tertiary tabs-trailing", () => { run.selected = null; }));
  }

  function formatted(run, format) {
    if (!(format in run.formats)) {
      run.formats[format] = null;
      fetch(`/api/runs/${run.id}/result?format=${format}`)
        .then(async (response) => {
          const body = await response.json();
          return response.ok ? { body } : { error: body.error || response.statusText };
        })
        .catch((error) => ({ error: error.message }))
        .then((result) => { run.formats[format] = result; rerender(); });
    }
    const result = run.formats[format];
    if (!result) return empty("Formatting the response", "The server's formatter renders this run's response.");
    if (result.error) return node("div", result.error, "alert alert-danger");
    if (format === "gql") return highlighted("code-table", Highlight.gql(result.body));
    return yaml(typeof result.body === "string" ? result.body : JSON.stringify(result.body, null, 2));
  }

  function render(run, model, format) {
    names = model;
    grants(run);
    const rows = document.getElementById("rows");
    if (run.error) rows.replaceChildren(node("div", run.error, "alert alert-danger"));
    else if (!run.rows) rows.replaceChildren(empty("No results yet", "Pick an example or write a query, then run it."));
    else if (format === "table") rows.replaceChildren(table(run.rows, "No rows for this caller"));
    else rows.replaceChildren(formatted(run, format));
    document.getElementById("count").textContent = run.rows ? String(run.rows.length) : "";
    const title = document.getElementById("title"), meta = document.getElementById("stage-meta");
    const detail = document.getElementById("detail");
    const s = run.stages.find((x) => x.id === run.selected);
    if (!s) {
      title.textContent = "Logical plan";
      meta.replaceChildren(run.stages.length ? node("span", "Select a stage for its plans, request, security, rows and package YAML.") : "");
      document.getElementById("tabs").replaceChildren();
      detail.replaceChildren(run.logical.length ? plan(run.logical) : empty("No plan yet", "The logical plan appears when a query runs."));
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
