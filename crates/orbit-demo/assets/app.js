"use strict";

const $ = (id) => document.getElementById(id);
const STEP_MS = 220;
let run = null;
let queue = [];
let timer = null;

function fresh() {
  return { stages: [], edges: [], logical: [], grants: [], subplans: {}, rows: null, error: null, selected: null, tab: null, done: false };
}

function stage(id) {
  let found = run.stages.find((s) => s.id === id);
  if (!found) {
    found = { id, backend: "clickhouse", connection: "main", status: "waiting", rendered: [], samples: [], keys: [], checks: [], hydration: true };
    run.stages.push(found);
    const last = run.stages.filter((s) => s.id < id && !s.hydration).pop();
    if (last) run.edges.push({ id: `e${last.id}-${id}`, from: last.id, to: id, kind: "rows", label: "rows: checked ids" });
  }
  return found;
}

function edgeLabel(edge) {
  if (edge.kind === "keys") return `keys: (${edge.properties.join(", ")})` + (edge.count === undefined ? "" : ` · ${edge.count}`);
  const cols = edge.columns ? edge.columns.slice(0, 3).join(", ") + (edge.columns.length > 3 ? ", …" : "") : "";
  return `rows${edge.count === undefined ? "" : " · " + edge.count}${cols ? ": " + cols : ""}`;
}

function apply(event) {
  switch (event.event) {
    case "caller": run.grants = event.grants; break;
    case "check": stage(run.current).checks.push(event); break;
    case "logical": run.logical = event.plan; break;
    case "split":
      for (const info of event.stages) {
        run.stages.push({ ...info, status: "waiting", rendered: [], samples: [], keys: [], checks: [] });
        run.subplans[info.id] = { plan: info.logical };
        for (const input of info.inputs) {
          run.edges.push({ id: `e${input.from}-${info.id}-${input.kind}`, from: input.from, to: info.id, kind: input.kind, properties: input.properties || [] });
        }
      }
      break;
    case "started": run.current = event.stage; stage(event.stage).status = "running"; break;
    case "planned": stage(event.stage).planned = event.plan; markRows(event.stage, event.plan); break;
    case "emitted": stage(event.stage).emitted = event.plan; break;
    case "rendered": stage(event.stage).rendered.push(...event.requests); break;
    case "keys": {
      stage(event.to).keys.push(event);
      const edge = run.edges.find((e) => e.from === event.from && e.to === event.to && e.kind === "keys");
      if (edge) { edge.count = event.count; edge.state = event.count ? "flowing" : "empty"; }
      break;
    }
    case "rows": {
      stage(event.stage).rows = event.count;
      for (const edge of run.edges.filter((e) => e.from === event.stage && e.kind === "rows")) { edge.count = event.count; edge.state = "flowing"; }
      break;
    }
    case "samples": stage(event.stage).samples = event.samples; break;
    case "finished": {
      const s = stage(event.stage);
      s.status = "done"; s.elapsed = event.elapsed_ms;
      s.summary = `${s.rows ?? 0} rows · ${event.elapsed_ms.toFixed(1)} ms`;
      break;
    }
    case "failed": { const s = stage(event.stage); s.status = "failed"; s.error = event.error; s.summary = "failed"; break; }
    case "done": run.rows = event.rows; run.done = true; status(`${event.count} rows`); break;
    case "error": run.error = event.error; run.done = true; status(event.error, true); break;
  }
}

function markRows(id, plan) {
  for (const line of plan) {
    const match = /^Rows \{ stage: (\d+) \} -> \[(.*)\]$/.exec(line);
    if (!match) continue;
    const edge = run.edges.find((e) => e.from === Number(match[1]) && e.to === id && e.kind === "rows");
    if (edge) edge.columns = match[2].split(", ").map((c) => c.replace(/^\d+\./, "")).filter((c) => !c.startsWith("_orbit"));
  }
}

function render() {
  for (const edge of run.edges) edge.label = edgeLabel(edge);
  for (const s of run.stages) if (s.status === "running") s.summary = "running";
  Dag.draw($("dag"), run, run.selected, (id) => { run.selected = id; run.tab = null; render(); });
  Panels.render(run);
}

function pump() {
  if (!queue.length) { timer = null; return; }
  try {
    apply(queue.shift());
    render();
  } catch (error) {
    status(`page error: ${error.message}`, true);
    console.error(error);
  }
  timer = setTimeout(pump, $("slow").checked ? STEP_MS : 0);
}

function status(text, error) {
  $("status").textContent = text;
  $("status").className = error ? "error" : "";
}

async function post(path, body) {
  const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const json = await response.json();
  if (!response.ok) throw new Error(json.error || response.statusText);
  return json;
}

async function start() {
  if (timer) clearTimeout(timer);
  queue = []; timer = null; run = fresh(); render();
  status("Running");
  const { run: id } = await post("/api/runs", { query: $("query").value, caller: $("caller").value });
  const source = new EventSource(`/api/runs/${id}/events`);
  source.onmessage = (message) => {
    const event = JSON.parse(message.data);
    queue.push(event);
    if (event.event === "done" || event.event === "error") source.close();
    if (!timer) pump();
  };
  source.onerror = () => source.close();
}

async function boot() {
  run = fresh();
  const meta = await (await fetch("/api/meta")).json();
  $("caller").replaceChildren(...meta.callers.map((c) => new Option(c.label, c.name)));
  $("examples").replaceChildren(...meta.examples.map((example) => {
    const item = document.createElement("li");
    item.textContent = example.title;
    const who = document.createElement("small");
    who.textContent = `as ${example.caller}`;
    item.appendChild(who);
    item.addEventListener("click", () => { $("query").value = example.query; $("caller").value = example.caller; start(); });
    return item;
  }));
  if (meta.examples.length) { $("query").value = meta.examples[0].query; $("caller").value = meta.examples[0].caller; }
  status("Ready");
  render();
}

document.addEventListener("orbit:render", render);
$("run").addEventListener("click", () => start().catch((e) => status(e.message, true)));
$("reload").addEventListener("click", () => post("/api/reload", {}).then((r) => status(`Reloaded ${r.rows} seed rows`)).catch((e) => status(e.message, true)));
boot().catch((e) => status(e.message, true));
