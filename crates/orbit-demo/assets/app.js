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
  const cols = edge.columns ? edge.columns.slice(0, 2).join(", ") + (edge.columns.length > 2 ? ", …" : "") : "";
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
      break;
    }
    case "failed": { const s = stage(event.stage); s.status = "failed"; s.error = event.error; break; }
    case "done": run.rows = event.rows; run.done = true; status(Dag.plural(event.count, "row"), "success"); break;
    case "error": run.error = event.error; run.done = true; status("Query failed", "danger"); break;
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
  Dag.draw($("dag"), run, run.selected, (id) => { run.selected = id; run.tab = null; render(); });
  const done = run.stages.filter((s) => s.status === "done").length;
  $("dag-summary").textContent = run.stages.length ? `${done} of ${run.stages.length} stages finished` : "";
  Panels.render(run);
}

function pump() {
  if (!queue.length) { timer = null; return; }
  try {
    apply(queue.shift());
    render();
  } catch (error) {
    status(`Page error: ${error.message}`, "danger");
    console.error(error);
  }
  timer = setTimeout(pump, stepping ? STEP_MS : 0);
}

function status(text, tone) {
  $("status").className = `signal signal-${tone || "neutral"}`;
  $("status").lastElementChild.textContent = text;
}

async function post(path, body) {
  const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const json = await response.json();
  if (!response.ok) throw new Error(json.error || response.statusText);
  return json;
}

async function start() {
  if (timer) clearTimeout(timer);
  if (stream) stream.close();
  queue = []; timer = null; run = fresh(); render();
  status("Running", "info");
  const started = ++generation;
  const { run: id } = await post("/api/runs", { query: $("query").value, caller: $("caller").value });
  if (started !== generation) return;
  const events = new EventSource(`/api/runs/${id}/events`);
  stream = events;
  events.onmessage = (message) => {
    const event = JSON.parse(message.data);
    queue.push(event);
    if (event.event === "done" || event.event === "error") events.close();
    if (!timer) pump();
  };
  events.onerror = () => events.close();
}

function hint() {
  const caller = callers.find((c) => c.name === $("caller").value);
  $("caller-hint").textContent = caller ? (caller.declared.deployment === "zanzibar" ? "Zanzibar-style provider" : "GitLab session") : "";
}

function choose(index) {
  const example = examples[index];
  $("query").value = example.query;
  $("caller").value = example.caller;
  $("example-title").textContent = example.title;
  for (const [i, item] of [...$("examples").children].entries()) item.firstElementChild.classList.toggle("example-active", i === index);
  hint();
}

function examplesList() {
  return examples.map((example, index) => {
    const item = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "example";
    button.textContent = example.title;
    const who = document.createElement("span");
    who.className = "example-caller";
    who.textContent = `as ${example.caller}`;
    button.appendChild(who);
    button.addEventListener("click", () => { choose(index); start().catch((e) => status(e.message, "danger")); });
    item.appendChild(button);
    return item;
  });
}

function pace(step) {
  stepping = step;
  $("pace-step").classList.toggle("segmented-active", step);
  $("pace-instant").classList.toggle("segmented-active", !step);
}

function themeLabel() { $("theme").textContent = Theme.current() === "dark" ? "Light" : "Dark"; }

let stream = null;
let generation = 0;
let callers = [];
let examples = [];
let stepping = true;

async function boot() {
  run = fresh();
  const meta = await (await fetch("/api/meta")).json();
  callers = meta.callers;
  examples = meta.examples;
  $("caller").replaceChildren(...callers.map((c) => new Option(c.label, c.name)));
  $("examples").replaceChildren(...examplesList());
  if (examples.length) choose(0);
  status("Ready");
  render();
}

document.addEventListener("orbit:render", render);
$("run").addEventListener("click", () => start().catch((e) => status(e.message, "danger")));
$("caller").addEventListener("change", hint);
$("pace-step").addEventListener("click", () => pace(true));
$("pace-instant").addEventListener("click", () => pace(false));
$("theme").addEventListener("click", () => { Theme.toggle(); themeLabel(); render(); });
$("reload").addEventListener("click", () => post("/api/reload", {})
  .then((r) => boot().then(() => status(`Reloaded ${r.rows} seed rows`, "success")))
  .catch((e) => status(e.message, "danger")));
themeLabel();
boot().catch((e) => status(e.message, "danger"));
