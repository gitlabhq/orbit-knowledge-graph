"use strict";

const $ = (id) => document.getElementById(id);
const STEP_MS = 220;
let run = null;
let queue = [];
let timer = null;

function fresh() {
  return { id: null, stages: [], edges: [], logical: [], grants: [], subplans: {}, rows: null, formats: {}, error: null, selected: null, tab: null, done: false, t0: performance.now(), events: [] };
}

function stage(id) {
  return run.stages.find((s) => s.id === id);
}

function describe(edge) {
  const count = edge.count === undefined ? "" : ` · ${Dag.plural(edge.count, edge.kind === "keys" ? "key" : "row")}`;
  if (edge.kind === "keys") {
    const scope = edge.inputs.every((input) => input.scope);
    const bindings = [...new Set(edge.inputs.map((input) => input.binding))].join(", ");
    const types = [...new Set(edge.inputs.map((input) => `(${input.types.join(", ")})`))].join(" ");
    edge.label = scope ? `scope for ${bindings}` : `${bindings} ${types}`;
    edge.title = `${scope ? "Scope keys" : "Keys"} ${types} from stage ${edge.from} drive ${bindings} in stage ${edge.to}${count}`;
  } else {
    edge.label = "rows";
    edge.title = `Rows of stage ${edge.from} into Combine${count}${edge.columns ? `: ${edge.columns.join(", ")}` : ""}`;
  }
}

function apply(event, t) {
  switch (event.event) {
    case "caller": run.grants = event.grants; break;
    case "check": stage(run.current).checks.push(event); break;
    case "plans": run.logical = event.logical; run.subplans = event.stages; break;
    case "split":
      for (const info of event.stages) {
        run.stages.push({ ...info, status: "waiting", rendered: [], samples: [], keys: [], checks: [] });
        for (const input of info.inputs) {
          const kind = input.kind === "rows" ? "rows" : "keys";
          const id = `e${input.from}-${info.id}-${kind}`;
          let edge = run.edges.find((e) => e.id === id);
          if (!edge) run.edges.push(edge = { id, from: input.from, to: info.id, kind, inputs: [] });
          if (kind === "keys") edge.inputs.push({ scope: input.kind === "scope", binding: input.binding, types: input.types });
        }
      }
      break;
    case "started": run.current = event.stage; stage(event.stage).status = "running"; stage(event.stage).startedAt = t; break;
    case "planned": stage(event.stage).planned = event.plan; markRows(event.stage, event.plan); break;
    case "emitted": stage(event.stage).emitted = event.plan; break;
    case "rendered": stage(event.stage).rendered.push(...event.requests); break;
    case "keys": {
      stage(event.to).keys.push(event);
      const edge = run.edges.find((e) => e.from === event.from && e.to === event.to && e.kind === "keys");
      if (edge) { edge.count = event.count; edge.at = t; }
      break;
    }
    case "rows": {
      stage(event.stage).rows = event.count;
      stage(event.stage).rowsAt = t;
      for (const edge of run.edges.filter((e) => e.from === event.stage && e.kind === "rows")) { edge.count = event.count; edge.at = t; }
      break;
    }
    case "samples": stage(event.stage).samples = event.samples; break;
    case "finished": {
      const s = stage(event.stage);
      s.status = "done"; s.elapsed = event.elapsed_ms; s.finishedAt = t;
      break;
    }
    case "failed": { const s = stage(event.stage); s.status = "failed"; s.error = event.error; break; }
    case "done": run.rows = event.rows; run.done = true; break;
    case "error": run.error = event.error; run.done = true; break;
  }
}

// Combine reads its inputs by position, so the names come from each input stage's last output.
function markRows(id, plan) {
  for (const line of plan) {
    const match = /^Rows \{ stage: (\d+), columns: \[([\d, ]*)\] \}/.exec(line);
    const edge = match && run.edges.find((e) => e.from === Number(match[1]) && e.to === id && e.kind === "rows");
    if (!edge) continue;
    const outputs = /-> \[(.*)\]$/.exec(stage(edge.from).emitted?.at(-1) || "")?.[1].split(", ") || [];
    edge.columns = match[2].split(", ").map((i) => outputs[Number(i)]).filter((c) => c && !c.startsWith("_orbit"));
  }
}

function view() {
  return { flow, now: performance.now() - run.t0, speed: 1, wall: performance.now() };
}

let frames = null;

function animate() {
  if (frames) return;
  const step = () => {
    frames = Dag.frame($("dag"), view()) ? requestAnimationFrame(step) : null;
  };
  frames = requestAnimationFrame(step);
}

// The run as it stood `t` ms after it began, rebuilt from its timed events.
function modelAt(t) {
  const final = run;
  run = fresh();
  run.selected = final.selected;
  for (const event of final.events) {
    if (event.at_ms > t) break;
    apply(event, event.at_ms);
  }
  const model = run;
  run = final;
  for (const edge of model.edges) describe(edge);
  return model;
}

let replay = null;

function replayView() {
  return { flow, now: replay.t, speed: replay.speed, wall: replay.wall };
}

function replayReady() {
  const last = run.events[run.events.length - 1].at_ms;
  replay = { t: last, last, playing: false, active: false, speed: Number($("replay-speed").value), wall: 0, shown: -1 };
  replay.t = replayEnd();
  $("replay").hidden = false;
  replayControls();
}

function replayEnd() {
  return replay.last + 700 * replay.speed;
}

function replayControls() {
  const end = replayEnd();
  $("replay-time").value = Math.round((replay.t / end) * 1000);
  $("replay-clock").textContent = `${Math.min(replay.t, replay.last).toFixed(1)} of ${replay.last.toFixed(1)} ms`;
  $("replay-play").textContent = replay.playing ? "Pause" : "Replay";
}

function drawReplay() {
  const model = modelAt(replay.t);
  const v = replayView();
  Dag.draw($("dag"), model, run.selected, select, v);
  const flight = model.edges.filter((e) => Dag.inFlight(model, e, v)).map((e) => {
    const to = model.stages.find((s) => s.id === e.to);
    const target = to && Dag.kind(to.backend) === "combine" ? "Combine" : `stage ${e.to}`;
    return `${Dag.plural(e.count, e.kind === "keys" ? "key" : "row")} stage ${e.from} → ${target}`;
  });
  $("replay-flight").textContent = flight.length ? `In flight: ${flight.join(" · ")}` : "Nothing in flight";
}

function playReplay() {
  if (replay.playing) { replay.playing = false; replayControls(); return; }
  if (replay.t >= replayEnd()) replay.t = 0;
  replay.playing = replay.active = true;
  let before = performance.now();
  const step = (now) => {
    if (!replay || !replay.playing) return;
    const dt = now - before;
    before = now;
    replay.wall += dt;
    replay.t = Math.min(replayEnd(), replay.t + dt * replay.speed);
    const shown = run.events.findIndex((e) => e.at_ms > replay.t);
    if (shown !== replay.shown) { replay.shown = shown; drawReplay(); } else Dag.frame($("dag"), replayView());
    if (replay.t >= replayEnd()) replay.playing = false;
    replayControls();
    if (replay.playing) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
  replayControls();
}

function scrub() {
  replay.playing = false;
  replay.active = true;
  replay.t = (Number($("replay-time").value) / 1000) * replayEnd();
  replay.shown = -1;
  drawReplay();
  replayControls();
}

function select(id) { run.selected = id; run.tab = null; render(); }

function render() {
  for (const edge of run.edges) describe(edge);
  if (replay && replay.active) drawReplay();
  else Dag.draw($("dag"), run, run.selected, select, view());
  if (flow && !(replay && replay.active)) animate();
  const done = run.stages.filter((s) => s.status === "done").length;
  $("dag-summary").textContent = run.stages.length ? `${done} of ${run.stages.length} stages finished` : "";
  Panels.render(run, names, format);
}

function pump() {
  if (!queue.length) { timer = null; return; }
  try {
    const event = queue.shift();
    run.events.push(event);
    apply(event, performance.now() - run.t0);
    if (event.event === "done") { status(Dag.plural(event.count, "row"), "success"); replayReady(); }
    if (event.event === "error") status("Query failed", "danger");
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
  queue = []; timer = null; run = fresh(); replay = null; $("replay").hidden = true; render();
  status("Running", "info");
  const started = ++generation;
  const { run: id } = await post("/api/runs", { query: $("query").value, caller: $("caller").value });
  if (started !== generation) return;
  run.id = id;
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
  const session = caller && (caller.declared.deployment === "zanzibar" ? "Zanzibar-style provider" : "GitLab session");
  $("caller-hint").textContent = caller ? `${caller.declared.role} · ${session}` : "";
}

function paint() {
  Highlight.render($("query-highlight"), Highlight.gql($("query").value + "\n"));
  scrollSync();
}

function scrollSync() {
  $("query-highlight").scrollTop = $("query").scrollTop;
  $("query-highlight").scrollLeft = $("query").scrollLeft;
}

function choose(index) {
  const example = examples[index];
  $("query").value = example.query;
  paint();
  $("caller").value = example.caller;
  $("example-title").textContent = example.title;
  $("example-subtitle").textContent = example.subtitle;
  $("example-subtitle").title = example.subtitle;
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

function flowView(on) {
  flow = on;
  $("view-flow").classList.toggle("segmented-active", on);
  $("view-static").classList.toggle("segmented-active", !on);
  render();
}

function formatView(name) {
  format = name;
  for (const item of $("formats").children) item.classList.toggle("segmented-active", item.dataset.format === name);
  render();
}

function themeLabel() { $("theme").textContent = Theme.current() === "dark" ? "Light" : "Dark"; }

let stream = null;
let generation = 0;
let callers = [];
let examples = [];
let names = { entities: new Set(), relationships: new Set() };
let stepping = true;
let format = "table";
let flow =!matchMedia("(prefers-reduced-motion: reduce)").matches;

async function boot() {
  run = fresh();
  const meta = await (await fetch("/api/meta")).json();
  callers = meta.callers;
  examples = meta.examples;
  names = { entities: new Set(meta.entities), relationships: new Set(meta.relationships) };
  $("caller").replaceChildren(...callers.map((c) => new Option(c.declared.label, c.name)));
  $("examples").replaceChildren(...examplesList());
  if (examples.length) choose(0);
  status("Ready");
  render();
}

document.addEventListener("orbit:render", render);
$("query").addEventListener("input", paint);
$("query").addEventListener("scroll", scrollSync);
new ResizeObserver(scrollSync).observe($("query"));
$("run").addEventListener("click", () => start().catch((e) => status(e.message, "danger")));
$("caller").addEventListener("change", hint);
$("pace-step").addEventListener("click", () => pace(true));
$("pace-instant").addEventListener("click", () => pace(false));
$("view-flow").addEventListener("click", () => flowView(true));
$("replay-play").addEventListener("click", playReplay);
$("replay-time").addEventListener("input", scrub);
$("replay-speed").addEventListener("change", () => { if (replay) { replay.speed = Number($("replay-speed").value); if (!replay.playing) replay.t = Math.min(replay.t, replayEnd()); replayControls(); } });
$("view-static").addEventListener("click", () => flowView(false));
for (const item of $("formats").children) item.addEventListener("click", () => formatView(item.dataset.format));
$("theme").addEventListener("click", () => { Theme.toggle(); themeLabel(); render(); });
$("reload").addEventListener("click", () => post("/api/reload", {})
  .then((r) => boot().then(() => status(`Reloaded ${r.rows} seed rows`, "success")))
  .catch((e) => status(e.message, "danger")));
themeLabel();
$("view-flow").classList.toggle("segmented-active", flow);
$("view-static").classList.toggle("segmented-active", !flow);
boot().catch((e) => status(e.message, "danger"));
