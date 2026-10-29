/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */
var SHELL_ONLY = new Set(["<", ">", "#", "$(", "`"]);
var HARMLESS_REDIRECT = /^(?:&>>?|>>?)(?:&\d+|\s*\/dev\/null)(?=[\s;&|)]|$)/;
function simpleCommands(command) {
  const commands = [];
  let words = [];
  let word = "";
  let inWord = false;
  let quote = "";
  let piped = false;
  const endWord = () => {
    if (inWord)
      words.push(word);
    word = "";
    inWord = false;
  };
  const endCommand = () => {
    endWord();
    if (words.length)
      commands.push(piped ? ["|", ...words] : words);
    words = [];
    piped = false;
  };
  for (let i = 0;i < command.length; i++) {
    const c = command[i] ?? "";
    if (quote !== "'" && (c === "`" || c === "$" && command[i + 1] === "("))
      words.push(c === "`" ? "`" : "$(");
    if (quote) {
      if (c === quote)
        quote = "";
      else if (c === "\\" && quote === '"' && i + 1 < command.length)
        word += command[++i];
      else
        word += c;
    } else if (c === "'" || c === '"') {
      quote = c;
      inWord = true;
    } else if (c === "\\" && command[i + 1] === `
`) {
      i++;
      endWord();
    } else if (c === "\\" && i + 1 < command.length) {
      word += command[++i];
      inWord = true;
    } else if ("&>".includes(c) && HARMLESS_REDIRECT.test(command.slice(i))) {
      if (/^\d+$/.test(word) && /\d/.test(command[i - 1] ?? "")) {
        word = "";
        inWord = false;
      } else
        endWord();
      i += (command.slice(i).match(HARMLESS_REDIRECT)?.[0].length ?? 1) - 1;
    } else if (c === "|" && command[i + 1] !== "|") {
      endCommand();
      piped = true;
      if (command[i + 1] === "&")
        i++;
    } else if (c === `
` || ";&|()".includes(c)) {
      endCommand();
      if ("&|".includes(c) && command[i + 1] === c)
        i++;
    } else if ("<>".includes(c) || c === "#" && !inWord) {
      endWord();
      words.push(c);
    } else if (/\s/.test(c)) {
      endWord();
    } else {
      word += c;
      inWord = true;
    }
  }
  endCommand();
  return commands;
}
function orbitWords(words) {
  let i = 0;
  while (/^(?:ORBIT|GLAB)_\w*=/.test(words[i] ?? ""))
    i++;
  if (words[i] === "glab" && words[i + 1] === "orbit")
    i += 2;
  else if (words[i]?.split("/").pop() === "orbit")
    i += 1;
  else
    return null;
  while (words[i]?.startsWith("-"))
    i++;
  return words.slice(i);
}
var PREFIX_WORDS = new Set(["do", "then", "else", "elif", "if", "while", "until", "!", "time", "{"]);
var STRUCTURE = new Set(["for", "done", "fi", "esac", "case", "}"]);
var QUIET = new Set(["cd", "echo", "true", "printf", "head", "tail"]);
function runnable(command) {
  const out = [];
  let heredoc = "";
  for (const words of simpleCommands(command)) {
    if (heredoc) {
      if (words.join(" ") === heredoc)
        heredoc = "";
      continue;
    }
    const at = words.findIndex((word, i) => word === "<" && words[i + 1] === "<");
    if (at >= 0)
      heredoc = (words[at + 2] ?? "").replace(/^-/, "");
    let i = words[0] === "|" ? 1 : 0;
    while (PREFIX_WORDS.has(words[i] ?? ""))
      i++;
    if (i < words.length)
      out.push(words.slice(i));
  }
  return out;
}
function orbitInvocations(command) {
  return runnable(command).map(orbitWords).filter((words) => words !== null);
}
function otherPrograms(command) {
  const names = runnable(command).filter((words) => !orbitWords(words) && !STRUCTURE.has(words[0] ?? "")).map((words) => words.find((word) => !/^\w+=/.test(word))?.split("/").pop() ?? "");
  return [...new Set(names.filter((name) => name && !QUIET.has(name) && !SHELL_ONLY.has(name)))];
}
function subcommandOf(words, known) {
  const [sub, ...rest] = words;
  if (sub === undefined)
    return null;
  const nested = rest.findIndex((word) => known(sub + " " + word));
  if (nested >= 0)
    return { key: sub + " " + rest[nested], args: rest.slice(nested + 1) };
  return known(sub) ? { key: sub, args: rest } : null;
}

/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */
function e(o, t) {
  let m = new Map;
  for (let r = 0;r < o.length; r++) {
    let p = o[r], i = t(p, r, o);
    if (!m.has(i))
      m.set(i, p);
  }
  return Array.from(m.values());
}

var NO_ENTITIES = new Set;
var UNIQUE_KEYS = new Set(["id", "full_path", "username"]);
function entityOfKey(key, known) {
  const m = key.match(/^(\w+)_id$/);
  if (!m?.[1])
    return;
  const name = m[1].split("_").map((w) => (w[0]?.toUpperCase() ?? "") + w.slice(1)).join("");
  return known.has(name) ? name : undefined;
}
function pinOf(keys, equals, label, known) {
  const one = equals.some((key) => UNIQUE_KEYS.has(key));
  if (keys.length === 0)
    return { score: 0, one };
  const implied = keys.map((key) => entityOfKey(key, known)).find((name) => name && name !== label);
  return { score: keys.includes("id") ? 2 : 1, one, ...implied ? { implied } : {} };
}
function pluralize(word) {
  if (/[^aeiou]y$/i.test(word))
    return word.slice(0, -1) + "ies";
  if (/(s|x|z|ch|sh)$/i.test(word))
    return word + "es";
  return word + "s";
}
function displayOf({ type, one }) {
  if (type === "any")
    return "Anything";
  const words = type.split(/(?<=[a-z0-9])(?=[A-Z])/).map((w, i) => i === 0 ? w : w.toLowerCase());
  if (!one)
    words[words.length - 1] = pluralize(words[words.length - 1] ?? "");
  return words.join(" ");
}
function pathOf(run, separator = " \u2192 ") {
  const nodes = [];
  for (const node of run) {
    const last = nodes[nodes.length - 1];
    if (last?.type === node.type)
      last.one &&= node.one;
    else
      nodes.push({ ...node });
  }
  return { nodes, text: nodes.map(displayOf).join(separator) };
}
function entitiesOf(text, known = NO_ENTITIES) {
  const labels = {};
  for (const m of text.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) {
    if (m[1] && m[2])
      labels[m[1]] ??= m[2];
  }
  const whereKeys = {};
  const whereEquals = {};
  for (const clause of text.split(/\bWHERE\b/i).slice(1)) {
    const where = clause.split(/\b(?:MATCH|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT)\b/i)[0] ?? "";
    for (const m of where.matchAll(/\b(\w+)\.(\w+)\s*(=|<>|!=|<=|>=|<|>|\bIN\b|\bSTARTS\b|\bENDS\b|\bCONTAINS\b)/gi)) {
      if (!m[1] || !m[2])
        continue;
      (whereKeys[m[1]] ??= []).push(m[2]);
      if (m[3] === "=")
        (whereEquals[m[1]] ??= []).push(m[2]);
    }
  }
  const paths = [];
  for (const segment of text.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? "";
    for (const part of splitTopLevel(pattern)) {
      const nodes = [];
      const pins = [];
      const hops = [];
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name = "", label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? [];
          const node = label || labels[name] || "any";
          const inline = [...(m[1].match(/\{([^}]*)\}/)?.[1] ?? "").matchAll(/(\w+)\s*:/g)].map((k) => k[1] ?? "");
          const where = name ? whereKeys[name] ?? [] : [];
          const equals = name ? whereEquals[name] ?? [] : [];
          nodes.push(node);
          pins.push(pinOf([...inline, ...where], [...inline, ...equals], node, known));
        } else
          hops.push(m[2] ? "left" : m[3] ? "right" : "none");
      }
      const typed = nodes.map((type, i) => ({ type, one: pins[i]?.one ?? false }));
      paths.push(...readFromPin(typed, pins) ?? readForward(typed, hops));
    }
  }
  const unique = e(paths, (p) => p.text);
  const walks = unique.filter((p) => p.nodes.length > 1);
  const kept = unique.filter((p) => p.nodes.length > 1 || !walks.some((w) => w.nodes.some((n) => n.type === p.nodes[0]?.type)));
  if (kept.length)
    return kept.map((p) => p.text).join(", ");
  const names = e([...text.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)].map((m) => m[1] ?? ""), (name) => name).filter(Boolean);
  return names.map((type) => displayOf({ type, one: false })).join(" \u2192 ");
}
function readFromPin(nodes, pins) {
  const best = Math.max(0, ...pins.map((p) => p.score));
  const at = pins.findIndex((p) => p.score === best);
  if (best === 0 || pins.filter((p) => p.score === best).length > 1)
    return null;
  const implied = pins[at]?.implied;
  const sides = [];
  if (at < nodes.length - 1)
    sides.push(nodes.slice(at));
  if (at > 0)
    sides.push(nodes.slice(0, at + 1).reverse());
  if (sides.length === 0)
    sides.push(nodes.slice(at, at + 1));
  return sides.map((side) => pathOf(implied ? [{ type: implied, one: true }, ...side] : side));
}
function readForward(nodes, hops) {
  if (hops.length === 0)
    return nodes[0] ? [pathOf([nodes[0]])] : [];
  const paths = [];
  let start = 0;
  for (let i = 1;i <= hops.length; i++) {
    if (i < hops.length && hops[i] === hops[start])
      continue;
    const run = nodes.slice(start, i + 1);
    if (hops[start] === "left")
      run.reverse();
    paths.push(pathOf(run, hops[start] === "none" ? " \u2014 " : " \u2192 "));
    start = i;
  }
  return paths;
}
function splitTopLevel(text) {
  const parts = [""];
  let depth = 0;
  for (const ch of text) {
    if ("([{".includes(ch))
      depth++;
    else if (")]}".includes(ch))
      depth--;
    if (ch === "," && depth === 0)
      parts.push("");
    else
      parts[parts.length - 1] += ch;
  }
  return parts.map((p) => p.trim()).filter(Boolean);
}

var labels_default = {
  commands: {
    query: { done: "Explored Orbit", running: "Exploring Orbit" },
    schema: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    ontology: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    status: { done: "Checked Orbit", running: "Checking Orbit" },
    "graph-status": { done: "Checked indexing", running: "Checking indexing" },
    grep: { done: "Searched code", running: "Searching code" },
    context: { done: "Read code", running: "Reading code" },
    "workspace index": { done: "Indexed repo", running: "Indexing repo" },
    "workspace sql": { done: "Queried local graph", running: "Querying local graph" },
    "workspace schema": { done: "Read local schema", running: "Reading local schema" },
    "workspace list": { done: "Listed repos", running: "Listing repos" },
    "workspace mcp": { done: "Served local graph", running: "Serving local graph" },
    "workspace reset": { done: "Reset local graph", running: "Resetting local graph" },
    index: { done: "Indexed repo", running: "Indexing repo" },
    sql: { done: "Queried local graph", running: "Querying local graph" },
    list: { done: "Listed repos", running: "Listing repos" },
    skills: { done: "Read Orbit guide", running: "Reading Orbit guide" },
    skill: { done: "Read Orbit guide", running: "Reading Orbit guide" },
    setup: { done: "Set up Orbit", running: "Setting up Orbit" },
    uninstall: { done: "Removed Orbit setup", running: "Removing Orbit setup" },
    "config get": { done: "Read Orbit settings", running: "Reading Orbit settings" },
    "config list": { done: "Read Orbit settings", running: "Reading Orbit settings" },
    "config set": { done: "Changed Orbit settings", running: "Changing Orbit settings" },
    version: { done: "Checked version", running: "Checking version" }
  },
  tools: {
    query_graph: { done: "Explored Orbit", running: "Exploring Orbit" },
    get_graph_schema: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    get_query_dsl: { done: "Read query syntax", running: "Reading query syntax" },
    get_response_format: { done: "Read response format", running: "Reading response format" },
    list_commands: { done: "Listed Orbit tools", running: "Listing Orbit tools" }
  }
};

var MARK = "\u25CE";
var COMMAND_LABELS = labels_default.commands;
var TOOL_LABELS = labels_default.tools;
var QUERY = labels_default.commands.query;
var SCHEMA = labels_default.commands.schema;
var QUERY_LABEL = QUERY.done;
var VALUE_FLAGS = new Set([
  "--db",
  "--file",
  "--response-format",
  "--full-path",
  "--namespace-id",
  "--project-id",
  "--repo",
  "--budget",
  "--timeout"
]);
var VALUE_FLAGS_BY_KEY = {
  grep: new Set(["-A", "-B", "-C", "-m", "-g", "--glob", "--include", "--exclude", "--exclude-dir", "-t", "--type", "-T", "--type-not", "-M", "--max-columns"]),
  "workspace sql": new Set(["-f", "-F", "--format"])
};
var SHOWS_ARGUMENT = new Set(["grep", "context", "workspace sql", "sql", "schema", "ontology", "graph-status", "skills", "skill"]);
function labelFor(table, key) {
  return Object.hasOwn(table, key) ? table[key] : undefined;
}
function callOf(label, detail) {
  return { label: label.done, active: label.running, detail };
}
function clip(text, max) {
  const flat = text.replace(/\s+/g, " ").trim();
  return flat.length > max ? flat.slice(0, max - 1) + "\u2026" : flat;
}
function firstArg(args, valueFlags) {
  for (let i = 0;i < args.length; i++) {
    const arg = args[i] ?? "";
    if (arg === "-e" || arg === "--regexp")
      return args[i + 1] ?? "";
    if (VALUE_FLAGS.has(arg) || valueFlags.has(arg))
      i++;
    else if (!arg.startsWith("-"))
      return arg;
  }
  return "";
}
function detailFor(key, args, known) {
  if (key === "query") {
    const text = args.join(" ");
    const schema = text.match(/CALL\s+db\.schema\(\s*'?(\w*)/i);
    if (schema)
      return schema[1] ?? "";
    const file = args.indexOf("--file");
    return entitiesOf(text, known) || (file >= 0 ? args[file + 1] ?? "" : "");
  }
  if (!SHOWS_ARGUMENT.has(key))
    return "";
  const valueFlags = VALUE_FLAGS_BY_KEY[key] ?? new Set;
  return clip(firstArg(key.startsWith("skill") && args[0] === "get" ? args.slice(1) : args, valueFlags), 48);
}
function stringField(input, key) {
  const value = typeof input === "object" && input !== null ? input[key] : undefined;
  return typeof value === "string" ? value : undefined;
}
function describeCommand(command, known = NO_ENTITIES) {
  const calls = orbitInvocations(command).flatMap((words) => {
    const sub = subcommandOf(words, (key) => labelFor(COMMAND_LABELS, key) !== undefined);
    if (!sub)
      return [];
    const isSchemaQuery = sub.key === "query" && sub.args.some((arg) => /^\s*CALL\s+db\.schema\(/i.test(arg));
    return [callOf(isSchemaQuery ? SCHEMA : labelFor(COMMAND_LABELS, sub.key) ?? QUERY, detailFor(sub.key, sub.args, known))];
  });
  const [first] = calls;
  if (!first)
    return null;
  const detail = [...new Set(calls.filter((call) => call.label === first.label).map((call) => call.detail).filter(Boolean))].join(", ");
  const also = otherPrograms(command);
  return { ...first, detail, ...also.length ? { also } : {} };
}
function describeOrbitCall(tool, input, known = NO_ENTITIES) {
  const command = stringField(input, "command");
  if (tool.toLowerCase() === "bash")
    return command === undefined ? null : describeCommand(command, known);
  const [, server = "", name = tool] = tool.match(/^mcp__(.+?)__(.+)$/) ?? [];
  if (!/orbit|knowledge[-_]?graph|gkg/i.test(server) && !/orbit|query_graph|get_graph_schema/i.test(name))
    return null;
  const invoked = stringField(input, "command_name") ?? command ?? name;
  const key = Object.keys(TOOL_LABELS).find((k) => invoked.endsWith(k));
  return callOf(key && labelFor(TOOL_LABELS, key) || QUERY, input === undefined ? "" : entitiesOf(JSON.stringify(input), known));
}

var OrbitUiPlugin = async ({ client }) => ({
  "tool.execute.before": async (input, output) => {
    try {
      const call = describeOrbitCall(input.tool, output.args);
      if (!call)
        return;
      const message = MARK + " " + call.active + (call.detail ? "  " + call.detail : "") + (call.also ? "  \xB7 also " + call.also.join(", ") : "");
      client.tui.showToast({ body: { message, variant: "info", duration: 2000 } }).catch(() => {});
    } catch {}
  }
});
export {
  OrbitUiPlugin
};
