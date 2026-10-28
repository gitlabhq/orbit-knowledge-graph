import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const REMINDER = "{{reminder}}";

// OpenCode draws tool rows itself, so a toast is the one place a plugin can show an Orbit call.
// Each key is an orbit command or alias, with its subcommand after a space; a Rust test checks
// the keys against the CLI. Commands not listed here show no toast.
const LABELS = {
  "query": "Exploring Orbit",
  "schema": "Reading Orbit schema",
  "ontology": "Reading Orbit schema",
  "status": "Checking Orbit",
  "graph-status": "Checking indexing",
  "grep": "Searching code",
  "context": "Reading code",
  "workspace index": "Indexing repo",
  "workspace sql": "Querying local graph",
  "workspace schema": "Reading local schema",
  "workspace list": "Listing repos",
  "index": "Indexing repo",
  "sql": "Querying local graph",
  "list": "Listing repos",
  "skills": "Reading Orbit guide",
  "skill": "Reading Orbit guide",
  "setup": "Setting up Orbit",
  "uninstall": "Removing Orbit setup",
  "config get": "Reading Orbit settings",
  "config list": "Reading Orbit settings",
  "config set": "Changing Orbit settings",
  "version": "Checking version",
};

// Splits only at unquoted separators, so orbit named inside an argument is not a call
function simpleCommands(command) {
  const commands = [];
  let words = [];
  let word = "";
  let inWord = false;
  let quote = "";
  const endWord = () => {
    if (inWord) words.push(word);
    word = "";
    inWord = false;
  };
  const endCommand = () => {
    endWord();
    if (words.length) commands.push(words);
    words = [];
  };
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (quote) {
      if (c === quote) quote = "";
      else if (c === "\\" && quote === '"' && i + 1 < command.length) word += command[++i];
      else word += c;
    } else if (c === "'" || c === '"') {
      quote = c;
      inWord = true;
    } else if (c === "\\" && command[i + 1] === "\n") {
      i++;
      endWord();
    } else if (c === "\\" && i + 1 < command.length) {
      word += command[++i];
      inWord = true;
    } else if (c === "\n" || ";&|()".includes(c)) {
      endCommand();
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

// Not exported: OpenCode loads every exported function of a plugin file as a plugin
function orbitLabel(command) {
  for (const words of simpleCommands(String(command || ""))) {
    let i = 0;
    while (i < words.length && /^[A-Za-z_]\w*=/.test(words[i])) i++;
    if (words[i] === "glab" && words[i + 1] === "orbit") i += 2;
    else if (words[i] && words[i].split("/").pop() === "orbit") i += 1;
    else continue;
    while (i < words.length && words[i].startsWith("-")) i++;
    const sub = words[i];
    const args = words.slice(i + 1);
    if (sub === "query" && args.some((w) => /^\s*CALL\s+db\.schema\(/i.test(w))) return LABELS.schema;
    for (const key of [...args.map((w) => sub + " " + w), sub]) {
      if (Object.hasOwn(LABELS, key)) return LABELS[key];
    }
  }
  return null;
}

export const OrbitPlugin = async ({ client } = {}) => {
  let reminded = false;
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit");
  return {
    "tool.execute.before": async (input, output) => {
      if (input.tool !== "bash" || !client || !client.tui) return;
      const label = orbitLabel(output.args && output.args.command);
      if (!label) return;
      // Not awaited: OpenCode runs the command only after this hook returns
      Promise.resolve()
        .then(() => client.tui.showToast({ body: { message: "◎ " + label, variant: "info", duration: 2000 } }))
        .catch(() => {});
    },
    "tool.execute.after": async (input, output) => {
      if (reminded) return;
      if (input.tool !== "bash") return;
      if (!existsSync(join(root, "local", "manifest.duckdb"))) return;
      output.output = output.output + "\n\n" + REMINDER;
      reminded = true;
    },
  };
};
