import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const REMINDER = "{{reminder}}";

// OpenCode draws tool rows itself, so a toast is the one place a plugin can show an Orbit call
const LABELS = {
  query: "Exploring Orbit",
  ontology: "Reading Orbit schema",
  dsl: "Reading query syntax",
  tools: "Listing Orbit tools",
  status: "Checking Orbit",
  "graph-status": "Checking indexing",
  grep: "Searching code",
  context: "Reading code",
  sql: "Querying local graph",
  schema: "Reading local schema",
  list: "Listing repos",
  "repo-map": "Mapping repo",
  index: "Indexing repo",
  skills: "Reading Orbit guide",
  setup: "Setting up Orbit",
  config: "Reading Orbit settings",
  version: "Checking version",
};

// Not exported: OpenCode loads every exported function of a plugin file as a plugin
function orbitLabel(command) {
  const match = String(command || "").match(/\bglab\s+orbit\s+([\w-]+)?/);
  if (!match) return null;
  const sub = match[1] || "";
  if (sub === "query" && /CALL\s+db\.schema\(/i.test(command)) return LABELS.ontology;
  return LABELS[sub] || LABELS.query;
}

export const OrbitPlugin = async ({ client } = {}) => {
  let reminded = false;
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit");
  return {
    "tool.execute.before": async (input, output) => {
      if (input.tool !== "bash") return;
      const label = orbitLabel(output.args && output.args.command);
      if (!label || !client || !client.tui) return;
      try {
        await client.tui.showToast({ body: { message: "◎ " + label, variant: "info", duration: 2000 } });
      } catch {}
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
