import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const LAUNCHER = "{{orbit}}".split(" ");
const GUARD_FLAGS = "{{graph_first}}".split(" ").filter(Boolean);
const MCP_SERVER = "{{mcp_server}}";
const GRAPH = join(process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit"), "graph.duckdb");

async function guard(tool_name, tool_input, session_id, cwd) {
  try {
    if (!tool_name || !existsSync(GRAPH)) return null;
    const kind = tool_name === "Read" ? "read" : "search";
    const call = JSON.stringify({ tool_name, tool_input, session_id, cwd });
    const stdout = await new Promise((resolve) => {
      const child = spawn(LAUNCHER[0], [...LAUNCHER.slice(1), "hook-guard", kind, ...GUARD_FLAGS], {
        stdio: ["pipe", "pipe", "ignore"],
      });
      let output = "";
      const timer = setTimeout(() => child.kill(), 10000);
      child.on("error", () => { clearTimeout(timer); resolve(""); });
      child.stdout.on("data", (chunk) => { output += chunk; });
      child.on("close", () => { clearTimeout(timer); resolve(output); });
      child.stdin.on("error", () => {});
      child.stdin.end(call);
    });
    return JSON.parse(String(stdout)).hookSpecificOutput ?? null;
  } catch {
    return null;
  }
}
