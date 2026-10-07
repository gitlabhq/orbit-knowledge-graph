import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const LAUNCHER = "{{orbit}}".split(" ");
const GRAPH = join(process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit"), "graph.duckdb");
const shown = new Set();

async function nudge(tool_name, tool_input, cwd) {
  try {
    if ((tool_name !== "Bash" && tool_name !== "Grep") || !existsSync(GRAPH)) return null;
    const stdout = await new Promise((resolve) => {
      const child = spawn(LAUNCHER[0], [...LAUNCHER.slice(1), "hook-guard", "search"], {
        cwd,
        stdio: ["pipe", "pipe", "ignore"],
      });
      let output = "";
      const timer = setTimeout(() => child.kill(), 10000);
      child.on("error", () => { clearTimeout(timer); resolve(""); });
      child.stdout.on("data", (chunk) => { output += chunk; });
      child.on("close", () => { clearTimeout(timer); resolve(output); });
      child.stdin.on("error", () => {});
      child.stdin.end(JSON.stringify({ tool_name, tool_input, cwd }));
    });
    const text = JSON.parse(String(stdout)).hookSpecificOutput?.additionalContext;
    if (!text || shown.has(text)) return null;
    shown.add(text);
    return text;
  } catch {
    return null;
  }
}
