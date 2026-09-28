import { spawn } from "child_process";
import { existsSync } from "fs";
import { join, resolve } from "path";
import { homedir } from "os";

const LAUNCHER = "{{orbit}}".split(" ");
const GUARD_FLAGS = "{{graph_first}}".split(" ").filter(Boolean);
const MCP_SERVER = "{{mcp_server}}";

function toGuardCall(tool, args) {
  switch (tool) {
    case "bash":
    case "shell":
      return ["search", { tool_name: "Bash", tool_input: { command: args.command } }, args.workdir];
    case "read":
      return [
        "read",
        {
          tool_name: "Read",
          tool_input: { file_path: args.filePath, offset: args.offset, limit: args.limit },
        },
      ];
    case "grep":
      return [
        "search",
        { tool_name: "Grep", tool_input: { pattern: args.pattern, path: args.path, glob: args.include } },
      ];
    case "glob":
      return ["search", { tool_name: "Glob", tool_input: { pattern: args.pattern, path: args.path } }];
  }
  if (tool.startsWith(MCP_SERVER + "_")) {
    return ["search", { tool_name: "mcp__" + MCP_SERVER + "__" + tool.slice(MCP_SERVER.length + 1), tool_input: {} }];
  }
  return null;
}

function runGuard(kind, call) {
  return new Promise((resolve) => {
    let out = "";
    let child;
    try {
      child = spawn(LAUNCHER[0], [...LAUNCHER.slice(1), "hook-guard", kind, ...GUARD_FLAGS], {
        stdio: ["pipe", "pipe", "ignore"],
      });
    } catch {
      resolve(null);
      return;
    }
    const timer = setTimeout(() => child.kill(), 10000);
    child.on("error", () => {
      clearTimeout(timer);
      resolve(null);
    });
    child.stdout.on("data", (chunk) => {
      out += chunk;
    });
    child.on("close", () => {
      clearTimeout(timer);
      try {
        resolve(JSON.parse(out).hookSpecificOutput ?? null);
      } catch {
        resolve(null);
      }
    });
    child.stdin.on("error", () => {});
    child.stdin.end(JSON.stringify(call));
  });
}

export const OrbitPlugin = async ({ directory }) => {
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit");
  const notes = new Map();
  return {
    "tool.execute.before": async (input, output) => {
      let decision = null;
      try {
        if (!existsSync(join(root, "graph.duckdb"))) return;
        const mapped = toGuardCall(input.tool, output.args ?? {});
        if (!mapped) return;
        const [kind, call, workdir] = mapped;
        const cwd = workdir ? resolve(directory, workdir) : directory;
        decision = await runGuard(kind, { ...call, session_id: input.sessionID, cwd });
      } catch {
        return;
      }
      if (decision?.permissionDecision === "deny") {
        throw new Error(decision.permissionDecisionReason);
      }
      if (decision?.additionalContext) notes.set(input.callID, decision.additionalContext);
    },
    "tool.execute.after": async (input, output) => {
      const note = notes.get(input.callID);
      if (!note) return;
      notes.delete(input.callID);
      output.output = (output.output ?? "") + "\n\n" + note;
    },
  };
};
