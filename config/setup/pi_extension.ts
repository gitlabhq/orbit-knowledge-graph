import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const LAUNCHER = "{{orbit}}".split(" ");
const GUARD_FLAGS = "{{graph_first}}".split(" ").filter(Boolean);

type GuardCall = { tool_name: string; tool_input: Record<string, unknown> };
type GuardDecision = {
  permissionDecision?: string;
  permissionDecisionReason?: string;
  additionalContext?: string;
};

function toGuardCall(tool: string, input: Record<string, any>): [string, GuardCall] | null {
  switch (tool) {
    case "bash":
      return ["search", { tool_name: "Bash", tool_input: { command: input.command } }];
    case "read":
      return [
        "read",
        {
          tool_name: "Read",
          tool_input: { file_path: input.path, offset: input.offset, limit: input.limit },
        },
      ];
    case "grep":
      return [
        "search",
        { tool_name: "Grep", tool_input: { pattern: input.pattern, path: input.path, glob: input.glob } },
      ];
    case "find":
      return ["search", { tool_name: "Glob", tool_input: { pattern: input.pattern, path: input.path } }];
  }
  return null;
}

function runGuard(kind: string, call: Record<string, unknown>): Promise<GuardDecision | null> {
  return new Promise((resolve) => {
    let out = "";
    let child: ReturnType<typeof spawn>;
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
    child.stdout?.on("data", (chunk) => {
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
    child.stdin?.on("error", () => {});
    child.stdin?.end(JSON.stringify(call));
  });
}

export default function (pi: any) {
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit");
  const notes = new Map<string, string>();

  pi.on("tool_call", async (event: any, ctx: any) => {
    try {
      if (!existsSync(join(root, "graph.duckdb"))) return;
      const mapped = toGuardCall(event.toolName, event.input ?? {});
      if (!mapped) return;
      const [kind, call] = mapped;
      const decision = await runGuard(kind, {
        ...call,
        session_id: ctx.sessionManager.getSessionId(),
        cwd: ctx.cwd,
      });
      if (decision?.permissionDecision === "deny") {
        return { block: true, reason: decision.permissionDecisionReason };
      }
      if (decision?.additionalContext) notes.set(event.toolCallId, decision.additionalContext);
    } catch {
      return;
    }
  });

  pi.on("tool_result", async (event: any) => {
    const note = notes.get(event.toolCallId);
    if (!note) return;
    notes.delete(event.toolCallId);
    return { content: [...event.content, { type: "text", text: note }] };
  });
}
