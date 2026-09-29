{{hook_client}}
import { resolve } from "node:path";

export const OrbitPlugin = async ({ directory }) => {
  const names = { bash: "Bash", shell: "Bash", read: "Read", grep: "Grep", glob: "Glob" };
  const notes = new Map();
  return {
    "tool.execute.before": async (input, output) => {
      let decision;
      try {
        const args = output.args ?? {};
        const name = names[input.tool] ?? (input.tool.startsWith(MCP_SERVER + "_")
          ? "mcp__" + MCP_SERVER + "__" + input.tool.slice(MCP_SERVER.length + 1) : null);
        const cwd = name === "Bash" && args.workdir ? resolve(directory, args.workdir) : directory;
        decision = await guard(name, { ...args, file_path: args.filePath, glob: args.include }, input.sessionID, cwd);
      } catch {
        return;
      }
      if (decision?.permissionDecision === "deny") throw new Error(decision.permissionDecisionReason);
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
