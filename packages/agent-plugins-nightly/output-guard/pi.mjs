import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { alreadyGuarded, parseOutputLimit, readEnds, shrinkEnds, wholeOutput } from "./core.mjs";

const GUARDED_TOOLS = new Set(["bash", "codemode"]);

function scriptOutputLimit(toolName, input) {
  if (toolName.toLowerCase() !== "codemode" || typeof input.code !== "string") return 0;
  const tokens = /^\s*\/\/ @options:.*"max_output_tokens"\s*:\s*(\d+)/.exec(input.code)?.[1];
  return Number(tokens ?? 0) * 4;
}

/**
 * The spilled file lacks the codemode header and the status after the bash notice. Without them a
 * truncated failure would read as a success.
 */
function withPiFraming(original, body, fullOutputPath) {
  const header = /^Script (?:completed|failed)\nWall time [^\n]*\nOutput:\n/.exec(original)?.[0] ?? "";
  const notice = original.lastIndexOf(`Full output: ${fullOutputPath}`);
  const closing = notice === -1 ? -1 : original.indexOf("]", notice);
  const status = closing === -1 ? "" : original.slice(closing + 1).trim();
  return `${header}${body}${status ? `\n\n${status}` : ""}`;
}

export function registerOutputGuard(pi) {
  const limit = parseOutputLimit(process.env.ORBIT_OUTPUT_GUARD);
  if (limit === undefined) return;

  pi.on("tool_result", (event) => {
    try {
      if (!GUARDED_TOOLS.has(event.toolName.toLowerCase()) || event.parentToolCallId) return;
      if (event.content.some((part) => part.type !== "text")) return;
      const details = event.details;
      const sizeLimit = Math.max(limit, scriptOutputLimit(event.toolName, event.input));
      const original = event.content.map((part) => (part.type === "text" ? part.text : "")).join("");
      if (alreadyGuarded(original)) return;
      let fullOutputPath = typeof details?.fullOutputPath === "string" ? details.fullOutputPath : undefined;
      if (!fullOutputPath && original.length <= sizeLimit) return;
      const ends = fullOutputPath ? readEnds(fullOutputPath, sizeLimit) : wholeOutput(original);
      if (!fullOutputPath) {
        fullOutputPath = join(tmpdir(), `orbit-output-${randomUUID()}.txt`);
        writeFileSync(fullOutputPath, original, { mode: 0o600, flag: "wx" });
      }
      const body = shrinkEnds(ends, sizeLimit, fullOutputPath);
      if (!body) return;
      const shrunk = fullOutputPath === details?.fullOutputPath ? withPiFraming(original, body, fullOutputPath) : body;
      return { content: [{ type: "text", text: shrunk }], structuredContent: event.structuredContent };
    } catch {
      // A guard failure must never disturb the session.
    }
  });
}

export default registerOutputGuard;
