import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseOutputLimit, readEnds, shrinkEnds, wholeOutput } from "./core.mjs";

const GUARDED_TOOLS = new Set(["bash", "codemode"]);

/** The output budget a codemode script asked for with `// @options: {"max_output_tokens": N}`, in characters. */
/** @param {string} toolName @param {Record<string, unknown>} input */
function scriptOutputLimit(toolName, input) {
  if (toolName.toLowerCase() !== "codemode" || typeof input.code !== "string") return 0;
  const tokens = /^\s*\/\/ @options:.*"max_output_tokens"\s*:\s*(\d+)/.exec(input.code)?.[1];
  return Number(tokens ?? 0) * 4;
}

/**
 * The parts of a result that Pi wrote around the output it spilled to a file and that the file lacks: the
 * codemode header ("Script failed") and whatever follows the bash notice ("Command exited with code 1").
 * A truncated failure must not read as a success.
 */
/** @param {string} original @param {string} body @param {string} fullOutputPath */
function withPiFraming(original, body, fullOutputPath) {
  const header = /^Script (?:completed|failed)\nWall time [^\n]*\nOutput:\n/.exec(original)?.[0] ?? "";
  const notice = original.lastIndexOf(`Full output: ${fullOutputPath}`);
  const closing = notice === -1 ? -1 : original.indexOf("]", notice);
  const status = closing === -1 ? "" : original.slice(closing + 1).trim();
  return `${header}${body}${status ? `\n\n${status}` : ""}`;
}

/**
 * Cuts a large `bash` or `codemode` result down to its head and tail, so a noisy command does not
 * fill the context. The full output stays on disk: Pi's own temp file when it already truncated
 * the result (`details.fullOutputPath`, read back so the head is the real start), otherwise an owner-only file written here.
 * A codemode script that raised `max_output_tokens` raises the limit with it. Nested calls made by a
 * codemode script are left alone, because the script needs their full output.
 * `ORBIT_OUTPUT_GUARD` sets the limit in characters (default 10000) or `off`.
 */
/** @param {import("@earendil-works/pi-coding-agent").ExtensionAPI} pi */
export function registerOutputGuard(pi) {
  const limit = parseOutputLimit(process.env.ORBIT_OUTPUT_GUARD);
  if (limit === undefined) return;

  pi.on("tool_result", (event) => {
    try {
      if (!GUARDED_TOOLS.has(event.toolName.toLowerCase()) || event.parentToolCallId) return;
      if (event.content.some((part) => part.type !== "text")) return;
      const details = /** @type {{ fullOutputPath?: unknown } | undefined} */ (event.details);
      const sizeLimit = Math.max(limit, scriptOutputLimit(event.toolName, event.input));
      const original = event.content.map((part) => (part.type === "text" ? part.text : "")).join("");
      /** @type {string | undefined} */
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
