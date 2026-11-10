import { statSync } from "node:fs";
import { resolve } from "node:path";
import { isGuardDisabled, lookupDefinition, lookupTimeoutMs } from "./core.mjs";
import { analyzeSearch, normalizeTerm, orbitTerms } from "./parse.mjs";

// Setup replaces the placeholder with the launcher this install uses: `glab orbit` or `orbit`.
const LAUNCHER = "{{orbit}}".split(" ");

function isFile(cwd) {
  return (path) => {
    try {
      return statSync(resolve(cwd, path)).isFile();
    } catch {
      return false;
    }
  };
}

/**
 * Appends Orbit's definition of an identifier to a finished `rg`/`grep` result. It never blocks or
 * alters the search, and reads only an index that already exists. A timeout or failure stops lookups
 * for the session. `ORBIT_SEARCH_GUARD=off` disables it.
 */
export function registerSearchGuard(pi, lookup = lookupDefinition) {
  if (isGuardDisabled(process.env.ORBIT_SEARCH_GUARD)) return;

  const handled = new Set();
  let tripped = false;

  pi.on("tool_result", async (event, ctx) => {
    try {
      if (event.parentToolCallId) return;
      if (event.toolName.toLowerCase() === "bash" && typeof event.input.command === "string") {
        const queried = orbitTerms(event.input.command);
        if (queried.length > 0) {
          for (const term of queried) handled.add(normalizeTerm(term));
          return;
        }
      }
      if (tripped) return;
      const verdict = analyzeSearch(event.toolName, event.input, isFile(ctx.cwd));
      if (!("term" in verdict)) return;
      const key = normalizeTerm(verdict.term);
      if (handled.has(key)) return;
      handled.add(key);
      const { text, failed } = await lookup(verdict.term, ctx.cwd, {
        launchers: [LAUNCHER],
        timeoutMs: lookupTimeoutMs(process.env.ORBIT_SEARCH_GUARD_TIMEOUT_MS),
        signal: ctx.signal,
      });
      if (failed) tripped = true;
      if (!text) return;
      return { content: [...event.content, { type: "text", text: `\n\n${text}` }], structuredContent: event.structuredContent };
    } catch {}
  });
}

export default registerSearchGuard;
