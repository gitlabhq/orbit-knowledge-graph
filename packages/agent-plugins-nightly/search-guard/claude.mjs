#!/usr/bin/env node

import { readFileSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isGuardDisabled, lookupDefinition, lookupTimeoutMs } from "./core.mjs";
import { analyzeSearch, normalizeTerm, orbitTerms } from "./parse.mjs";

// Claude Code copies this file as is, so the launcher is found at run time: `glab orbit` first, then a
// standalone `orbit` on the PATH.
const LAUNCHERS = [["glab", "orbit"], ["orbit"]];

function isExistingFile(path, cwd) {
  try {
    return statSync(resolve(cwd, path)).isFile();
  } catch {
    return false;
  }
}

function statePath(sessionId) {
  return join(tmpdir(), `orbit-search-guard-${String(sessionId).replace(/[^\w-]/g, "_")}.json`);
}

/** Per-session state: terms already handled and a circuit breaker for failed lookups. */
function readState(sessionId) {
  try {
    const { terms, tripped } = JSON.parse(readFileSync(statePath(sessionId), "utf8"));
    return { terms: new Set(terms), tripped: tripped === true };
  } catch {
    return { terms: new Set(), tripped: false };
  }
}

function writeState(sessionId, state) {
  try {
    writeFileSync(statePath(sessionId), JSON.stringify({ terms: [...state.terms], tripped: state.tripped }));
  } catch {}
}

/** The hook's JSON output for a finished `rg`/`grep` search, or null to pass through. */
export async function run(input, env = process.env, lookup = lookupDefinition) {
  if (isGuardDisabled(env.ORBIT_SEARCH_GUARD)) return null;
  const sessionId = input.session_id ?? "unknown";
  const state = readState(sessionId);
  if (input.tool_name === "Bash" && typeof input.tool_input?.command === "string") {
    const queried = orbitTerms(input.tool_input.command).map(normalizeTerm);
    if (queried.length > 0) {
      for (const term of queried) state.terms.add(term);
      writeState(sessionId, state);
      return null;
    }
  }
  if (state.tripped || (input.tool_name !== "Bash" && input.tool_name !== "Grep")) return null;
  const cwd = input.cwd ?? process.cwd();
  const verdict = analyzeSearch(input.tool_name, input.tool_input ?? {}, (path) => isExistingFile(path, cwd));
  if (!("term" in verdict)) return null;
  const key = normalizeTerm(verdict.term);
  if (state.terms.has(key)) return null;
  const { text, failed } = await lookup(verdict.term, cwd, {
    launchers: LAUNCHERS,
    timeoutMs: lookupTimeoutMs(env.ORBIT_SEARCH_GUARD_TIMEOUT_MS),
  });
  state.terms.add(key);
  if (failed) state.tripped = true;
  writeState(sessionId, state);
  if (!text) return null;
  return { hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext: text } };
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const output = await run(JSON.parse(readFileSync(0, "utf8")));
    if (output) process.stdout.write(`${JSON.stringify(output)}\n`);
  } catch {}
}
