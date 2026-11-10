#!/usr/bin/env node

import { lstatSync, mkdirSync, readFileSync, realpathSync, renameSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
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

/** A private per-user directory: the plugin data dir, else `~/.cache/orbit`. Undefined when it is not ours. */
function stateDirectory(env) {
  const directory = env.CLAUDE_PLUGIN_DATA || join(env.XDG_CACHE_HOME || join(homedir(), ".cache"), "orbit");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = lstatSync(directory);
  const foreign = typeof process.getuid === "function" && stat.uid !== process.getuid();
  return stat.isDirectory() && !foreign ? directory : undefined;
}

function statePath(env, sessionId) {
  const directory = stateDirectory(env);
  return directory && join(directory, `search-guard-${String(sessionId).replace(/[^\w-]/g, "_")}.json`);
}

function readState(env, sessionId) {
  try {
    const { terms, tripped } = JSON.parse(readFileSync(statePath(env, sessionId), "utf8"));
    return { terms: new Set(terms), tripped: tripped === true };
  } catch {
    return { terms: new Set(), tripped: false };
  }
}

/** Written to a fresh file and renamed over the state, so a planted symlink is replaced, not followed. */
function writeState(env, sessionId, state) {
  try {
    const path = statePath(env, sessionId);
    if (!path) return;
    const pending = `${path}.${process.pid}.tmp`;
    writeFileSync(pending, JSON.stringify({ terms: [...state.terms], tripped: state.tripped }), { mode: 0o600, flag: "wx" });
    renameSync(pending, path);
  } catch {}
}

export async function run(input, env = process.env, lookup = lookupDefinition) {
  if (isGuardDisabled(env.ORBIT_SEARCH_GUARD)) return null;
  const sessionId = input.session_id ?? "unknown";
  const state = readState(env, sessionId);
  if (input.tool_name === "Bash" && typeof input.tool_input?.command === "string") {
    const queried = orbitTerms(input.tool_input.command).map(normalizeTerm);
    if (queried.length > 0) {
      for (const term of queried) state.terms.add(term);
      writeState(env, sessionId, state);
      return null;
    }
  }
  if (state.tripped || (input.tool_name !== "Bash" && input.tool_name !== "Grep")) return null;
  const cwd = input.cwd ?? process.cwd();
  const verdict = analyzeSearch(input.tool_name, input.tool_input ?? {}, (path) => isExistingFile(path, cwd));
  if (!("term" in verdict)) return null;
  const key = normalizeTerm(verdict.term);
  if (state.terms.has(key)) return null;
  const { text, failed, busy } = await lookup(verdict.term, cwd, {
    launchers: LAUNCHERS,
    timeoutMs: lookupTimeoutMs(env.ORBIT_SEARCH_GUARD_TIMEOUT_MS),
  });
  if (!busy) state.terms.add(key);
  if (failed) state.tripped = true;
  writeState(env, sessionId, state);
  if (!text) return null;
  return { hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext: text } };
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const output = await run(JSON.parse(readFileSync(0, "utf8")));
    if (output) process.stdout.write(`${JSON.stringify(output)}\n`);
  } catch {}
}
