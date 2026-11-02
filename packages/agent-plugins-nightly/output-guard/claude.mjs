#!/usr/bin/env node
/**
 * PostToolUse hook: cut a large Bash result down to its head and tail with
 * `updatedToolOutput`, so a noisy command does not fill the context. The full
 * output stays on disk: Claude Code's own persisted file when it already
 * truncated the result (`persistedOutputPath`), otherwise a file written here.
 *
 * ORBIT_OUTPUT_GUARD sets the limit in characters (default 10000, at most 29000 here), or `off`
 *. Any failure passes the result through.
 */

import { randomUUID } from "node:crypto";
import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseOutputLimit, readEnds, shrinkEnds, shrinkOutput } from "./core.mjs";

// Claude Code re-persists a replaced result over about 30,000 characters and shows only a 2 KB preview of it
// (observed on 2.1.312), so the limit stops below that: a larger one would show the model less.
const MAX_LIMIT = 29_000;

function shrinkField(text, limit, persistedPath) {
  if (typeof text !== "string") return undefined;
  if (persistedPath) {
    return shrinkEnds(readEnds(persistedPath, limit), limit, persistedPath);
  }
  if (text.length <= limit) return undefined;
  const path = join(tmpdir(), `orbit-output-${randomUUID()}.txt`);
  writeFileSync(path, text, { mode: 0o600, flag: "wx" });
  return shrinkOutput(text, limit, path);
}

/** Pure entry point: returns the hook's JSON output, or null to pass through. */
export function run(input, env = process.env) {
  const configured = parseOutputLimit(env.ORBIT_OUTPUT_GUARD, () => {});
  const limit = configured === undefined ? undefined : Math.min(configured, MAX_LIMIT);
  const response = input.tool_response;
  if (limit === undefined || input.tool_name !== "Bash" || !response || response.isImage) return null;

  const stdout = shrinkField(response.stdout, limit, response.persistedOutputPath);
  const stderr = shrinkField(response.stderr, limit);
  if (stdout === undefined && stderr === undefined) return null;
  // Without the persisted-output fields Claude Code does not wrap the result in its own "too large" preview,
  // which would show only the start of the cut output and hide the tail and the marker.
  const { persistedOutputPath, persistedOutputSize, ...rest } = response;
  return {
    hookSpecificOutput: {
      hookEventName: "PostToolUse",
      updatedToolOutput: { ...rest, stdout: stdout ?? response.stdout, stderr: stderr ?? response.stderr },
    },
  };
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const output = run(JSON.parse(readFileSync(0, "utf8")));
    if (output) process.stdout.write(`${JSON.stringify(output)}\n`);
  } catch {
    // A guard failure must never disturb the session.
  }
}
