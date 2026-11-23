#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { alreadyGuarded, parseOutputLimit, readEnds, shrinkEnds, shrinkOutput } from "./core.mjs";

const MAX_LIMIT = 29_000;

function shrinkField(text, limit, persistedPath) {
  if (typeof text !== "string" || alreadyGuarded(text)) return undefined;
  if (persistedPath) {
    return shrinkEnds(readEnds(persistedPath, limit), limit, persistedPath);
  }
  if (text.length <= limit) return undefined;
  const path = join(tmpdir(), `orbit-output-${randomUUID()}.txt`);
  writeFileSync(path, text, { mode: 0o600, flag: "wx" });
  return shrinkOutput(text, limit, path);
}

export function run(input, env = process.env) {
  const configured = parseOutputLimit(env.ORBIT_OUTPUT_GUARD, () => {});
  const limit = configured === undefined ? undefined : Math.min(configured, MAX_LIMIT);
  const response = input.tool_response;
  if (limit === undefined || input.tool_name !== "Bash" || !response || response.isImage) return null;

  const stdout = shrinkField(response.stdout, limit, response.persistedOutputPath);
  const stderr = shrinkField(response.stderr, limit);
  if (stdout === undefined && stderr === undefined) return null;
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
  }
}
