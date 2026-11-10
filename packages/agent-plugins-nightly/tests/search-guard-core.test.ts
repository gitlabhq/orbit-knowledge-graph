import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  DEFAULT_TIMEOUT_MS,
  MAX_TIMEOUT_MS,
  extractConnections,
  extractDefinition,
  lookupDefinition,
  lookupTimeoutMs,
} from "../search-guard/core.mjs";
import { CONNECTIONS, CONTEXT_OUTPUT, DEFINITION, GREP_OUTPUT, stubOrbit } from "./search-guard-support.ts";

// The guard parses the Orbit CLI's text output. These tests pin the two shapes it depends on, so a
// change to the CLI output fails here instead of silently turning the guard off.

test("the definition is the first block under the `Kind name — path:range` header of grep output", () => {
  assert.equal(extractDefinition(GREP_OUTPUT), DEFINITION);
  assert.equal(extractDefinition("grep \"x\"\nno definitions here\n"), undefined);
});

test("connections come from the context block whose header ends with the definition's location", () => {
  assert.equal(extractConnections(CONTEXT_OUTPUT, "lib.rs:1-1"), CONNECTIONS);
  assert.equal(extractConnections(CONTEXT_OUTPUT, "other.rs:9-9"), undefined);
  assert.equal(extractConnections("parse_widget  [Function]  lib.rs:1-1\nCallers (2):\n", "lib.rs:1-1"), undefined);
});

test("with several definitions the block is picked by location", () => {
  const output = [
    "run  [Function]  a.rs:1-3",
    "",
    "Connections (1 indexed):",
    "  --> x  [calls]  (a.rs:2)",
    "",
    "run  [Function]  b.rs:5-9",
    "",
    "Connections (1 indexed):",
    "  <-- y  [calls]  (b.rs:7)",
  ].join("\n");
  assert.match(extractConnections(output, "b.rs:5-9") ?? "", /<-- y/);
});

test("the latency budget defaults to 1.5 s and is capped", () => {
  assert.equal(lookupTimeoutMs(undefined), DEFAULT_TIMEOUT_MS);
  assert.equal(lookupTimeoutMs("junk"), DEFAULT_TIMEOUT_MS);
  assert.equal(lookupTimeoutMs("250"), 250);
  assert.equal(lookupTimeoutMs("999999"), MAX_TIMEOUT_MS);
});

test("a lookup returns the definition with its connections and never indexes", async () => {
  const stub = stubOrbit();
  const { text } = await lookupDefinition("parse_widget", stub.repo, { launchers: [[stub.launcher("orbit")]] });
  assert.ok(text?.includes(DEFINITION) && text.includes(CONNECTIONS), text);
  const calls = stub.calls();
  assert.equal(calls.length, 3, calls.join("\n"));
  assert.match(calls[0], /^workspace list --format json$/);
  for (const call of calls.slice(1)) assert.match(call, /^(grep|context) --no-index --db \S+graph\.duckdb /, call);
});

test("without an index for the current commit nothing is looked up", async () => {
  const stub = stubOrbit({ indexed: false });
  const result = await lookupDefinition("parse_widget", stub.repo, { launchers: [[stub.launcher("orbit")]] });
  assert.deepEqual(result, {});
  assert.deepEqual(stub.calls(), ["workspace list --format json"]);
});

test("a failing grep reports a failure, and no matches does not", async () => {
  const broken = stubOrbit({ grepStatus: 2 });
  assert.deepEqual(await lookupDefinition("parse_widget", broken.repo, { launchers: [[broken.launcher("orbit")]] }), { failed: true });
  const quiet = stubOrbit({ grepStatus: 1 });
  assert.deepEqual(await lookupDefinition("parse_widget", quiet.repo, { launchers: [[quiet.launcher("orbit")]] }), {});
});

test("the budget stops a slow launcher and reports a failure", async () => {
  const stub = stubOrbit();
  writeFileSync(stub.launcher("orbit"), "#!/bin/sh\nsleep 5\n", { mode: 0o755 });
  const started = Date.now();
  const result = await lookupDefinition("parse_widget", stub.repo, { launchers: [[stub.launcher("orbit")]], timeoutMs: 300 });
  expect(result).toEqual({ failed: true });
  expect(Date.now() - started).toBeLessThan(2000);
});

test("the next launcher is tried when the first cannot list", async () => {
  const stub = stubOrbit();
  const missing = join(stub.bin, "nonexistent");
  const { text } = await lookupDefinition("parse_widget", stub.repo, { launchers: [[missing, "orbit"], [stub.launcher("orbit")]] });
  assert.ok(text?.includes(DEFINITION), text);
  assert.ok(text?.includes(`from \`${stub.launcher("orbit")} grep\``), text);
});
