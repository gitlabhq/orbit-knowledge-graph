import assert from "node:assert/strict";
import { lstatSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll as after, afterEach, beforeAll as before, test } from "bun:test";
import { run } from "../search-guard/claude.mjs";
import { DEFINITION, stubOrbit } from "./search-guard-support.ts";

const savedPath = process.env.PATH;
let scratch = "";
before(() => {
  scratch = mkdtempSync(join(tmpdir(), "search-guard-state-"));
});
after(() => {
  rmSync(scratch, { recursive: true, force: true });
});
afterEach(() => {
  process.env.PATH = savedPath;
});

const env = () => ({ CLAUDE_PLUGIN_DATA: scratch });
let session = 0;
const search = (cwd: string, command: string, extra: Record<string, unknown> = {}) => ({
  session_id: `s${++session}`,
  cwd,
  tool_name: "Bash",
  tool_input: { command },
  ...extra,
});

test("a single-identifier search gets the Orbit definition as context", async () => {
  const stub = stubOrbit();
  process.env.PATH = stub.bin;
  const output = await run(search(stub.repo, "rg parse_widget"), env());
  assert.equal(output?.hookSpecificOutput.hookEventName, "PostToolUse");
  assert.ok(output?.hookSpecificOutput.additionalContext.includes(DEFINITION));
  assert.ok(stub.calls().every((call) => call === "workspace list --format json" || call.includes("--no-index")), stub.calls().join("\n"));
});

test("glab without Orbit falls back to the standalone orbit", async () => {
  const stub = stubOrbit();
  writeFileSync(join(stub.bin, "glab"), "#!/bin/sh\nexit 1\n", { mode: 0o755 });
  process.env.PATH = stub.bin;
  const output = await run(search(stub.repo, "grep -rn parse_widget ."), env());
  assert.ok(output?.hookSpecificOutput.additionalContext.includes("from `orbit grep`"));
});

test("the Grep tool is handled and other tools and plain words are left alone", async () => {
  const stub = stubOrbit();
  process.env.PATH = stub.bin;
  const grep = await run(search(stub.repo, "", { tool_name: "Grep", tool_input: { pattern: "parse_widget" } }), env());
  assert.ok(grep?.hookSpecificOutput.additionalContext.includes(DEFINITION));
  assert.equal(await run(search(stub.repo, "", { tool_name: "Read", tool_input: { pattern: "parse_widget" } }), env()), null);
  assert.equal(await run(search(stub.repo, "rg widget"), env()), null);
});

test("a term is looked up once per session, and an Orbit call counts as a lookup", async () => {
  const stub = stubOrbit();
  process.env.PATH = stub.bin;
  const input = search(stub.repo, "rg parse_widget");
  assert.ok(await run(input, env()));
  assert.equal(await run(input, env()), null);
  const other = search(stub.repo, "glab orbit grep parse_widget");
  assert.equal(await run(other, env()), null);
  assert.equal(await run({ ...other, tool_input: { command: "rg parse_widget" } }, env()), null);
});

test("a failed lookup stops lookups for the session", async () => {
  const stub = stubOrbit({ grepStatus: 2 });
  process.env.PATH = stub.bin;
  const first = search(stub.repo, "rg parse_widget");
  assert.equal(await run(first, env()), null);
  const before = stub.calls().length;
  assert.equal(await run({ ...first, tool_input: { command: "rg other_widget" } }, env()), null);
  assert.equal(stub.calls().length, before);
});

test("ORBIT_SEARCH_GUARD=off disables the hook", async () => {
  const stub = stubOrbit();
  process.env.PATH = stub.bin;
  assert.equal(await run(search(stub.repo, "rg parse_widget"), { ...env(), ORBIT_SEARCH_GUARD: "off" }), null);
  assert.deepEqual(stub.calls(), []);
});

test("state lives in a private per-user directory, and a planted symlink is replaced, not followed", async () => {
  const stub = stubOrbit();
  process.env.PATH = stub.bin;
  const cache = mkdtempSync(join(tmpdir(), "search-guard-cache-"));
  const input = search(stub.repo, "rg parse_widget");
  const victim = join(cache, "victim.txt");
  writeFileSync(victim, "keep");
  const state = join(cache, "orbit", `search-guard-${input.session_id}.json`);
  assert.ok(await run(input, { XDG_CACHE_HOME: cache }));
  assert.equal(lstatSync(join(cache, "orbit")).mode & 0o777, 0o700);
  rmSync(state);
  symlinkSync(victim, state);
  assert.ok(await run(search(stub.repo, "rg other_widget", { session_id: input.session_id }), { XDG_CACHE_HOME: cache }));
  assert.equal(readFileSync(victim, "utf8"), "keep");
  assert.ok(lstatSync(state).isFile());
  rmSync(cache, { recursive: true, force: true });
});
