import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll as after, beforeAll as before, test } from "bun:test";
import { run } from "../output-guard/claude.mjs";

const savedTmpdir = process.env.TMPDIR;
let scratch = "";
before(() => {
  scratch = mkdtempSync(join(tmpdir(), "output-guard-"));
  process.env.TMPDIR = scratch;
});
after(() => {
  if (savedTmpdir === undefined) delete process.env.TMPDIR;
  else process.env.TMPDIR = savedTmpdir;
  rmSync(scratch, { recursive: true, force: true });
});

const lines = (count: number) => Array.from({ length: count }, (_, i) => `line ${i + 1}`).join("\n");
const bash = (response: Record<string, unknown>, toolName = "Bash") => ({
  tool_name: toolName,
  tool_input: { command: "x" },
  tool_response: { stderr: "", interrupted: false, isImage: false, ...response },
});

function updated(output: ReturnType<typeof run>) {
  assert.equal(output?.hookSpecificOutput.hookEventName, "PostToolUse");
  return output?.hookSpecificOutput.updatedToolOutput as Record<string, unknown>;
}

test("a large Bash result keeps its head and tail and points at the full output", () => {
  const stdout = lines(5000);
  const result = updated(run(bash({ stdout }), {}));
  const text = result.stdout as string;
  assert.ok(text.length < 10_300, `${text.length}`);
  assert.ok(text.startsWith("line 1\nline 2\n"));
  assert.ok(text.endsWith("line 4999\nline 5000"));
  const path = /Full output: (\S+)/.exec(text)?.[1] ?? "";
  assert.equal(readFileSync(path, "utf8"), stdout);
  assert.equal(statSync(path).mode & 0o777, 0o600);
  assert.equal(result.interrupted, false);
});

test("Claude Code's persisted file is reused and read in full", () => {
  const path = join(scratch, "persisted.txt");
  writeFileSync(path, lines(20000));
  const result = updated(run(bash({ stdout: lines(3000), persistedOutputPath: path }), {}));
  const text = result.stdout as string;
  assert.match(text, new RegExp(`Full output: ${path} `));
  assert.ok(text.endsWith("line 20000"));
});

test("multibyte output from the persisted file keeps its head and tail in order", () => {
  const cjk = Array.from({ length: 3000 }, (_, i) => `行${i + 1} ${"漢字".repeat(15)}`).join("\n");
  const path = join(scratch, "persisted-cjk.txt");
  writeFileSync(path, cjk);
  const text = updated(run(bash({ stdout: "preview", persistedOutputPath: path }), {})).stdout as string;
  assert.ok(!text.includes("\uFFFD"));
  const [head, tail] = text.split(/\n\[\.\.\. .*? \.\.\.\]\n/);
  const numbers = (part: string) => part.split("\n").map((line) => Number(/^行(\d+) /.exec(line)?.[1]));
  const first = numbers(head);
  const last = numbers(tail);
  assert.deepEqual(first, Array.from({ length: first.length }, (_, i) => i + 1));
  assert.deepEqual(last, Array.from({ length: last.length }, (_, i) => 3000 - last.length + 1 + i));
  assert.match(text, new RegExp(`of ${Buffer.byteLength(cjk)} bytes omitted`));
});

test("a limit above Claude Code's re-persist threshold is clamped, so the result stays under 30,000 characters", () => {
  const path = join(scratch, "persisted-raised.txt");
  const full = lines(8000);
  writeFileSync(path, full);
  const input = bash({ stdout: full.slice(0, 30_000), stderr: lines(20000), persistedOutputPath: path, persistedOutputSize: full.length });
  const result = updated(run(input, { ORBIT_OUTPUT_GUARD: "100k" }));
  for (const field of ["stdout", "stderr"]) {
    const text = result[field] as string;
    assert.ok(text.length < 30_000, `${field} ${text.length}`);
    assert.match(text, /bytes omitted/);
  }
  assert.equal(result.persistedOutputPath, undefined);
});

test("a large stderr is cut too", () => {
  const result = updated(run(bash({ stdout: "ok", stderr: lines(5000) }), {}));
  assert.equal(result.stdout, "ok");
  assert.match(result.stderr as string, /bytes omitted/);
});

test("small results, other tools, images and the off switch pass through", () => {
  assert.equal(run(bash({ stdout: lines(100) }), {}), null);
  assert.equal(run(bash({ stdout: lines(5000) }, "Read"), {}), null);
  assert.equal(run(bash({ stdout: lines(5000), isImage: true }), {}), null);
  assert.equal(run(bash({ stdout: lines(5000) }), { ORBIT_OUTPUT_GUARD: "off" }), null);
  assert.equal(run(bash({ stdout: lines(100) }), { ORBIT_OUTPUT_GUARD: "500" }) !== null, true);
});

test("the hook reads stdin and prints the JSON output", () => {
  const input = JSON.stringify(bash({ stdout: lines(5000) }));
  const { stdout, status } = spawnSync("node", [new URL("../output-guard/claude.mjs", import.meta.url).pathname], { input, encoding: "utf8" });
  assert.equal(status, 0);
  assert.match(JSON.parse(stdout).hookSpecificOutput.updatedToolOutput.stdout, /bytes omitted/);
  assert.equal(spawnSync("node", [new URL("../output-guard/claude.mjs", import.meta.url).pathname], { input: "not json", encoding: "utf8" }).stdout, "");
});

test("output another guard already cut is left alone", () => {
  const once = updated(run(bash({ stdout: lines(5000) }), {})).stdout as string;
  assert.equal(run(bash({ stdout: once }), { ORBIT_OUTPUT_GUARD: "500" }), null);
});
