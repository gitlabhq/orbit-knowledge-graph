import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll as after, beforeAll as before, test } from "bun:test";
import { DEFAULT_OUTPUT_LIMIT, parseOutputLimit, shrinkOutput } from "../output-guard/core.mjs";
import { registerOutputGuard } from "../output-guard/pi.mjs";

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

type Result = { content: { type: string; text: string }[]; structuredContent?: unknown } | undefined;
type Handler = (event: unknown) => Result;

function guard(env: string | undefined) {
  const saved = process.env.ORBIT_OUTPUT_GUARD;
  if (env === undefined) delete process.env.ORBIT_OUTPUT_GUARD;
  else process.env.ORBIT_OUTPUT_GUARD = env;
  let handler: Handler | undefined;
  registerOutputGuard({ on: (_name: string, fn: Handler) => (handler = fn) } as never);
  if (saved === undefined) delete process.env.ORBIT_OUTPUT_GUARD;
  else process.env.ORBIT_OUTPUT_GUARD = saved;
  return handler;
}

const lines = (count: number) => Array.from({ length: count }, (_, i) => `line ${i + 1}`).join("\n");
const event = (toolName: string, text: string, extra: Record<string, unknown> = {}) => ({
  toolName,
  input: {},
  content: [{ type: "text", text }],
  ...extra,
});

test("the limit defaults to 10000 and ORBIT_OUTPUT_GUARD overrides or disables it", () => {
  const warnings: string[] = [];
  const warn = (message: string) => warnings.push(message);
  assert.equal(parseOutputLimit(undefined, warn), DEFAULT_OUTPUT_LIMIT);
  assert.equal(parseOutputLimit("20k", warn), 20_000);
  assert.equal(parseOutputLimit("5000", warn), 5_000);
  for (const value of ["off", "0", "false", "no", ""]) assert.equal(parseOutputLimit(value, warn), undefined, value);
  assert.deepEqual(warnings, []);
  assert.equal(parseOutputLimit("lots", warn), DEFAULT_OUTPUT_LIMIT);
  assert.equal(warnings.length, 1);
  assert.equal(guard("off"), undefined);
});

test("a large bash result keeps its head and tail and points at the full output", () => {
  const text = lines(5000);
  const result = guard(undefined)?.(event("bash", text));
  const shrunk = result?.content[0].text ?? "";
  assert.ok(shrunk.length < DEFAULT_OUTPUT_LIMIT + 300, `${shrunk.length}`);
  assert.ok(shrunk.startsWith("line 1\nline 2\n"));
  assert.ok(shrunk.endsWith("line 4999\nline 5000"));
  const path = /Full output: (\S+)/.exec(shrunk)?.[1] ?? "";
  assert.equal(readFileSync(path, "utf8"), text);
  assert.equal(statSync(path).mode & 0o777, 0o600);
});

test("when Pi already truncated, the head and the omitted count come from its full file", () => {
  const full = lines(20000);
  const path = join(scratch, "pi-bash.log");
  writeFileSync(path, full);
  const tail = `${lines(20000).split("\n").slice(-2000).join("\n")}\n\n[Showing lines 18001-20000 of 20000. Full output: ${path}]`;
  const shrunk = guard(undefined)?.(event("bash", tail, { details: { fullOutputPath: path } }))?.content[0].text ?? "";
  assert.ok(shrunk.startsWith("line 1\nline 2\n"));
  assert.ok(shrunk.endsWith("line 20000"));
  assert.match(shrunk, new RegExp(`\\d+ of ${Buffer.byteLength(full)} bytes omitted. Full output: ${path} `));
});

test("a truncated failure keeps its exit status, and a spilled codemode result keeps its header", () => {
  const path = join(scratch, "pi-failing.log");
  writeFileSync(path, lines(20000));
  const failing = `${lines(2000)}\n\n[Showing lines 18001-20000 of 20000. Full output: ${path}]\n\nCommand exited with code 3`;
  const shrunk = guard(undefined)?.(event("bash", failing, { details: { fullOutputPath: path } }))?.content[0].text ?? "";
  assert.ok(shrunk.endsWith("\n\nCommand exited with code 3"));
  assert.ok(shrunk.startsWith("line 1\n"));

  const header = "Script failed\nWall time 1.2 seconds\nOutput:\n";
  const spilled = `${header}Warning: truncated output\n\n…\n\n[Full output: ${path} (read with offset/limit)]`;
  const result = guard(undefined)?.(event("codemode", spilled, { details: { fullOutputPath: path } }))?.content[0].text ?? "";
  assert.ok(result.startsWith(header));
});

test("a huge full-output file is read only at its ends", () => {
  const path = join(scratch, "pi-huge.log");
  writeFileSync(path, lines(200000));
  const shrunk = guard("1k")?.(event("bash", "tail", { details: { fullOutputPath: path } }))?.content[0].text ?? "";
  assert.ok(shrunk.startsWith("line 1\n"));
  assert.ok(shrunk.includes("line 200000"));
  assert.match(shrunk, new RegExp(`of ${Buffer.byteLength(lines(200000))} bytes omitted`));
});

test("multibyte output keeps its head and tail in order, with no line skipped or repeated", () => {
  const cjk = Array.from({ length: 3000 }, (_, i) => `行${i + 1} ${"漢字".repeat(15)}`).join("\n");
  const path = join(scratch, "pi-cjk.log");
  writeFileSync(path, cjk);
  const shrunk = guard(undefined)?.(event("bash", "tail", { details: { fullOutputPath: path } }))?.content[0].text ?? "";
  assert.ok(!shrunk.includes("\uFFFD"));
  const [head, tail] = shrunk.split(/\n\[\.\.\. .*? \.\.\.\]\n/);
  const numbers = (part: string) => part.split("\n").map((line) => Number(/^行(\d+) /.exec(line)?.[1]));
  const first = numbers(head);
  const last = numbers(tail);
  assert.deepEqual(first, Array.from({ length: first.length }, (_, i) => i + 1));
  assert.deepEqual(last, Array.from({ length: last.length }, (_, i) => 3000 - last.length + 1 + i));
  assert.ok(first.length > 1 && last.length > 1);
  assert.match(shrunk, new RegExp(`of ${Buffer.byteLength(cjk)} bytes omitted`));
});

test("codemode results are guarded too", () => {
  assert.match(guard(undefined)?.(event("codemode", lines(5000)))?.content[0].text ?? "", /bytes omitted/);
});

test("small results, other tools, nested calls and non-text results are left alone", () => {
  const handler = guard(undefined);
  assert.equal(handler?.(event("bash", lines(100))), undefined);
  assert.equal(handler?.(event("read", lines(5000))), undefined);
  assert.equal(handler?.(event("bash", lines(5000), { parentToolCallId: "call-1" })), undefined);
  const withImage = { ...event("bash", lines(5000)), content: [{ type: "text", text: lines(5000) }, { type: "image" }] };
  assert.equal(handler?.(withImage), undefined);
});

test("a codemode script that raised max_output_tokens raises the limit", () => {
  const text = lines(5000);
  const code = '// @options: {"max_output_tokens": 20000}\nreturn 1';
  assert.equal(guard(undefined)?.(event("codemode", text, { input: { code } })), undefined);
  const small = '// @options: {"max_output_tokens": 100}\nreturn 1';
  assert.match(guard(undefined)?.(event("codemode", text, { input: { code: small } }))?.content[0].text ?? "", /omitted/);
});

test("a custom limit applies", () => {
  const text = lines(100);
  assert.equal(guard("1k")?.(event("bash", text)), undefined);
  assert.match(guard("500")?.(event("bash", text))?.content[0].text ?? "", /bytes omitted/);
});

test("shrinkOutput cuts at line boundaries and returns nothing when the text fits", () => {
  assert.equal(shrinkOutput("short", 10, "/x"), undefined);
  const shrunk = shrinkOutput(lines(1000), 200, "/x") ?? "";
  for (const line of shrunk.split("\n").filter((line) => line.startsWith("line "))) assert.match(line, /^line \d+$/);
});
