import assert from "node:assert/strict";
import { test } from "bun:test";
import { registerSearchGuard } from "../search-guard/pi.mjs";

type Handler = (event: unknown, ctx: unknown) => Promise<{ content: { text: string }[] } | undefined>;
type Lookup = (term: string, cwd: string, options: { launchers: string[][]; timeoutMs: number }) => Promise<{ text?: string; failed?: boolean }>;

function guard(lookup: Lookup) {
  let handler: Handler | undefined;
  registerSearchGuard({ on: (_name: string, fn: Handler) => (handler = fn) } as never, lookup as never);
  assert.ok(handler);
  return handler;
}

const call = (command: string) => ({ toolName: "bash", input: { command }, content: [{ type: "text", text: "hits" }] });
const ctx = { cwd: process.cwd() };

test("a single-identifier search gets the definition appended, with the budget and launcher", async () => {
  const seen: Parameters<Lookup>[] = [];
  const handler = guard(async (...args) => {
    seen.push(args);
    return { text: "DEF" };
  });
  const result = await handler(call("rg parse_widget"), ctx);
  assert.deepEqual(result?.content.map((part) => part.text), ["hits", "\n\nDEF"]);
  assert.equal(seen[0][0], "parse_widget");
  assert.equal(seen[0][2].timeoutMs, 1500);
  assert.deepEqual(seen[0][2].launchers, [["{{orbit}}"]]);
});

test("plain words, repeated terms and Orbit lookups are skipped", async () => {
  let lookups = 0;
  const handler = guard(async () => {
    lookups++;
    return { text: "DEF" };
  });
  assert.equal(await handler(call("rg widget"), ctx), undefined);
  assert.ok(await handler(call("rg parse_widget"), ctx));
  assert.equal(await handler(call("rg parse_widget"), ctx), undefined);
  assert.equal(await handler(call("glab orbit grep other_widget"), ctx), undefined);
  assert.equal(await handler(call("rg other_widget"), ctx), undefined);
  assert.equal(lookups, 1);
});

test("a failed lookup stops lookups for the session", async () => {
  let lookups = 0;
  const handler = guard(async () => {
    lookups++;
    return { failed: true };
  });
  assert.equal(await handler(call("rg parse_widget"), ctx), undefined);
  assert.equal(await handler(call("rg other_widget"), ctx), undefined);
  assert.equal(lookups, 1);
});

test("ORBIT_SEARCH_GUARD=off registers nothing", () => {
  const saved = process.env.ORBIT_SEARCH_GUARD;
  process.env.ORBIT_SEARCH_GUARD = "off";
  let registered = false;
  registerSearchGuard({ on: () => (registered = true) } as never);
  if (saved === undefined) delete process.env.ORBIT_SEARCH_GUARD;
  else process.env.ORBIT_SEARCH_GUARD = saved;
  assert.equal(registered, false);
});
