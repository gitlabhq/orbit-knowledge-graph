import { expect, test } from "bun:test";

import { OrbitPlugin } from "./opencode_plugin.js";

async function toastsFor(commands, client) {
  const toasts = [];
  const hooks = await OrbitPlugin({
    client: client ?? { tui: { showToast: async ({ body }) => toasts.push(body.message) } },
  });
  for (const command of commands) {
    await hooks["tool.execute.before"]({ tool: "bash" }, { args: { command } });
  }
  await new Promise((resolve) => setTimeout(resolve, 0));
  return toasts;
}

test("each orbit subcommand shows its label", async () => {
  expect(
    await toastsFor([
      "glab orbit grep 'auth|login'",
      "glab orbit context src/main.rs:10-40",
      'glab orbit query "MATCH (n:Project) RETURN n LIMIT 1"',
      'glab orbit query "CALL db.schema()"',
      "glab orbit schema MergeRequest",
      "glab orbit ontology",
      "glab orbit status --full-path gitlab-org/gitlab",
      "glab orbit workspace index .",
      "glab orbit workspace sql 'SELECT 1'",
      "glab orbit workspace schema",
      "glab orbit skills",
      "glab orbit config set telemetry.enabled false",
    ]),
  ).toEqual([
    "◎ Searching code",
    "◎ Reading code",
    "◎ Exploring Orbit",
    "◎ Reading Orbit schema",
    "◎ Reading Orbit schema",
    "◎ Reading Orbit schema",
    "◎ Checking Orbit",
    "◎ Indexing repo",
    "◎ Querying local graph",
    "◎ Reading local schema",
    "◎ Reading Orbit guide",
    "◎ Changing Orbit settings",
  ]);
});

test("the standalone orbit binary counts too", async () => {
  expect(await toastsFor(["orbit grep foo", "~/.local/bin/orbit context a.rs", "ORBIT_X=1 orbit status"])).toEqual([
    "◎ Searching code",
    "◎ Reading code",
    "◎ Checking Orbit",
  ]);
});

test("flags before the subcommand are skipped", async () => {
  expect(await toastsFor(["glab orbit --yes grep foo", "orbit -y workspace --db x.duckdb sql 'SELECT 1'"])).toEqual([
    "◎ Searching code",
    "◎ Querying local graph",
  ]);
});

test("an orbit call after another command counts", async () => {
  expect(await toastsFor(["cd repo && glab orbit grep foo", "git pull; orbit context a.rs | head -20"])).toEqual([
    "◎ Searching code",
    "◎ Reading code",
  ]);
});

test("orbit named inside an argument is not a call", async () => {
  expect(
    await toastsFor([
      'rg "glab orbit grep" docs/',
      "echo 'run glab orbit setup'",
      'git commit -m "fix: glab orbit query output"',
      'rg "a|glab orbit grep" docs/',
      "cat orbit.md",
      "git status",
    ]),
  ).toEqual([]);
});

test("unknown, removed, and internal subcommands show no toast", async () => {
  expect(
    await toastsFor([
      "glab orbit",
      "glab orbit --help",
      "glab orbit gerp foo",
      "glab orbit dsl",
      "glab orbit repo-map overview",
      "glab orbit hook-guard",
      "glab orbit mcp serve",
      "glab orbit workspace",
      "glab orbit constructor",
    ]),
  ).toEqual([]);
});

test("other tools show no toast", async () => {
  const toasts = [];
  const hooks = await OrbitPlugin({ client: { tui: { showToast: async ({ body }) => toasts.push(body.message) } } });
  await hooks["tool.execute.before"]({ tool: "read" }, { args: { command: "glab orbit grep x" } });
  expect(toasts).toEqual([]);
});

test("the hook returns before the toast does", async () => {
  let release;
  const shown = new Promise((resolve) => (release = resolve));
  const hooks = await OrbitPlugin({ client: { tui: { showToast: () => shown } } });
  const before = hooks["tool.execute.before"]({ tool: "bash" }, { args: { command: "glab orbit grep x" } });
  expect(await Promise.race([before.then(() => "returned"), new Promise((r) => setTimeout(() => r("waited"), 50))])).toBe(
    "returned",
  );
  release();
});

test("a failing or missing client never throws", async () => {
  const throwing = { tui: { showToast: () => { throw new Error("no tui"); } } };
  const rejecting = { tui: { showToast: async () => { throw new Error("no tui"); } } };
  expect(await toastsFor(["glab orbit grep x"], throwing)).toEqual([]);
  expect(await toastsFor(["glab orbit grep x"], rejecting)).toEqual([]);
  const bare = await OrbitPlugin();
  await bare["tool.execute.before"]({ tool: "bash" }, { args: {} });
});

test("the module exports only the plugin", async () => {
  expect(Object.keys(await import("./opencode_plugin.js"))).toEqual(["OrbitPlugin"]);
});
