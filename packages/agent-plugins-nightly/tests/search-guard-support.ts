import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Output shaped like the CLI's for `parse_widget`, which lib.rs defines and two functions call:
// `orbit grep --db DB parse_widget REPO` and `orbit context --db DB parse_widget`.
// crates/orbit-cli/tests/no_index.rs pins the same shapes against the real binary.
export const GREP_OUTPUT = `grep "parse_widget" @ 0d846e51: 3 lines in 1 files
Function parse_widget — lib.rs:1-1
  1|pub fn parse_widget() {}

lib.rs
1-» Function parse_widget
1:pub fn parse_widget() {}
2-» Function caller:2-4
3:    parse_widget();
5-» Function other
5:pub fn other() { parse_widget(); caller(); }
`;

export const CONTEXT_OUTPUT = `parse_widget  [Function]  lib.rs:1-1
1|pub fn parse_widget() {}

parse_widget  [Function]  lib.rs:1-1

Connections (2 indexed):
  <-- caller  [calls]  (lib.rs:2)
  <-- other  [calls]  (:5)
`;

export const DEFINITION = "Function parse_widget — lib.rs:1-1\n  1|pub fn parse_widget() {}";
export const CONNECTIONS =
  "parse_widget  [Function]  lib.rs:1-1\nConnections (2 indexed):\n  <-- caller  [calls]  (lib.rs:2)\n  <-- other  [calls]  (:5)";

const GIT = execFileSync("which", ["git"], { encoding: "utf8" }).trim();

export type Stub = {
  /** A git repository with one commit. */
  repo: string;
  /** Directory holding the stub launchers and `git`; the first PATH entry in a test. */
  bin: string;
  /** Every Orbit invocation, one argument list per line. */
  calls: () => string[];
  launcher: (name: string) => string;
};

/** A repository plus an `orbit` stub that answers `workspace list`, `grep` and `context` from fixtures. */
export function stubOrbit(options: { indexed?: boolean; grepStatus?: number } = {}): Stub {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "search-guard-")));
  const repo = join(root, "repo");
  const bin = join(root, "bin");
  const git = (...args: string[]) => execFileSync("git", ["-c", "user.name=t", "-c", "user.email=t@t", ...args], { cwd: repo, encoding: "utf8" });
  mkdirSync(repo);
  mkdirSync(bin);
  git("init", "-q");
  writeFileSync(join(repo, "lib.rs"), "pub fn parse_widget() {}\n");
  git("add", "-A");
  git("commit", "-qm", "init");
  const head = git("rev-parse", "HEAD").trim();
  symlinkSync(GIT, join(bin, "git"));
  const rows = options.indexed === false ? [] : [{ repo_path: repo, commit_sha: head, status: "indexed", last_indexed_at: "2026-01-01", database_path: join(root, "graph.duckdb") }];
  writeFileSync(join(root, "list.json"), JSON.stringify(rows));
  writeFileSync(join(root, "grep.txt"), GREP_OUTPUT);
  writeFileSync(join(root, "context.txt"), CONTEXT_OUTPUT);
  const log = join(root, "calls.log");
  const script = `#!/bin/sh
echo "$*" >> ${log}
case "$1 $2" in
  "workspace list") /bin/cat ${join(root, "list.json")} ;;
  grep\\ *) /bin/cat ${join(root, "grep.txt")}; exit ${options.grepStatus ?? 0} ;;
  context\\ *) /bin/cat ${join(root, "context.txt")} ;;
  *) exit 2 ;;
esac
`;
  writeFileSync(join(bin, "orbit"), script);
  chmodSync(join(bin, "orbit"), 0o755);
  return {
    repo,
    bin,
    calls: () => {
      try {
        return readFileSync(log, "utf8").trim().split("\n");
      } catch {
        return [];
      }
    },
    launcher: (name) => join(bin, name),
  };
}
