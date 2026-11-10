import { execFile } from "node:child_process";
import { promisify } from "node:util";

// The whole lookup (git, index listing, grep, context) shares one budget: a search result should not
// stall for long. An indexed lookup takes well under a second. The cap keeps the Claude Code hook
// inside its own timeout.
export const DEFAULT_TIMEOUT_MS = 1500;
export const MAX_TIMEOUT_MS = 4000;
const MAX_DEFINITION_CHARS = 1800;
const MAX_CONNECTIONS_CHARS = 1200;
const MAX_OUTPUT_BYTES = 64 * 1024 * 1024;

const execFileAsync = promisify(execFile);
const FAILED = Symbol("failed");
const OVERSIZED = Symbol("oversized");

export function isGuardDisabled(value) {
  return /^(0|off|false|no)$/i.test(value ?? "");
}

export function lookupTimeoutMs(value) {
  const requested = Number(value);
  return requested > 0 ? Math.min(requested, MAX_TIMEOUT_MS) : DEFAULT_TIMEOUT_MS;
}

// Orbit stores native paths (`C:\repo`); git prints `C:/repo`.
function samePath(left, right) {
  const normal = (path) => (process.platform === "win32" ? path.replace(/\\/g, "/").toLowerCase() : path);
  return normal(String(left)) === normal(right);
}

function cut(block, maxChars) {
  return block.length > maxChars ? `${block.slice(0, maxChars)}\n[truncated]` : block;
}

/** The first definition block of `orbit grep` output: the `Kind name — path:range` header up to the first blank line. */
export function extractDefinition(output) {
  const lines = output.split("\n");
  const first = lines.findIndex((line) => /^[A-Z]\w* \S+ — \S+:\d+/.test(line));
  if (first === -1) return undefined;
  const end = lines.findIndex((line, index) => index > first && line.trim() === "");
  return cut(lines.slice(first, end === -1 ? lines.length : end).join("\n"), MAX_DEFINITION_CHARS);
}

function definitionLocation(definition) {
  const match = /^[A-Z]\w* (\S+) — (\S+:\d+(?:-\d+)?)/m.exec(definition);
  return match ? { name: match[1], location: match[2] } : undefined;
}

/**
 * The `orbit context` block of the definition at `location`: its `name  [Kind]  path:range` header and
 * the `Connections (N indexed):` lines (callers and callees). Context orders several definitions
 * differently from grep, so the block is chosen by the header's location.
 */
export function extractConnections(output, location) {
  const lines = output.split("\n");
  for (let index = 0; index < lines.length; index++) {
    if (!/^Connections \(\d+ indexed\):/.test(lines[index])) continue;
    let header = index - 1;
    while (header >= 0 && lines[header].trim() === "") header--;
    if (header < 0 || !lines[header].endsWith(`  ${location}`)) continue;
    const end = lines.findIndex((line, at) => at > index && line.trim() === "");
    return cut([lines[header], ...lines.slice(index, end === -1 ? lines.length : end)].join("\n"), MAX_CONNECTIONS_CHARS);
  }
  return undefined;
}

/** Runs commands against one overall deadline; each call gets the remaining budget. */
function budgeted(timeoutMs, signal) {
  const deadline = Date.now() + timeoutMs;
  return async (command, args, cwd) => {
    const remaining = deadline - Date.now();
    if (remaining <= 0) return FAILED;
    try {
      const running = execFileAsync(command, args, {
        cwd,
        encoding: "utf8",
        timeout: remaining,
        killSignal: "SIGKILL",
        signal,
        maxBuffer: MAX_OUTPUT_BYTES,
        env: { ...process.env, NO_COLOR: "1" },
      });
      // A launcher that wants to prompt (for a download, say) must see end of input, not wait.
      running.child.stdin?.end();
      const { stdout } = await running;
      return { stdout, status: 0 };
    } catch (error) {
      if (error.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") return OVERSIZED;
      if (typeof error.code === "number" && !error.killed && !error.signal) {
        return { stdout: error.stdout ?? "", status: error.code };
      }
      return FAILED;
    }
  };
}

/**
 * The existing local index of the repository containing `cwd`, at its current commit: the launcher
 * that answered, the database path and the repository top-level. Undefined when there is none, FAILED
 * when git or every launcher failed or ran out of budget.
 * Listing reads the manifest and never indexes. The first launcher of `launchers` that lists
 * successfully is used for the rest of the lookup.
 */
async function existingIndex(exec, launchers, cwd) {
  const git = await exec("git", ["rev-parse", "--show-toplevel", "HEAD"], cwd);
  if (git === FAILED) return FAILED;
  if (git === OVERSIZED || git.status !== 0) return undefined;
  const [top, head] = git.stdout.trim().split("\n");
  for (const launcher of launchers) {
    const [command, ...prefix] = launcher;
    const list = await exec(command, [...prefix, "workspace", "list", "--format", "json"], cwd);
    if (list === OVERSIZED) return undefined;
    if (list === FAILED || list.status !== 0) continue;
    try {
      const rows = JSON.parse(list.stdout).filter(
        (row) => samePath(row.repo_path, top) && row.commit_sha === head && row.status === "indexed",
      );
      rows.sort((a, b) => String(b.last_indexed_at).localeCompare(String(a.last_indexed_at)));
      return typeof rows[0]?.database_path === "string" ? { launcher, database: rows[0].database_path, top } : undefined;
    } catch {
      return undefined;
    }
  }
  return FAILED;
}

/**
 * Orbit's definition of `term`, with its callers and callees when `orbit context` has them:
 * `{ text }` to append to the search result, `{}` when there is nothing to add, `{ failed: true }`
 * when a command broke or ran out of budget (callers stop looking up for the session).
 * `launchers` are command prefixes to try in order, such as `[["glab", "orbit"], ["orbit"]]`.
 * Every Orbit call passes `--no-index`, so the lookup reads an index that is already there and
 * never builds or refreshes one.
 */
export async function lookupDefinition(term, cwd, { launchers, timeoutMs = DEFAULT_TIMEOUT_MS, signal }) {
  const exec = budgeted(timeoutMs, signal);
  const aborted = () => (signal?.aborted ? {} : { failed: true });
  const index = await existingIndex(exec, launchers, cwd);
  if (index === FAILED) return aborted();
  if (!index) return {};
  const { launcher, database, top } = index;
  const [command, ...prefix] = launcher;
  // Without a path, `orbit grep` also searches the GitLab group behind `origin` over the network.
  const grep = await exec(command, [...prefix, "grep", "--no-index", "--db", database, term, top], cwd);
  // `orbit grep` exits 1 when nothing matches, like rg.
  if (grep === OVERSIZED) return {};
  if (grep === FAILED || grep.status > 1) return aborted();
  const definition = grep.status === 0 ? extractDefinition(grep.stdout) : undefined;
  if (!definition) return {};
  const found = definitionLocation(definition);
  // Grep matching ignores case and `_`; context needs the exact name from the definition header.
  const context = found && (await exec(command, [...prefix, "context", "--no-index", "--db", database, found.name], cwd));
  const connections = context && context !== FAILED && context !== OVERSIZED && context.status === 0 ? extractConnections(context.stdout, found.location) : undefined;
  const name = launcher.join(" ");
  const lead = `Orbit definition of \`${term}\` (from \`${name} grep\` and \`${name} context\`; use it for callers and callees instead of more rg/grep):`;
  return { text: `${lead}\n${connections ? `${definition}\n\n${connections}` : definition}` };
}
