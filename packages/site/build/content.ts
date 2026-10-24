import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, posix } from "node:path";
import { SECTIONS } from "../src/sections";

export const siteRoot = join(import.meta.dirname, "..");
const repoRoot = join(siteRoot, "../..");
export const contentDir = join(siteRoot, "content");
const repoUrl = `https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/`;
const revision = process.env.CI_COMMIT_SHA ?? "main";
export const repoBlob = `${repoUrl}blob/${revision}/`;
const repoRaw = `${repoUrl}raw/${revision}/`;

const HOME = "docs/README.md";
const IMAGE = /\.(png|jpe?g|gif|svg|webp)$/i;
const CODE = /(`{3,}[\s\S]*?`{3,}|~{3,}[\s\S]*?~{3,}|`[^`\n]*`)/;
const INLINE_LINK = /\]\((?![a-z]+:|#|\/)([^)\s]+)((?:\s+"[^"]*")?)\)/g;
const REFERENCE_LINK = /^(\[[^\]]+\]:[ \t]*)(?![a-z]+:|#|\/)(\S+)/gm;

const sourceOfRoute = new Map<string, string>();
const changedOn = new Map<string, string>();

const isReadme = (file: string) => posix.basename(file) === "README.md";

const routeOf = (section: string, name: string) => posix.join(section, isReadme(name) ? posix.dirname(name) : name.replace(/\.md$/, ""));

const contentName = (file: string, route: string) => (isReadme(file) ? posix.join(route, "index.md") : `${route}.md`);

export const sourcePaths = [HOME, ...SECTIONS.map(({ dir }) => dir)].map((path) => join(repoRoot, path));

export function sourceFiles(): Map<string, string> {
  return new Map([
    [HOME, ""],
    ...SECTIONS.flatMap(({ route, dir }) =>
      readdirSync(join(repoRoot, dir), { recursive: true, encoding: "utf8" })
        .filter((name) => name.endsWith(".md"))
        .map((name): [string, string] => [posix.join(dir, name), routeOf(route, name)]),
    ),
  ]);
}

function siteLink(target: string, file: string, routes: Map<string, string>): string {
  const [path = "", hash] = target.split("#");
  const linked = posix.join(posix.dirname(file), path);
  const absolute = join(repoRoot, linked);
  if (!existsSync(absolute)) throw new Error(`${file}: the link target ${target} does not exist`);
  const page = statSync(absolute).isDirectory() ? posix.join(linked, "README.md") : linked;
  const route = routes.get(page);
  const fragment = hash ? `#${hash}` : "";
  if (route !== undefined) return `/${route}${fragment}`;
  return `${IMAGE.test(linked) ? repoRaw : repoBlob}${linked}${fragment}`;
}

export function siteLinks(markdown: string, file: string, routes: Map<string, string>): string {
  return markdown
    .split(CODE)
    .map((part, index) =>
      index % 2
        ? part
        : part
            .replace(INLINE_LINK, (_link, target: string, title: string) => `](${siteLink(target, file, routes)}${title})`)
            .replace(REFERENCE_LINK, (_link, label: string, target: string) => `${label}${siteLink(target, file, routes)}`),
    )
    .join("");
}

function commitDates(files: string[]) {
  changedOn.clear();
  const log = execFileSync("git", ["log", "--format=%x00%cs", "--name-only", "--", ...files], { cwd: repoRoot, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  for (const entry of log.split("\0").slice(1)) {
    const [date = "", ...changed] = entry.trim().split("\n");
    for (const file of changed) if (!changedOn.has(file)) changedOn.set(file, date);
  }
}

export function syncContent() {
  const routes = sourceFiles();
  rmSync(contentDir, { recursive: true, force: true });
  sourceOfRoute.clear();
  for (const [file, route] of routes) {
    const target = join(contentDir, contentName(file, route));
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, siteLinks(readFileSync(join(repoRoot, file), "utf8"), file, routes));
    sourceOfRoute.set(route, file);
  }
  commitDates([...routes.keys()]);
}

export function engineeringMeta(route: string, frontmatter: Record<string, unknown>) {
  const source = sourceOfRoute.get(route);
  if (!source) throw new Error(`no source file for the route /${route}`);
  return {
    sourceUrl: `${repoBlob}${source}`,
    authors: [frontmatter.authors]
      .flat()
      .filter((author): author is string => typeof author === "string")
      .map((author) => author.replace(/^@/, "")),
    lastChanged: changedOn.get(source),
  };
}
