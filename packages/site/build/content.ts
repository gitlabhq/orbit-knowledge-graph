import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, posix } from "node:path";
import { parseFrontmatter } from "@gitlab-org/orbit-markdown/author";
import { parse as parseYaml } from "yaml";
import { itemFields, postFields } from "../src/roadmap/schema";
import { PAGES_DIR, SECTIONS } from "../src/sections";
import { fetchAvatars, type PageAuthor } from "./avatars";
import { fetchTrackers, type Tracker } from "./trackers";

export const siteRoot = join(import.meta.dirname, "..");
const repoRoot = join(siteRoot, "../..");
export const contentDir = join(siteRoot, "content");
const dataDir = join(siteRoot, "data");
const repoUrl = `https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/`;
const revision = process.env.CI_COMMIT_SHA ?? "main";
export const repoBlob = `${repoUrl}blob/${revision}/`;
const repoRaw = `${repoUrl}raw/${revision}/`;

const ENGINEERING_HOME = `${PAGES_DIR}/engineering/README.md`;
const dirOf = (route: string) => `${PAGES_DIR}/${route}`;
const IMAGE = /\.(png|jpe?g|gif|svg|webp)$/i;
const CODE = /(`{3,}[\s\S]*?`{3,}|~{3,}[\s\S]*?~{3,}|`[^`\n]*`)/;
const INLINE_LINK = /\]\((?![a-z]+:|#|\/)([^)\s]+)((?:\s+"[^"]*")?)\)/g;
const REFERENCE_LINK = /^(\[[^\]]+\]:[ \t]*)(?![a-z]+:|#|\/)(\S+)/gm;

const sourceOfRoute = new Map<string, string>();
const changedOn = new Map<string, string>();

const isReadme = (file: string) => posix.basename(file) === "README.md";

const routeOf = (section: string, name: string) => posix.join(section, isReadme(name) ? posix.dirname(name) : name.replace(/\.md$/, ""));

const contentName = (file: string, route: string) => (isReadme(file) ? posix.join(route, "index.md") : `${route}.md`);

export const sourcePaths = [ENGINEERING_HOME, ...SECTIONS.map(({ route }) => dirOf(route))].map((path) => join(repoRoot, path));

export function sourceFiles(): Map<string, string> {
  return new Map([
    [ENGINEERING_HOME, "engineering"],
    ...SECTIONS.flatMap(({ route }) =>
      readdirSync(join(repoRoot, dirOf(route)), { recursive: true, encoding: "utf8" })
        .filter((name) => name.endsWith(".md"))
        .map((name): [string, string] => [posix.join(dirOf(route), name), routeOf(route, name)]),
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

let trackers: Record<string, Tracker> = {};
let authors: Record<string, PageAuthor> = {};

function syncData() {
  rmSync(dataDir, { recursive: true, force: true });
  for (const { route } of SECTIONS)
    for (const name of readdirSync(join(repoRoot, dirOf(route))).filter((file) => file.endsWith(".yaml"))) {
      mkdirSync(join(dataDir, route), { recursive: true });
      copyFileSync(join(repoRoot, dirOf(route), name), join(dataDir, route, name));
    }
  mkdirSync(join(dataDir, "roadmap"), { recursive: true });
  writeFileSync(join(dataDir, "roadmap", "trackers.yaml"), JSON.stringify(trackers));
}

const frontmatterOf = (file: string) => parseFrontmatter(readFileSync(join(repoRoot, file), "utf8")).frontmatter;

const authorNames = (frontmatter: Record<string, unknown>) =>
  [frontmatter.authors]
    .flat()
    .filter((author): author is string => typeof author === "string")
    .map((author) => author.replace(/^@/, ""));

const trackingUrls = () =>
  [...sourceFiles()]
    .filter(([, route]) => route.startsWith("roadmap/"))
    .map(([file]) => frontmatterOf(file).tracking)
    .filter((url): url is string => typeof url === "string");

const pageAuthors = () =>
  [...sourceFiles()]
    .filter(([, route]) => !route.startsWith("content/blog/"))
    .flatMap(([file]) => authorNames(frontmatterOf(file)));

export async function syncGitLab() {
  const token = process.env.ORBIT_MARKDOWN_TOKEN;
  [trackers, authors] = await Promise.all([fetchTrackers(trackingUrls(), token), fetchAvatars(pageAuthors(), token)]);
  syncData();
}

const sectionData = (route: string, name: string) => parseYaml(readFileSync(join(dataDir, route, name), "utf8"));

let roadmapSchema = { themes: [] as string[], authors: [] as string[] };

export function syncContent() {
  const routes = sourceFiles();
  syncData();
  roadmapSchema = { themes: sectionData("roadmap", "roadmap.yaml").themes.map((theme: { id: string }) => theme.id), authors: Object.keys(sectionData("content", "authors.yaml")) };
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

export function siteMeta(route: string, frontmatter: Record<string, unknown>, body: string) {
  const source = sourceOfRoute.get(route);
  if (!source) throw new Error(`no source file for the route /${route}`);
  return {
    sourceUrl: `${repoBlob}${source}`,
    authors: authorNames(frontmatter).map((username) => authors[username] ?? { username }),
    lastChanged: changedOn.get(source),
    order: typeof frontmatter.order === "number" ? frontmatter.order : undefined,
    words: body.split(/\s+/).filter(Boolean).length,
    ...roadmapFields(route, frontmatter),
  };
}

function roadmapFields(route: string, frontmatter: Record<string, unknown>) {
  if (route.startsWith("content/blog/")) return { post: postFields(frontmatter, roadmapSchema.authors) };
  if (route.startsWith("roadmap/")) return { item: itemFields(frontmatter, roadmapSchema.themes) };
  return {};
}
