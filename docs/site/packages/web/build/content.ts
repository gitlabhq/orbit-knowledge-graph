import { execFileSync } from "node:child_process";
import { mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const siteRoot = join(import.meta.dirname, "../../..");
const architectureDir = join(siteRoot, "../architecture");
const contentDir = join(siteRoot, "content");
const repoBlob = `https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/${process.env.CI_COMMIT_SHA ?? "main"}/docs/`;

const sourceName = (route: string) => (route === "" ? "README.md" : `${route}.md`);

export const architectureSources = () =>
  readdirSync(architectureDir)
    .filter((name) => name.endsWith(".md"))
    .map((name) => join(architectureDir, name));

export function siteLinks(markdown: string): string {
  return markdown.replace(/\]\((?![a-z]+:|#|\/)([^)\s]+)\)/g, (_link, target: string) => {
    const [path, hash] = target.split("#");
    const fragment = hash ? `#${hash}` : "";
    if (path.startsWith("../")) return `](${repoBlob}${path.slice(3)}${fragment})`;
    return `](/${path.replace(/\.md$/, "").replace(/^README$/, "")}${fragment})`;
  });
}

export function syncContent() {
  rmSync(contentDir, { recursive: true, force: true });
  mkdirSync(contentDir, { recursive: true });
  for (const source of architectureSources()) {
    const name = source.slice(architectureDir.length + 1);
    writeFileSync(join(contentDir, name === "README.md" ? "index.md" : name), siteLinks(readFileSync(source, "utf8")));
  }
}

function lastChanged(route: string): string | undefined {
  const date = execFileSync("git", ["log", "-1", "--format=%cs", "--", join(architectureDir, sourceName(route))], { encoding: "utf8" }).trim();
  return date || undefined;
}

export function architectureMeta(route: string, frontmatter: Record<string, unknown>) {
  return {
    source: sourceName(route),
    authors: [frontmatter.authors].flat().filter((author): author is string => typeof author === "string"),
    lastChanged: lastChanged(route),
  };
}
