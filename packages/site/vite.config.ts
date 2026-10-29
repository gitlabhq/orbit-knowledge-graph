import { readFileSync } from "node:fs";
import { join } from "node:path";
import { orbitSite } from "@gitlab-org/orbit-site/vite";
import { defineConfig, mergeConfig, type Plugin } from "vite";
import { siteMeta, siteRoot, sourcePaths, syncContent, syncTrackers } from "./build/content";
import { HOME } from "./src/sections";

function syncSources(): Plugin {
  return {
    name: "sync-engineering-content",
    configureServer(server) {
      server.watcher.add(sourcePaths);
      server.watcher.on("all", (_event, path) => {
        if (path.endsWith(".md") && sourcePaths.some((source) => path.startsWith(source))) syncContent();
      });
    },
  };
}

const version: string = JSON.parse(readFileSync(join(siteRoot, "../../package.json"), "utf8")).version;
const onDefaultBranch = process.env.CI_COMMIT_BRANCH !== undefined && process.env.CI_COMMIT_BRANCH === process.env.CI_DEFAULT_BRANCH;
process.env.VITE_ORBIT_VERSION = version;
process.env.VITE_ORBIT_RELEASE = process.env.CI_COMMIT_TAG ?? (onDefaultBranch ? `v${version}` : "");


export default defineConfig(async () => {
  syncContent();
  await syncTrackers();
  return mergeConfig(
    orbitSite({
      name: HOME.title,
      title: "",
      banner: {
        kind: "external",
        message: "Public site. Safe to share with customers and the community.",
        switchUrl: { dev: "http://localhost:5173/", prod: "https://gitlab-org.gitlab.io/orbit/portal/" },
      },
      search: "Search GitLab Orbit",
      routes: { "": HOME },
      port: 5190,
      siteRoot,
      markdown: { project: "gitlab-org/orbit/knowledge-graph", tokenEnv: "ORBIT_MARKDOWN_TOKEN" },
      meta: siteMeta,
      avatars: (route) => !route.startsWith("content/blog/"),
    }),
    { plugins: [syncSources()] },
  );
});
