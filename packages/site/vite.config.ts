import { readFileSync } from "node:fs";
import { join } from "node:path";
import { orbitSite } from "@gitlab-org/orbit-site/vite";
import { defineConfig, mergeConfig, type Plugin } from "vite";
import { siteMeta, siteRoot, sourcePaths, syncContent } from "./build/content";
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

process.env.VITE_ORBIT_VERSION = JSON.parse(readFileSync(join(siteRoot, "../../package.json"), "utf8")).version;
process.env.VITE_ORBIT_RELEASE = process.env.CI_COMMIT_TAG ?? "";

syncContent();

export default defineConfig(
  mergeConfig(
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
      meta: siteMeta,
    }),
    { plugins: [syncSources()] },
  ),
);
