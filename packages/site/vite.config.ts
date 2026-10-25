import { readFileSync } from "node:fs";
import { join } from "node:path";
import { orbitSite } from "@gitlab-org/orbit-site/vite";
import { defineConfig, mergeConfig, type Plugin } from "vite";
import { engineeringMeta, siteRoot, sourcePaths, syncContent } from "./build/content";

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
      name: "Orbit engineering",
      title: "Engineering",
      banner: {
        kind: "external",
        message: "How the Orbit team builds and runs GitLab Orbit.",
        switchUrl: { dev: "http://localhost:5173/", prod: "https://gitlab-org.gitlab.io/orbit/portal/" },
      },
      search: "Search the engineering docs",
      port: 5190,
      siteRoot,
      meta: engineeringMeta,
    }),
    { plugins: [syncSources()] },
  ),
);
