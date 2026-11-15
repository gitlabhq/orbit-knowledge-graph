import { orbitSite } from "@gitlab-org/orbit-site/vite";
import { defineConfig, mergeConfig, type Plugin } from "vite";
import { siteMeta, siteRoot, sourcePaths, syncContent, syncTrackers } from "./build/content";
import { HOME, NEXT_PAGE } from "./src/sections";

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

export default defineConfig(async () => {
  syncContent();
  await syncTrackers();
  return mergeConfig(
    orbitSite({
      name: HOME.title,
      title: "",
      search: "Search GitLab Orbit",
      routes: { "": HOME, next: NEXT_PAGE },
      port: 5190,
      siteRoot,
      markdown: { project: "gitlab-org/orbit/knowledge-graph", tokenEnv: "ORBIT_MARKDOWN_TOKEN" },
      meta: siteMeta,
      avatars: (route) => !route.startsWith("content/blog/"),
    }),
    { plugins: [syncSources()] },
  );
});
