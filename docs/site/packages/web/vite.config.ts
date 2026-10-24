import { orbitSite } from "@gitlab-org/orbit-site/vite";
import { defineConfig } from "vite";
import { architectureMeta, architectureSources, syncContent } from "./build/content";

syncContent();

export default defineConfig(
  orbitSite({
    name: "Orbit architecture",
    title: "Architecture",
    banner: {
      kind: "external",
      message: "The design contracts of GitLab Orbit.",
      switchUrl: { dev: "http://localhost:5173/", prod: "https://gitlab-org.gitlab.io/orbit/portal/" },
    },
    search: "Search the architecture",
    port: 5190,
    meta: architectureMeta,
    watch: architectureSources(),
  }),
);
