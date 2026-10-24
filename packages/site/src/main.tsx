import "./styles/site.css";
import { renderSite, routerOptions } from "@gitlab-org/orbit-site";
import { createRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen";

export const router = createRouter({ routeTree, ...routerOptions });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

declare module "@gitlab-org/orbit-site" {
  interface PageMeta {
    sourceUrl: string;
    authors: string[];
    lastChanged?: string;
  }
}

renderSite(router);
