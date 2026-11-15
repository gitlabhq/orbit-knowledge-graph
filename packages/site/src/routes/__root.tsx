import { pageHead, SiteShell, site } from "@gitlab-org/orbit-site";
import { createRootRoute, HeadContent, Outlet, useRouterState } from "@tanstack/react-router";
import { TabNav } from "../components/TabNav";

export const Route = createRootRoute({
  head: () => pageHead(site.name),
  component: RootLayout,
});

// The home page starts dark in both themes, so its top bar is dark too. It is clear over the hero, and
// slides away while the reader scrolls down.
function RootLayout() {
  const home = useRouterState({ select: (state) => state.location.pathname.replace(/\/+$/, "") === "" });
  return (
    <>
      <HeadContent />
      <SiteShell
        headerClassName={home ? "gl-dark gl-dark-scope" : undefined}
        autoHideHeader={home}
        nav={<TabNav className="hidden md:flex" />}
        mobileNav={<TabNav className="flex overflow-x-auto px-2 py-1 md:hidden" />}
      >
        <Outlet />
      </SiteShell>
    </>
  );
}
