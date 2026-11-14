import { pageHead, SiteShell, site } from "@gitlab-org/orbit-site";
import { createRootRoute, HeadContent, Outlet } from "@tanstack/react-router";
import { TabNav } from "../components/TabNav";
import { Version } from "../components/Version";

export const Route = createRootRoute({
  head: () => pageHead(site.name),
  component: RootLayout,
});

function RootLayout() {
  return (
    <>
      <HeadContent />
      <SiteShell nav={<TabNav className="hidden md:flex" />} mobileNav={<TabNav className="flex overflow-x-auto px-2 py-1 md:hidden" />} end={<Version />}>
        <Outlet />
      </SiteShell>
    </>
  );
}
