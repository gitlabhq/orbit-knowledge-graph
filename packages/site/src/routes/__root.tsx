import { pageHead, site, TopBar } from "@gitlab-org/orbit-site";
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
      <div className="flex min-h-full flex-col">
        <TopBar className="mx-auto max-w-[90rem] px-4" nav={<TabNav className="hidden md:flex" />} end={<Version />}>
          <TabNav className="flex overflow-x-auto border-t border-border px-2 py-1 md:hidden" />
        </TopBar>
        <main id="main" className="mx-auto w-full max-w-[90rem] flex-1">
          <Outlet />
        </main>
      </div>
    </>
  );
}
