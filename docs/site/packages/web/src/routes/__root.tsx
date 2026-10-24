import { pageHead, site, TopBar } from "@gitlab-org/orbit-site";
import { createRootRoute, HeadContent, Outlet } from "@tanstack/react-router";
import { Sidebar } from "../components/Sidebar";

export const Route = createRootRoute({
  head: () => pageHead(site.name),
  component: RootLayout,
});

function RootLayout() {
  return (
    <>
      <HeadContent />
      <div className="flex min-h-full flex-col">
        <TopBar className="mx-auto max-w-[90rem] px-4" />
        <div className="mx-auto flex w-full max-w-[90rem] flex-1">
          <Sidebar />
          <main id="main" className="min-w-0 flex-1 px-6 py-8 lg:px-10">
            <Outlet />
          </main>
        </div>
      </div>
    </>
  );
}
