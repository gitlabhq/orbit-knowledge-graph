import { pageHead, SiteShell, site } from "@gitlab-org/orbit-site";
import { createRootRoute, HeadContent, Outlet, useRouterState } from "@tanstack/react-router";
import { useEffect } from "react";
import { TabNav } from "../components/TabNav";

export const Route = createRootRoute({
  head: () => pageHead(site.name),
  component: RootLayout,
});

const LANDINGS = new Set(["", "/next"]);

function RootLayout() {
  const home = useRouterState({ select: (state) => LANDINGS.has(state.location.pathname.replace(/\/+$/, "")) });

  useEffect(() => {
    const panel = document.querySelector<HTMLElement>(".page-panel");
    if (!panel) return;
    const measure = () => document.documentElement.style.setProperty("--main-scrollbar", `${panel.offsetWidth - panel.clientWidth}px`);
    const sized = new ResizeObserver(measure);
    sized.observe(panel);
    measure();
    return () => sized.disconnect();
  }, [home]);
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
