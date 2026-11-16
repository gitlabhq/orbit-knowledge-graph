import { pageHead, SiteShell, site } from "@gitlab-org/orbit-site";
import { createRootRoute, HeadContent, Outlet, useRouterState } from "@tanstack/react-router";
import { useEffect } from "react";
import { TabNav } from "../components/TabNav";

export const Route = createRootRoute({
  head: () => pageHead(site.name),
  component: RootLayout,
});

// The home page and the Orbit Next page start dark in both themes, so their top bar is dark too. It is clear
// over the hero, and slides away while the reader scrolls down.
const LANDINGS = new Set(["", "/next"]);

function RootLayout() {
  const home = useRouterState({ select: (state) => LANDINGS.has(state.location.pathname.replace(/\/+$/, "")) });

  // The landing top bar lies over the page, so it would cover the main area's scrollbar. Its right edge stops
  // at the scrollbar, measured here, so the scrollbar runs the full height of the window.
  useEffect(() => {
    const main = document.getElementById("main");
    if (!main) return;
    const measure = () => document.documentElement.style.setProperty("--main-scrollbar", `${main.offsetWidth - main.clientWidth}px`);
    const sized = new ResizeObserver(measure);
    sized.observe(main);
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
