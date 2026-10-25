import { cn, findPage, Icon, type Page, pageLink, pages } from "@gitlab-org/orbit-site";
import { Link, useLocation } from "@tanstack/react-router";
import { ENGINEERING } from "../sections";

const OPEN_LIMIT = 12;
const ITEM = "block rounded-control px-3 py-1.5 text-sm no-underline transition-colors";
const ACTIVE = { className: "bg-hover font-semibold text-default" };
const INACTIVE = { className: "text-subtle hover:bg-hover hover:text-default" };

const parentOf = (path: string) => path.slice(0, path.lastIndexOf("/"));

const byOrder = (a: Page, b: Page) => (a.meta.order ?? Infinity) - (b.meta.order ?? Infinity) || a.source.localeCompare(b.source);

const childrenOf = (path: string) => pages.filter((page) => parentOf(page.path) === path).sort(byOrder);

const within = (path: string, current: string) => current === path || current.startsWith(`${path}/`);

function SidebarLink({ page, label }: { page: Page; label?: string }) {
  return (
    <Link {...pageLink(page.path)} title={page.meta.title} className={cn(ITEM, "truncate")} activeOptions={{ exact: true }} activeProps={ACTIVE} inactiveProps={INACTIVE}>
      {label ?? page.meta.title}
    </Link>
  );
}

function Tree({ path, current }: { path: string; current: string }) {
  return (
    <ul className="m-0 list-none p-0">
      {childrenOf(path).map((page) => {
        const nested = childrenOf(page.path);
        return (
          <li key={page.path}>
            {nested.length === 0 ? (
              <SidebarLink page={page} />
            ) : (
              <details open={within(page.path, current)} className="group/sub">
                <summary className={cn(ITEM, "flex cursor-pointer list-none items-center gap-1 text-subtle hover:bg-hover hover:text-default")}>
                  <Icon name="chevron-right" size={12} className="shrink-0 transition-transform group-open/sub:rotate-90" />
                  {page.meta.title}
                </summary>
                <div className="ml-3 border-l border-border pl-2">
                  <SidebarLink page={page} label="Overview" />
                  <Tree path={page.path} current={current} />
                </div>
              </details>
            )}
          </li>
        );
      })}
    </ul>
  );
}

export function Sidebar() {
  const current = useLocation().pathname.replace(/^\/+|\/+$/g, "");
  const overview = findPage("engineering");
  return (
    <nav aria-label="Engineering" className="max-h-48 shrink-0 overflow-y-auto border-b border-border px-3 py-3 md:sticky md:top-[var(--header-h)] md:h-[calc(100vh-var(--header-h))] md:max-h-none md:w-64 md:border-r md:border-b-0 md:py-6">
      {overview && <SidebarLink page={overview} label="Overview" />}
      {ENGINEERING.map(({ route, label }) => {
        const index = findPage(route);
        const open = within(route, current) || childrenOf(route).length <= OPEN_LIMIT;
        return (
          <details key={route} open={open} className="group mt-4">
            <summary className="flex cursor-pointer list-none items-center gap-1 px-3 py-1 text-xs font-semibold tracking-wide text-subtle uppercase hover:text-default">
              <Icon name="chevron-right" size={12} className="transition-transform group-open:rotate-90" />
              {label}
              <span className="ml-auto font-normal normal-case">{childrenOf(route).length || ""}</span>
            </summary>
            {index && <SidebarLink page={index} label="Overview" />}
            <Tree path={route} current={current} />
          </details>
        );
      })}
    </nav>
  );
}
