import { cn, Icon, pageLink, SiteLink, useEscape } from "@gitlab-org/orbit-site";
import { Link, useLocation } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { type Tab, TABS, USER_DOCS } from "../sections";

const TAB = "inline-flex shrink-0 items-center gap-1 rounded-control px-3 py-1.5 text-sm no-underline transition-colors";
const ACTIVE = "bg-hover font-semibold text-default";
const INACTIVE = "text-subtle hover:bg-hover hover:text-default";

const isActive = (route: string, current: string) => (route === "" ? current === "" : current === route || current.startsWith(`${route}/`));

function TabMenu({ tab, current }: { tab: Tab & { menu: NonNullable<Tab["menu"]> }; current: string }) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEscape(open, () => setOpen(false));
  useEffect(() => setOpen(false), [current]);
  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => root.current?.contains(event.target as Node) || setOpen(false);
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);
  return (
    <div ref={root} className="relative">
      <button type="button" aria-expanded={open} aria-haspopup="true" onClick={() => setOpen(!open)} className={cn(TAB, isActive(tab.route, current) ? ACTIVE : INACTIVE)}>
        {tab.label}
        <Icon name="chevron-down" size={12} className={cn("transition-transform", open && "rotate-180")} />
      </button>
      {open && (
        <ul className="absolute top-full left-0 z-50 m-0 mt-1 w-72 list-none rounded-control border border-border bg-page p-1 shadow-lg">
          {tab.menu.map((item) => (
            <li key={item.route}>
              <Link {...pageLink(item.route)} className="block rounded-control px-3 py-2 no-underline hover:bg-hover">
                <span className={cn("block text-sm text-default", current === item.route && "font-semibold")}>{item.label}</span>
                <span className="block text-xs text-subtle">{item.description}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function TabNav({ className }: { className: string }) {
  const current = useLocation().pathname.replace(/^\/+|\/+$/g, "");
  return (
    <nav aria-label="Site" className={cn("items-center gap-1", className)}>
      {TABS.map((tab) =>
        tab.menu ? (
          <TabMenu key={tab.route} tab={{ ...tab, menu: tab.menu }} current={current} />
        ) : (
          <Link key={tab.route} {...pageLink(tab.route)} className={cn(TAB, isActive(tab.route, current) ? ACTIVE : INACTIVE)}>
            {tab.label}
          </Link>
        ),
      )}
      <SiteLink href={USER_DOCS.href} className={cn(TAB, INACTIVE)}>
        {USER_DOCS.label}
        <Icon name="external-link" size={12} />
      </SiteLink>
    </nav>
  );
}
