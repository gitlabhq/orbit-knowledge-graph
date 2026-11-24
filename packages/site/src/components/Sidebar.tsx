import { useState } from "react";
import { findPage, Icon, type NavItem, type Page, pages, SectionNav } from "@gitlab-org/orbit-site";
import { useRouterState } from "@tanstack/react-router";
import { ENGINEERING } from "../sections";

const parentOf = (path: string) => path.slice(0, path.lastIndexOf("/"));

const byOrder = (a: Page, b: Page) => (a.meta.order ?? Infinity) - (b.meta.order ?? Infinity) || a.source.localeCompare(b.source);

function tree(path: string): NavItem[] {
  return pages
    .filter((page) => parentOf(page.path) === path)
    .sort(byOrder)
    .map((page) => {
      const children = tree(page.path);
      return { path: page.path, title: page.meta.title, ...(children.length > 0 && { children }) };
    });
}

const items = (): NavItem[] => [
  { path: "engineering", title: findPage("engineering")?.meta.title ?? "Engineering" },
  ...ENGINEERING.map(({ route, label }) => ({ path: route, title: label, children: tree(route) })),
  { path: "team", title: "Team" },
];

export function Sidebar({ title }: { title: string }) {
  const path = useRouterState({ select: (state) => state.location.pathname });
  const [open, setOpen] = useState(false);
  const [shown, setShown] = useState(path);
  if (shown !== path) {
    setShown(path);
    setOpen(false);
  }
  return (
    <div data-open={open} className="sidebar md:contents">
      <button type="button" aria-expanded={open} onClick={() => setOpen(!open)} className="sidebar-toggle md:hidden">
        <span className="sidebar-toggle-section">Developers</span>
        <span className="min-w-0 flex-1 truncate">{title}</span>
        <Icon name={open ? "chevron-up" : "chevron-down"} size={16} />
      </button>
      <SectionNav items={items()} label="Engineering" storageKey="orbit-site.sidebar-collapsed" />
    </div>
  );
}
