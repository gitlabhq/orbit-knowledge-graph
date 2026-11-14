import { findPage, type NavItem, type Page, pages, SectionNav } from "@gitlab-org/orbit-site";
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
];

export function Sidebar() {
  return <SectionNav items={items()} label="Engineering" storageKey="orbit-site.sidebar-collapsed" />;
}
