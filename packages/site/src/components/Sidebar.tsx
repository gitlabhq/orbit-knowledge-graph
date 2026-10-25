import { DocsNav, findPage, type NavItem, type Page, pages } from "@gitlab-org/orbit-site";
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
  return (
    <DocsNav
      items={items()}
      label="Engineering"
      className="max-h-48 shrink-0 overflow-y-auto border-b border-border px-3 py-3 md:sticky md:top-[var(--header-h)] md:h-[calc(100vh-var(--header-h))] md:max-h-none md:w-72 md:border-r md:border-b-0 md:py-6"
    />
  );
}
