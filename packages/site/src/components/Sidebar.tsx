import { type Page, pageLink, pages } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { SECTIONS } from "../sections";

const ITEM = "block rounded-control px-3 py-1.5 text-sm no-underline transition-colors";

const sectionPages = (section: string) =>
  pages
    .filter((page) => page.path === section || page.path.startsWith(`${section}/`))
    .sort((a, b) => Number(b.path === section) - Number(a.path === section) || a.source.localeCompare(b.source));

const homePages = () => SECTIONS.flatMap(({ route }) => pages.filter((page) => page.path === route));

export function Sidebar({ page }: { page: Page }) {
  const [section = ""] = page.path.split("/");
  return (
    <nav aria-label="Pages" className="max-h-48 shrink-0 overflow-y-auto border-b border-border px-3 py-3 md:sticky md:max-h-none md:top-[var(--header-h)] md:h-[calc(100vh-var(--header-h))] md:w-64 md:border-r md:border-b-0 md:py-6">
      {(section ? sectionPages(section) : homePages()).map((item) => (
        <Link
          key={item.path}
          {...pageLink(item.path)}
          className={ITEM}
          activeOptions={{ exact: true }}
          activeProps={{ className: "bg-hover font-semibold text-default" }}
          inactiveProps={{ className: "text-subtle hover:bg-hover hover:text-default" }}
        >
          {item.path === section ? "Overview" : item.meta.title}
        </Link>
      ))}
    </nav>
  );
}
