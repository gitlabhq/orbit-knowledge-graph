import { findPage, pageLink } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";

const ORDER = ["", "data-model", "sdlc-indexing", "code-indexing", "schema-evolution", "querying", "security", "orbit-cli", "testing", "template"];

const ITEM = "block rounded-control px-3 py-1.5 text-sm no-underline transition-colors";

export function Sidebar() {
  return (
    <nav aria-label="Architecture" className="hidden w-64 shrink-0 border-r border-border px-3 py-6 md:sticky md:top-[var(--header-h)] md:block md:h-[calc(100vh-var(--header-h))]">
      {ORDER.map(findPage).map(
        (page) =>
          page && (
            <Link
              key={page.path}
              {...pageLink(page.path)}
              className={ITEM}
              activeOptions={{ exact: true }}
              activeProps={{ className: "bg-hover font-semibold text-default" }}
              inactiveProps={{ className: "text-subtle hover:bg-hover hover:text-default" }}
            >
              {page.path === "" ? "Overview" : page.meta.title}
            </Link>
          ),
      )}
    </nav>
  );
}
