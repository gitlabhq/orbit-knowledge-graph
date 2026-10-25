import { cn, pageLink } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { SECTIONS } from "../sections";

export function SectionNav({ className }: { className: string }) {
  return (
    <nav aria-label="Sections" className={cn("items-center gap-1", className)}>
      {SECTIONS.map(({ route, label }) => (
        <Link
          key={route}
          {...pageLink(route)}
          className="shrink-0 rounded-control px-3 py-1.5 text-sm no-underline transition-colors"
          activeProps={{ className: "bg-hover font-semibold text-default" }}
          inactiveProps={{ className: "text-subtle hover:bg-hover hover:text-default" }}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}
