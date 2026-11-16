import { Fragment } from "react";
import { cn, NavMenu, pageLink, SiteLink } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { TABS, USER_DOCS } from "../sections";

// No Home item: the GitLab Orbit logo links to the home page.
export function TabNav({ className }: { className: string }) {
  return (
    <nav aria-label="Site" className={cn("items-center gap-1", className)}>
      {TABS.map((tab, index) => (
        <Fragment key={tab.route}>
          {tab.menu ? (
            <NavMenu label={tab.label} route={tab.route} items={tab.menu} />
          ) : (
            <Link {...pageLink(tab.route)} className="gl-nav-item">
              {tab.label}
            </Link>
          )}
          {/* Docs sits second, after About. It opens in a new tab, which its label says. */}
          {index === 0 && (
            <SiteLink href={USER_DOCS.href} aria-label={`${USER_DOCS.label} (opens in a new tab)`} className="gl-nav-item">
              {USER_DOCS.label}
            </SiteLink>
          )}
        </Fragment>
      ))}
    </nav>
  );
}
