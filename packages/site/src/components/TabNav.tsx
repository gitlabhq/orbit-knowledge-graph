import { cn, Icon, NavMenu, pageLink, SiteLink } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { NEXT_TAB, TABS, USER_DOCS } from "../sections";

// No Home item: the GitLab Orbit logo links to the home page.
export function TabNav({ className }: { className: string }) {
  return (
    <nav aria-label="Site" className={cn("items-center gap-1", className)}>
      {TABS.map((tab) =>
        tab.menu ? (
          <NavMenu key={tab.route} label={tab.label} route={tab.route} items={tab.menu} />
        ) : (
          <Link key={tab.route} {...pageLink(tab.route)} className="gl-nav-item">
            {tab.label}
          </Link>
        ),
      )}
      <SiteLink href={USER_DOCS.href} aria-label={`${USER_DOCS.label} (opens in a new tab)`} className="gl-nav-item">
        {USER_DOCS.label}
        <Icon name="external-link" size={12} />
      </SiteLink>
      <Link {...pageLink(NEXT_TAB.route)} className="gl-nav-item">
        {NEXT_TAB.label}
      </Link>
    </nav>
  );
}
