import { cn, Icon, NavMenu, pageLink, SiteLink } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { TABS, USER_DOCS } from "../sections";

export function TabNav({ className }: { className: string }) {
  return (
    <nav aria-label="Site" className={cn("items-center gap-1", className)}>
      {TABS.map((tab) =>
        tab.menu ? (
          <NavMenu key={tab.route} label={tab.label} route={tab.route} items={tab.menu} />
        ) : (
          <Link key={tab.route} {...pageLink(tab.route)} activeOptions={{ exact: tab.route === "" }} className="gl-nav-item">
            {tab.label}
          </Link>
        ),
      )}
      <SiteLink href={USER_DOCS.href} className="gl-nav-item">
        {USER_DOCS.label}
        <Icon name="external-link" size={12} />
      </SiteLink>
    </nav>
  );
}
