import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { cn, Icon, pageLink, SiteLink, ThemeToggle } from "@gitlab-org/orbit-site";
import { Link, useRouterState } from "@tanstack/react-router";
import { TABS, USER_DOCS } from "../sections";

const openSearch = () => document.querySelector<HTMLButtonElement>(".site-shell > header .search-button")?.click();

export function MobileMenu({ className, sheetClassName }: { className?: string; sheetClassName?: string }) {
  const path = useRouterState({ select: (state) => state.location.pathname });
  const [open, setOpen] = useState(false);
  const [level, setLevel] = useState<string | null>(null);
  const [shown, setShown] = useState(path);
  if (shown !== path) {
    setShown(path);
    setOpen(false);
  }
  const sub = TABS.find((tab) => tab.route === level && tab.menu);

  useEffect(() => {
    if (!open) return;
    const root = document.documentElement;
    root.classList.add("mobile-menu-open");
    const onKey = (event: KeyboardEvent) => event.key === "Escape" && setOpen(false);
    window.addEventListener("keydown", onKey);
    return () => {
      root.classList.remove("mobile-menu-open");
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const close = () => {
    setOpen(false);
    setLevel(null);
  };
  return (
    <>
      <button type="button" aria-expanded={open} aria-controls="mobile-menu" aria-label={open ? "Close menu" : "Open menu"} onClick={() => (open ? close() : setOpen(true))} className={cn("gl-button btn-default-tertiary btn-icon", className)}>
        <Icon name={open ? "close" : "hamburger"} size={16} />
      </button>
      {createPortal(
        <nav id="mobile-menu" aria-label="Site" hidden={!open} className={cn("mobile-menu", sheetClassName)}>
          <button
            type="button"
            className="mobile-menu-search"
            onClick={openSearch}
          >
            <Icon name="search" size={16} />
            Search GitLab Orbit
          </button>
          {sub ? (
            <div key={sub.route} className="mobile-menu-level">
              <button type="button" onClick={() => setLevel(null)} className="mobile-menu-back">
                <Icon name="chevron-left" size={16} />
                Back
              </button>
              <p className="mobile-menu-group">{sub.label}</p>
              <ul className="mobile-menu-list">
                {sub.menu!.map((item) => (
                  <li key={item.route}>
                    <Link {...pageLink(item.route)} onClick={close} className="mobile-menu-row mobile-menu-row-sub">
                      <Icon name={item.icon ?? "git-merge"} size={16} />
                      <span>
                        {item.label}
                        <span className="mobile-menu-note">{item.description}</span>
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <ul key="top" className="mobile-menu-list mobile-menu-level">
              <li>
                <Link to="/" activeOptions={{ exact: true }} onClick={close} className="mobile-menu-row">
                  Home
                </Link>
              </li>
              {TABS.map((tab, index) => (
                <li key={tab.route}>
                  {tab.menu ? (
                    <button type="button" onClick={() => setLevel(tab.route)} className="mobile-menu-row">
                      {tab.label}
                      <Icon name="chevron-right" size={16} />
                    </button>
                  ) : (
                    <Link {...pageLink(tab.route)} onClick={close} className="mobile-menu-row">
                      {tab.label}
                    </Link>
                  )}
                  {index === 0 && (
                    <SiteLink href={USER_DOCS.href} aria-label={`${USER_DOCS.label} (opens in a new tab)`} className="mobile-menu-row">
                      {USER_DOCS.label}
                      <Icon name="external-link" size={16} />
                    </SiteLink>
                  )}
                </li>
              ))}
            </ul>
          )}
          <div className="mobile-menu-foot">
            <ThemeToggle />
          </div>
        </nav>,
        document.body,
      )}
    </>
  );
}
