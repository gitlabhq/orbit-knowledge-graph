import { Avatar, cn, type Page, type PageBody, PageContent, SiteLink, Toc, useHeadingLinks } from "@gitlab-org/orbit-site";
import { type ReactNode, useRef } from "react";
import type { PageAuthor } from "../../build/avatars";
import { Sidebar } from "./Sidebar";

function Authors({ authors }: { authors: PageAuthor[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-xs font-semibold uppercase tracking-wide text-subtle">{authors.length > 1 ? "Authors" : "Author"}</span>
      {authors.map(({ username, avatar }) => (
        <SiteLink
          key={username}
          href={`https://gitlab.com/${username}`}
          className="flex items-center gap-2 rounded-full border border-border-strong py-0.5 pr-3 pl-0.5 text-xs text-default no-underline hover:bg-hover"
        >
          <Avatar name={username} src={avatar} size={22} />
          @{username}
        </SiteLink>
      ))}
    </div>
  );
}

export function PageView({ page, body, sidebar = false, children }: { page: Page; body: PageBody; sidebar?: boolean; children?: ReactNode }) {
  const article = useRef<HTMLElement>(null);
  useHeadingLinks(article, page.path);
  return (
    <div className="flex flex-col md:flex-row">
      {sidebar && <Sidebar />}
      <div className={cn("flex min-w-0 flex-1 gap-10", sidebar ? "px-6 py-8 lg:px-10" : "page-container py-10")}>
        <article ref={article} className="min-w-0 max-w-4xl flex-1">
          <header className="mb-8 border-b border-border pb-6">
            <h1 className="m-0 text-3xl font-semibold text-default">{page.meta.title}</h1>
            {page.meta.description && <p className="mt-2 mb-0 text-base text-subtle">{page.meta.description}</p>}
            {children}
            {!page.meta.post && page.meta.authors.length > 0 && (
              <div className="mt-4">
                <Authors authors={page.meta.authors} />
              </div>
            )}
          </header>
          <PageContent body={body} />
          <footer className="mt-14 flex flex-wrap items-center justify-end gap-4 border-t border-border pt-5 text-xs text-subtle">
            {page.meta.lastChanged && <span>Updated {page.meta.lastChanged}</span>}
            <SiteLink href={page.meta.sourceUrl}>View source</SiteLink>
          </footer>
        </article>
        <Toc
          article={article}
          pageKey={page.path}
          className="sticky top-[calc(var(--header-h)+1.5rem)] hidden max-h-[calc(100vh-var(--header-h)-3rem)] w-56 shrink-0 self-start overflow-y-auto xl:block"
        />
      </div>
    </div>
  );
}
