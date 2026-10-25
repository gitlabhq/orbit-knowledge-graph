import { Avatar, type Page, type PageBody, PageContent, SiteLink, Toc, useHeadingLinks } from "@gitlab-org/orbit-site";
import { useRef } from "react";
import { Sidebar } from "./Sidebar";

function Authors({ authors }: { authors: string[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-xs font-semibold uppercase tracking-wide text-subtle">{authors.length > 1 ? "Authors" : "Author"}</span>
      {authors.map((author) => (
        <SiteLink
          key={author}
          href={`https://gitlab.com/${author}`}
          className="flex items-center gap-2 rounded-full border border-border-strong py-0.5 pr-3 pl-0.5 text-xs text-default no-underline hover:bg-hover"
        >
          <Avatar name={author} size={22} />
          @{author}
        </SiteLink>
      ))}
    </div>
  );
}

export function PageView({ page, body }: { page: Page; body: PageBody }) {
  const article = useRef<HTMLElement>(null);
  useHeadingLinks(article, page.path);
  return (
    <>
      <Sidebar page={page} />
      <div className="flex min-w-0 flex-1 gap-10 px-6 py-8 lg:px-10">
        <article ref={article} className="min-w-0 max-w-4xl flex-1">
          <header className="mb-8 border-b border-border pb-6">
            <h1 className="text-4xl font-extrabold tracking-tight text-default">{page.meta.title}</h1>
            {page.meta.description && <p className="mt-3 text-lg text-subtle">{page.meta.description}</p>}
            {page.meta.authors.length > 0 && (
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
    </>
  );
}
