import { cn } from "@gitlab-org/orbit-site";
import type { ReactNode } from "react";

// The PageLayout frame for routes that draw their own content, such as the roadmap and the home page.
// PageLayout renders only a page body, so these routes take its frame: the page panel, the centered
// text column, the header, and the footer at the bottom of the window. `wide` spans the full width.
export function Frame({
  title,
  description,
  header,
  footer,
  wide = false,
  hideHeader = false,
  children,
}: {
  title: string;
  description?: string;
  header?: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
  hideHeader?: boolean;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col md:h-full md:min-h-0 md:flex-row">
      <div className="page-panel flex min-w-0 flex-1">
        <div className="page-container flex min-w-0 flex-1 flex-col pt-10">
          <article className={cn("flex min-w-0 flex-1 flex-col", !wide && "mx-auto w-full max-w-4xl")}>
            {hideHeader ? (
              <h1 className="sr-only">{title}</h1>
            ) : (
              <header className="mb-8">
                <h1 className="m-0 text-3xl font-semibold text-default">{title}</h1>
                {description && <p className="mt-2 mb-0 text-base text-subtle">{description}</p>}
                {header}
              </header>
            )}
            {children}
            <div aria-hidden="true" className="h-14 shrink-0" />
            <footer className="mt-auto flex flex-wrap items-center justify-end gap-4 py-4 text-xs text-subtle">{footer}</footer>
          </article>
        </div>
      </div>
    </div>
  );
}
