import { type Page, SiteLink } from "@gitlab-org/orbit-site";

export function PageFooter({ page }: { page: Page }) {
  return (
    <span className="ml-auto flex gap-4">
      {page.meta.lastChanged && <span>Updated {page.meta.lastChanged}</span>}
      <SiteLink href={page.meta.sourceUrl}>View source</SiteLink>
    </span>
  );
}
