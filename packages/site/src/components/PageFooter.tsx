import { type Page, SiteLink } from "@gitlab-org/orbit-site";

// The kit footer dates a page by its file in content/, which the build writes and git does not track.
// These pages come from docs/pages/, so the footer shows that file's last commit and links its source,
// on the right, as before.
export function PageFooter({ page }: { page: Page }) {
  return (
    <span className="ml-auto flex gap-4">
      {page.meta.lastChanged && <span>Updated {page.meta.lastChanged}</span>}
      <SiteLink href={page.meta.sourceUrl}>View source</SiteLink>
    </span>
  );
}
