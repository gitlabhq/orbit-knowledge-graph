import { findPage, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { ContentHeader } from "../content/ContentHeader";
import { links, videos } from "../content/demos";
import { EmptyState } from "../roadmap/EmptyState";
import { LinkCard } from "../ui/LinkCard";
import { Video } from "../ui/Video";

export const Route = createFileRoute("/content/")({
  head: () => pageHeadOf(findPage("content")),
  component: ContentPage,
});

function ContentPage() {
  const page = findPage("content");
  const [featured, ...rest] = videos;
  return (
    <div className="page-container pb-14">
      <ContentHeader title={page?.meta.title ?? "Content"} description={page?.meta.description} />
      <div className="flex flex-col gap-10">
        {featured && <Video video={featured} featured />}
        {rest.length > 0 && (
          <ul className="m-0 grid list-none gap-4 p-0 md:grid-cols-3">
            {rest.map((video) => (
              <li key={video.id}>
                <Video video={video} />
              </li>
            ))}
          </ul>
        )}
        {links.length > 0 && (
          <section aria-labelledby="related" className="flex flex-col gap-4">
            <h2 id="related" className="m-0 text-xl font-semibold text-default">
              Related links
            </h2>
            <ul className="m-0 grid list-none gap-3 p-0 lg:grid-cols-2">
              {links.map((link) => (
                <li key={link.href}>
                  <LinkCard link={link} />
                </li>
              ))}
            </ul>
          </section>
        )}
        {!featured && links.length === 0 && <EmptyState message="No demos are available." reset={false} />}
      </div>
    </div>
  );
}
