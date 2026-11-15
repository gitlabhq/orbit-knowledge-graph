import { Embed, EmbedGrid, findPage, PageFrame, pageHeadOf, Video, VideoGrid } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { links, videos } from "../content/demos";
import { EmptyState } from "../roadmap/EmptyState";

export const Route = createFileRoute("/content/")({
  head: () => pageHeadOf(findPage("content")),
  component: ContentPage,
});

function ContentPage() {
  const page = findPage("content");
  return (
    <PageFrame path="content" title={page?.meta.title ?? "What's new"} description={page?.meta.description} footer={page && <PageFooter page={page} />}>
      {links.length > 0 && (
        <section aria-labelledby="related">
          <h2 id="related" className="m-0 text-xl font-semibold text-default">
            Articles and demos
          </h2>
          <EmbedGrid>
            {links.map((link) => (
              <Embed key={link.href} {...link} />
            ))}
          </EmbedGrid>
        </section>
      )}
      {videos.length > 0 && (
        <section aria-labelledby="videos" className="mt-6">
          <h2 id="videos" className="m-0 text-xl font-semibold text-default">
            Videos
          </h2>
          <VideoGrid>
            {videos.map((video) => (
              <Video key={video.id} {...video} />
            ))}
          </VideoGrid>
        </section>
      )}
      {videos.length === 0 && links.length === 0 && <EmptyState message="Nothing new yet." reset={false} />}
    </PageFrame>
  );
}
