import { Embed, EmbedGrid, findPage, LinkedHeading, PageFrame, pageHeadOf, Video, VideoGrid } from "@gitlab-org/orbit-site";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { type DemoVideo, featured, links, videos } from "../content/demos";

export const Route = createFileRoute("/about/")({
  beforeLoad: ({ location }) => {
    if (location.hash === "roadmap") throw redirect({ to: "/next", replace: true });
  },
  head: () => pageHeadOf(findPage("about")),
  component: AboutPage,
});

const without = ({ featured: _, ...video }: DemoVideo) => video;

function AboutPage() {
  const page = findPage("about");
  return (
    <PageFrame path="about" title={page?.meta.title ?? "About GitLab Orbit"} description={page?.meta.description} footer={page && <PageFooter page={page} />}>
      {featured && <Video {...without(featured)} bare />}
      {links.length > 0 && (
        <section aria-labelledby="related" className="mt-10">
          <LinkedHeading level={2} id="related" className="m-0 text-xl font-semibold text-default">
            Articles and demos
          </LinkedHeading>
          <EmbedGrid>
            {links.map((link) => (
              <Embed key={link.href} {...link} />
            ))}
          </EmbedGrid>
        </section>
      )}
      {videos.length > 0 && (
        <section aria-labelledby="videos" className="mt-6">
          <LinkedHeading level={2} id="videos" className="m-0 text-xl font-semibold text-default">
            Videos
          </LinkedHeading>
          <VideoGrid>
            {videos.map((video) => (
              <Video key={video.id} {...without(video)} />
            ))}
          </VideoGrid>
        </section>
      )}
    </PageFrame>
  );
}
