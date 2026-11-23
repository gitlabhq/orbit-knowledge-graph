import { baseMdxComponents, findPage, LinkCard, LinkCards, LinkedHeading, NotFound, PageLayout, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { Sidebar } from "../components/Sidebar";
import { GUIDES } from "../sections";

export const Route = createFileRoute("/$")({
  loader: async ({ params }) => {
    const page = findPage(params._splat ?? "");
    if (!page) throw notFound();
    return { page, body: await page.load() };
  },
  head: ({ loaderData }) => pageHeadOf(loaderData?.page),
  component: ContentRoute,
  notFoundComponent: () => <NotFound />,
});

function ContentRoute() {
  const { page, body } = Route.useLoaderData();
  return (
    <PageLayout
      page={page}
      body={body}
      components={baseMdxComponents}
      nav={page.path.startsWith("engineering") ? <Sidebar title={page.meta.title} /> : undefined}
      footer={<PageFooter page={page} />}
    >
      {page.path === "engineering" && (
        <section aria-labelledby="guides" className="mt-10">
          <LinkedHeading level={2} id="guides" className="m-0 text-xl font-semibold text-default">
            Guides
          </LinkedHeading>
          <LinkCards>
            {GUIDES.map((guide) => (
              <LinkCard key={guide.route} title={guide.label} href={`/${guide.route}`} description={guide.description} icon={guide.icon} />
            ))}
          </LinkCards>
        </section>
      )}
    </PageLayout>
  );
}
