import { baseMdxComponents, findPage, NotFound, PageLayout, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { Sidebar } from "../components/Sidebar";

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
      nav={page.path.startsWith("engineering") ? <Sidebar /> : undefined}
      owners={<PageFooter page={page} />}
    />
  );
}
