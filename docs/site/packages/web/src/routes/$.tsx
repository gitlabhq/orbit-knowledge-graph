import { findPage, NotFound, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { PageView } from "../components/PageView";

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
  return <PageView page={page} body={body} />;
}
