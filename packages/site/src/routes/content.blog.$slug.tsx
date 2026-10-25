import { NotFound, pageHead } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { PageView } from "../components/PageView";
import { findPost } from "../content/blog";
import { PostMeta } from "../content/PostMeta";

export const Route = createFileRoute("/content/blog/$slug")({
  loader: async ({ params }) => {
    const post = findPost(params.slug);
    if (!post) throw notFound();
    return { post, body: await post.page.load() };
  },
  head: ({ loaderData }) => pageHead(loaderData?.post.title ?? "Page not found"),
  component: PostPage,
  notFoundComponent: () => <NotFound />,
});

function PostPage() {
  const { post, body } = Route.useLoaderData();
  return (
    <PageView page={post.page} body={body}>
      <div className="mt-5">
        <PostMeta post={post} size={28} />
      </div>
    </PageView>
  );
}
