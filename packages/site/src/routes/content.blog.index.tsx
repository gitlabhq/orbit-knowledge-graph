import { pageHead } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { posts } from "../content/blog";
import { ContentHeader } from "../content/ContentHeader";
import { PostCard } from "../content/PostCard";
import { EmptyState } from "../roadmap/EmptyState";

export const Route = createFileRoute("/content/blog/")({
  head: () => pageHead("Blog"),
  component: BlogPage,
});

function BlogPage() {
  return (
    <div className="page-container pb-14">
      <ContentHeader title="Blog" description="Engineering notes and product updates from the Orbit team." />
      {posts.length === 0 ? (
        <EmptyState message="The blog has no posts." reset={false} />
      ) : (
        <ul className="m-0 grid list-none gap-4 p-0 sm:grid-cols-2 lg:grid-cols-3">
          {posts.map((post) => (
            <li key={post.slug}>
              <PostCard post={post} />
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
