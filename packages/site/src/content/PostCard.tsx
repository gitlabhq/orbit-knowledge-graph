import { cn } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import type { Post } from "./blog";
import { PostMeta } from "./PostMeta";

export function PostCard({ post, featured = false }: { post: Post; featured?: boolean }) {
  return (
    <Link to="/content/blog/$slug" params={{ slug: post.slug }} className={cn("card group flex h-full flex-col gap-3 no-underline", featured ? "p-7" : "p-5")}>
      {post.tags.length > 0 && (
        <span className="flex flex-wrap gap-1.5">
          {post.tags.map((tag) => (
            <span key={tag} className="rounded-full bg-surface-strong px-2 py-0.5 text-xs font-medium text-subtle">
              {tag}
            </span>
          ))}
        </span>
      )}
      <span className={cn("leading-snug font-bold tracking-tight text-default", featured ? "text-2xl sm:text-3xl" : "text-lg")}>{post.title}</span>
      <span className={cn("text-subtle", featured ? "text-base" : "line-clamp-2 text-sm")}>{post.description}</span>
      <span className="mt-auto pt-2">
        <PostMeta post={post} size={featured ? 28 : 22} />
      </span>
    </Link>
  );
}
