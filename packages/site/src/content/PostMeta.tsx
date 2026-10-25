import { Avatar } from "@gitlab-org/orbit-site";
import type { Post } from "./blog";
import { formatDate } from "../roadmap/calendar";

export function PostMeta({ post, size = 24 }: { post: Post; size?: number }) {
  return (
    <span className="flex items-center gap-3 text-sm text-subtle">
      <span className="flex shrink-0 items-center -space-x-1.5">
        {post.authors.map((author) => (
          <Avatar key={author.username} name={author.name} src={author.avatar} size={size} className="ring-2 ring-page" />
        ))}
      </span>
      <span className="min-w-0 leading-tight">
        <span className="block truncate font-medium text-default">{post.authors.map((author) => author.name).join(", ")}</span>
        <span className="block text-xs">
          <time dateTime={post.date}>{formatDate(post.date)}</time> · {post.minutes} min read
        </span>
      </span>
    </span>
  );
}
