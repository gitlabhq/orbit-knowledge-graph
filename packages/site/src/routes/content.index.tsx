import { findPage, Icon, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { posts } from "../content/blog";
import { ContentHeader } from "../content/ContentHeader";
import { videos } from "../content/demos";
import { formatDate } from "../roadmap/calendar";

export const Route = createFileRoute("/content/")({
  head: () => pageHeadOf(findPage("content")),
  component: ContentPage,
});

type Choice = { to: "/content/blog" | "/content/demos"; icon: string; title: string; description: string; count: string; latest?: string };

function ChoiceCard({ to, icon, title, description, count, latest }: Choice) {
  return (
    <Link to={to} className="card group flex h-full flex-col gap-3 p-5 no-underline">
      <span className="flex items-center gap-3">
        <Icon name={icon} size={16} className="text-subtle" />
        <span className="text-base font-semibold text-default">{title}</span>
        <span className="ml-auto text-sm text-subtle">{count}</span>
      </span>
      <span className="text-sm text-subtle">{description}</span>
      {latest && <span className="mt-auto truncate border-t border-border pt-3 text-sm text-default">Latest: {latest}</span>}
      <span className="inline-flex items-center gap-1 text-sm font-semibold text-link">
        Open {title.toLowerCase()}
        <Icon name="arrow-right" size={12} />
      </span>
    </Link>
  );
}

const plural = (count: number, word: string) => `${count} ${word}${count === 1 ? "" : "s"}`;

function ContentPage() {
  const page = findPage("content");
  const [post] = posts;
  const [video] = videos;
  const choices: Choice[] = [
    {
      to: "/content/blog",
      icon: "document",
      title: "Blog",
      description: "Engineering notes and product updates from the Orbit team.",
      count: plural(posts.length, "post"),
      latest: post && `${post.title} · ${formatDate(post.date)}`,
    },
    {
      to: "/content/demos",
      icon: "play",
      title: "Demos",
      description: "Videos and talks that show how Orbit works and how agents use it.",
      count: plural(videos.length, "video"),
      latest: video?.title,
    },
  ];
  return (
    <div className="page-container pb-14">
      <ContentHeader title={page?.meta.title ?? "Content"} description={page?.meta.description} />
      <ul className="m-0 grid list-none gap-4 p-0 md:grid-cols-2">
        {choices.map((choice) => (
          <li key={choice.to}>
            <ChoiceCard {...choice} />
          </li>
        ))}
      </ul>
    </div>
  );
}
