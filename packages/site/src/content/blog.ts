import { type Page, pages } from "@gitlab-org/orbit-site";
import authorData from "@data/content/authors.yaml";

export type Author = { username: string; name: string; title?: string; avatar?: string };

export type Post = {
  slug: string;
  page: Page;
  title: string;
  description: string;
  date: string;
  authors: Author[];
  tags: string[];
  minutes: number;
};

const WORDS_PER_MINUTE = 220;

const authors = authorData as Record<string, Omit<Author, "username">>;

function author(username: string): Author {
  const { avatar, ...known } = authors[username];
  return { username, ...known, avatar: avatar && !/^https?:/.test(avatar) ? `${import.meta.env.BASE_URL}${avatar}` : avatar };
}

export const posts: Post[] = pages
  .flatMap((page) => (page.meta.post ? [{ page, ...page.meta.post }] : []))
  .map(({ page, date, authors: usernames, tags }) => ({
    slug: page.path.slice("content/blog/".length),
    page,
    title: page.meta.title,
    description: page.meta.description ?? "",
    date,
    authors: usernames.map(author),
    tags,
    minutes: Math.max(1, Math.round(page.meta.words / WORDS_PER_MINUTE)),
  }))
  .sort((a, b) => b.date.localeCompare(a.date) || a.title.localeCompare(b.title));

const bySlug = new Map(posts.map((post) => [post.slug, post]));

export const findPost = (slug: string) => bySlug.get(slug);
