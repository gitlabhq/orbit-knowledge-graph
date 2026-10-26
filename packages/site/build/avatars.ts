export type PageAuthor = { username: string; avatar?: string };

async function avatarUrl(username: string, token: string | undefined, fetcher: typeof fetch) {
  const response = await fetcher(`https://gitlab.com/api/v4/users?username=${encodeURIComponent(username)}`, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  if (!response.ok) throw new Error(`GitLab returned ${response.status}`);
  const [user] = (await response.json()) as { avatar_url?: string | null }[];
  if (!user) throw new Error("no active GitLab user has this username");
  return user.avatar_url ?? undefined;
}

export async function fetchAvatar(username: string, token: string | undefined, fetcher: typeof fetch = fetch): Promise<PageAuthor> {
  try {
    const avatar = await avatarUrl(username, token, fetcher);
    return { username, ...(avatar && { avatar }) };
  } catch (error) {
    console.warn(`author @${username} shows initials: ${(error as Error).message}`);
    return { username };
  }
}

export async function fetchAvatars(usernames: string[], token: string | undefined, fetcher: typeof fetch = fetch): Promise<Record<string, PageAuthor>> {
  const authors = await Promise.all([...new Set(usernames)].map((username) => fetchAvatar(username, token, fetcher)));
  return Object.fromEntries(authors.map((author) => [author.username, author]));
}
