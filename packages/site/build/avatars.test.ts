import { describe, expect, spyOn, test } from "bun:test";
import { fetchAvatar, fetchAvatars } from "./avatars";

const AVATAR = "https://gitlab.com/uploads/-/system/user/avatar/1/avatar.png";

const reply = (users: unknown, status = 200) => (async () => new Response(JSON.stringify(users), { status })) as unknown as typeof fetch;

describe("author avatars", () => {
  test("reads the GitLab avatar of an author", async () => {
    expect(await fetchAvatar("alice", undefined, reply([{ avatar_url: AVATAR }]))).toStrictEqual({ username: "alice", avatar: AVATAR });
    expect(await fetchAvatar("alice", undefined, reply([{ avatar_url: null }]))).toStrictEqual({ username: "alice" });
  });

  test("falls back to initials for an unknown author or a failed request", async () => {
    const warn = spyOn(console, "warn").mockImplementation(() => {});
    const offline = (async () => {
      throw new Error("offline");
    }) as unknown as typeof fetch;
    expect(await fetchAvatar("nobody", undefined, reply([]))).toStrictEqual({ username: "nobody" });
    expect(await fetchAvatar("alice", undefined, reply({}, 500))).toStrictEqual({ username: "alice" });
    expect(await fetchAvatar("alice", undefined, offline)).toStrictEqual({ username: "alice" });
    expect(warn.mock.calls.map(([message]) => message)).toEqual([
      "author @nobody shows initials: no active GitLab user has this username",
      "author @alice shows initials: GitLab returned 500",
      "author @alice shows initials: offline",
    ]);
    warn.mockRestore();
  });

  test("asks GitLab once for each author and sends the token only when set", async () => {
    const asked: [string, unknown][] = [];
    const fetcher = (async (url: string, init: RequestInit) => {
      asked.push([url, init.headers]);
      return new Response(JSON.stringify([{ avatar_url: AVATAR }]));
    }) as unknown as typeof fetch;
    expect(Object.keys(await fetchAvatars(["alice", "bob", "alice"], "t", fetcher))).toEqual(["alice", "bob"]);
    await fetchAvatars(["a.b c"], undefined, fetcher);
    expect(asked).toEqual([
      ["https://gitlab.com/api/v4/users?username=alice", { authorization: "Bearer t" }],
      ["https://gitlab.com/api/v4/users?username=bob", { authorization: "Bearer t" }],
      ["https://gitlab.com/api/v4/users?username=a.b%20c", {}],
    ]);
  });
});
