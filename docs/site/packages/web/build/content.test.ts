import { describe, expect, test } from "bun:test";
import { siteLinks } from "./content";

describe("siteLinks", () => {
  test("turns page links into site routes", () => {
    expect(siteLinks("[Security](security.md#supply-chain) and [overview](README.md)")).toBe("[Security](/security#supply-chain) and [overview](/)");
  });

  test("sends links outside the folder to the repository", () => {
    expect(siteLinks("[ADR](../design-documents/decisions/001_grpc_communication.md)")).toMatch(
      /^\[ADR\]\(https:\/\/gitlab\.com\/gitlab-org\/orbit\/knowledge-graph\/-\/blob\/[^/]+\/docs\/design-documents\/decisions\/001_grpc_communication\.md\)$/,
    );
  });

  test("keeps absolute, anchor, and site links", () => {
    const markdown = "[a](https://example.com) [b](#terms) [c](/testing)";
    expect(siteLinks(markdown)).toBe(markdown);
  });
});
