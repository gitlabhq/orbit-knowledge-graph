import { describe, expect, test } from "bun:test";
import { describeContent } from "@gitlab-org/orbit-site/testing";
import { HOME } from "../src/sections";
import { contentDir, repoBlob, siteLinks, siteMeta, sourceFiles, syncContent } from "./content";

syncContent();
describeContent(contentDir, { routes: { "": HOME }, meta: siteMeta });

describe("site links", () => {
  const routes = sourceFiles();
  const adr = "docs/design-documents/decisions/021_entity_context_expansion.md";
  const repoRaw = repoBlob.replace("/-/blob/", "/-/raw/");

  test("maps every section README to the section route", () => {
    expect(routes.get("docs/README.md")).toBe("engineering");
    expect(routes.get("docs/design-documents/decisions/README.md")).toBe("engineering/adr");
    expect(routes.get("docs/runbooks/README.md")).toBe("engineering/runbooks");
  });

  test("turns links to published pages and folders into site routes", () => {
    expect(siteLinks('[ADR 007](007_monetization_engineering.md#context "ADR") [Home](../../README.md) [Runbooks](../../runbooks/)', adr, routes)).toBe(
      '[ADR 007](/engineering/adr/007_monetization_engineering#context "ADR") [Home](/engineering) [Runbooks](/engineering/runbooks)',
    );
    expect(siteLinks("[adr]: 007_monetization_engineering.md", adr, routes)).toBe("[adr]: /engineering/adr/007_monetization_engineering");
  });

  test("sends links to other repository files to the repository", () => {
    expect(siteLinks("[Local development](../../dev/local-development.md) ![Logo](../../assets/orbit-logo.png)", adr, routes)).toBe(
      `[Local development](${repoBlob}docs/dev/local-development.md) ![Logo](${repoRaw}docs/assets/orbit-logo.png)`,
    );
  });

  test("keeps absolute links, anchors, and code", () => {
    const markdown = "[a](https://example.com) [b](#terms) [c](/testing) `[d](missing.md)`\n\n```md\n[e](missing.md)\n```\n\n~~~md\n[f](missing.md)\n~~~";
    expect(siteLinks(markdown, adr, routes)).toBe(markdown);
  });

  test("rejects a link to a file that does not exist", () => {
    expect(() => siteLinks("[x](missing.md)", adr, routes)).toThrow(`${adr}: the link target missing.md does not exist`);
  });
});
