import { describe, expect, test } from "bun:test";
import { describeContent } from "@gitlab-org/orbit-site/testing";
import { HOME } from "../src/sections";
import { contentDir, repoBlob, siteLinks, siteMeta, sourceFiles, syncContent } from "./content";

syncContent();
describeContent(contentDir, { routes: { "": HOME }, meta: siteMeta });

describe("site links", () => {
  const routes = sourceFiles();
  const adr = "docs/pages/engineering/adr/021_entity_context_expansion.md";
  const repoRaw = repoBlob.replace("/-/blob/", "/-/raw/");

  test("maps every section README to the section route", () => {
    expect(routes.get("docs/pages/engineering/README.md")).toBe("engineering");
    expect(routes.get("docs/pages/engineering/adr/README.md")).toBe("engineering/adr");
    expect(routes.get("docs/pages/engineering/testing/README.md")).toBe("engineering/testing");
  });

  test("turns links to published pages and folders into site routes", () => {
    expect(siteLinks('[ADR 003](003_api_design.md#context "ADR") [Home](../README.md) [Testing](../testing/)', adr, routes)).toBe(
      '[ADR 003](/engineering/adr/003_api_design#context "ADR") [Home](/engineering/) [Testing](/engineering/testing/)',
    );
    expect(siteLinks("[adr]: 003_api_design.md", adr, routes)).toBe("[adr]: /engineering/adr/003_api_design");
  });

  test("sends links to other repository files to the repository", () => {
    expect(siteLinks("[Fuzzing](../../../../crates/fuzz/README.md) ![Logo](../../../assets/orbit-logo.png)", adr, routes)).toBe(
      `[Fuzzing](${repoBlob}crates/fuzz/README.md) ![Logo](${repoRaw}docs/assets/orbit-logo.png)`,
    );
  });

  test("keeps off-site pages and sections out of the site, and links to them in the repository", () => {
    expect(routes.has("docs/pages/engineering/adr/007_monetization_engineering.md")).toBe(false);
    expect(routes.has("docs/pages/engineering/runbooks/README.md")).toBe(false);
    expect(siteLinks("[ADR 007](007_monetization_engineering.md) [Runbooks](../runbooks/)", adr, routes)).toBe(
      `[ADR 007](${repoBlob}docs/pages/engineering/adr/007_monetization_engineering.md) [Runbooks](${repoBlob}docs/pages/engineering/runbooks/)`,
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
