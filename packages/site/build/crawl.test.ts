import { describe, expect, test } from "bun:test";
import { CORPUS, LATENCY } from "../src/next/searches";
import { QUESTIONS } from "../src/questions";
import { CRAWL } from "./crawl";

const home = CRAWL[""]!.markdown!;
const next = CRAWL.next!.markdown!;

describe("crawl text", () => {
  test("Orbit Next quotes the scale and latency the page shows", () => {
    for (const stat of CORPUS) expect(next).toContain(stat.value);
    for (const row of LATENCY) expect(next).toContain(`about ${row.ms} ms`);
  });

  test("the home page lists every common question, with its answer", () => {
    for (const { question, answer } of QUESTIONS) {
      expect(home).toContain(`### ${question}`);
      expect(home).toContain(answer);
    }
  });

  test("Orbit Next keeps the forward-looking notice", () => {
    expect(next).toContain("may be subject to change or delay");
  });

  test("example references sit in code spans, so GitLab does not link them", () => {
    for (const markdown of [home, next]) {
      expect(markdown.replace(/```[\s\S]*?```/g, "").replace(/`[^`]*`/g, "")).not.toMatch(/(?<![\w&])(?:[\w.-]+(?:\/[\w.-]+)*)?[!#&]\d+\b/m);
    }
  });
});
