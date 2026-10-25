import { describe, expect, test } from "bun:test";
import { itemFields, trackingLabel } from "../src/roadmap/schema";

const item = { description: "D", theme: "agents", status: "planned", start: "FY27 Q4" };

describe("roadmap tracking", () => {
  test("labels group and project work items by their reference", () => {
    expect(trackingLabel("https://gitlab.com/groups/gitlab-org/orbit/-/epics/17")).toBe("orbit&17");
    expect(trackingLabel("https://gitlab.com/groups/gitlab-org/orbit/-/work_items/21190")).toBe("orbit&21190");
    expect(trackingLabel("https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1075")).toBe("knowledge-graph#1075");
    expect(trackingLabel("https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1421")).toBe("knowledge-graph#1421");
    expect(trackingLabel("https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/milestones/3")).toBe("Tracking work item");
  });

  test("accepts only gitlab.com tracking URLs", () => {
    expect(itemFields({ ...item, tracking: "https://gitlab.com/groups/gitlab-org/orbit/-/epics/17" }, ["agents"]).tracking).toBe("https://gitlab.com/groups/gitlab-org/orbit/-/epics/17");
    expect(() => itemFields({ ...item, tracking: "https://example.com/1" }, ["agents"])).toThrow("tracking must be a https://gitlab.com/ URL");
  });
});
