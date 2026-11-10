import { describe, expect, test } from "bun:test";
import { fetchTracker, fetchTrackers } from "./trackers";

const URL = "https://gitlab.com/groups/gitlab-org/orbit/-/work_items/17";

const reply = (workItem: unknown) => (async () => new Response(JSON.stringify({ data: { namespace: { workItem } } }))) as unknown as typeof fetch;

describe("roadmap trackers", () => {
  test("reads the state, due date, and health of a work item", async () => {
    const widgets = [{ type: "HEALTH_STATUS", healthStatus: "atRisk" }, { type: "START_AND_DUE_DATE", dueDate: "2026-10-31" }];
    expect(await fetchTracker(URL, "t", reply({ state: "OPEN", confidential: false, widgets }))).toEqual({ state: "OPEN", dueDate: "2026-10-31", health: "atRisk" });
    expect(await fetchTracker(URL, "t", reply({ state: "CLOSED", confidential: false, widgets: [] }))).toEqual({ state: "CLOSED" });
  });

  test("rejects a confidential or unreadable work item", async () => {
    await expect(fetchTracker(URL, "t", reply({ state: "OPEN", confidential: true, widgets: [] }))).rejects.toThrow("is confidential");
    await expect(fetchTracker(URL, "t", reply(null))).rejects.toThrow("does not exist or the token cannot read it");
  });

  test("needs a token only when an item has a tracker", async () => {
    expect(await fetchTrackers([], undefined)).toEqual({});
    await expect(fetchTrackers([URL], undefined)).rejects.toThrow("Set ORBIT_MARKDOWN_TOKEN");
  });
});
