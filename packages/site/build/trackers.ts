import { trackingRef } from "../src/roadmap/schema";

export type Health = "onTrack" | "needsAttention" | "atRisk";
export type Tracker = { state: "OPEN" | "CLOSED"; dueDate?: string; health?: Health };

type Widget = { type: string; dueDate?: string | null; healthStatus?: Health | null };
type WorkItem = { state: Tracker["state"]; confidential: boolean; widgets: Widget[] };

const QUERY = `query Tracker($path: ID!, $iid: String!) {
  namespace(fullPath: $path) {
    workItem(iid: $iid) {
      state
      confidential
      widgets {
        type
        ... on WorkItemWidgetStartAndDueDate { dueDate }
        ... on WorkItemWidgetHealthStatus { healthStatus }
      }
    }
  }
}`;

export async function fetchTracker(url: string, token: string, fetcher: typeof fetch = fetch): Promise<Tracker> {
  const ref = trackingRef(url);
  if (!ref) throw new Error(`tracking ${url} is not a GitLab work item URL`);
  const response = await fetcher(`${new URL(url).origin}/api/graphql`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ query: QUERY, variables: { path: ref.namespace, iid: ref.iid } }),
  });
  if (!response.ok) throw new Error(`tracking ${url}: GitLab returned ${response.status}`);
  const { data, errors } = (await response.json()) as { data?: { namespace?: { workItem?: WorkItem | null } | null }; errors?: { message: string }[] };
  if (errors?.length) throw new Error(`tracking ${url}: ${errors.map((error) => error.message).join("; ")}`);
  const item = data?.namespace?.workItem;
  if (!item) throw new Error(`tracking ${url} does not exist or the token cannot read it`);
  if (item.confidential) throw new Error(`tracking ${url} is confidential, and the public site cannot show it`);
  const dueDate = item.widgets.find((widget) => widget.dueDate)?.dueDate ?? undefined;
  const health = item.widgets.find((widget) => widget.healthStatus)?.healthStatus ?? undefined;
  return { state: item.state, ...(dueDate && { dueDate }), ...(health && { health }) };
}

export async function fetchTrackers(urls: string[], token: string | undefined, fetcher: typeof fetch = fetch): Promise<Record<string, Tracker>> {
  if (urls.length === 0) return {};
  if (!token) throw new Error("Reading roadmap trackers needs a GitLab token with the read_api scope. Set ORBIT_MARKDOWN_TOKEN in the environment.");
  const entries = await Promise.all([...new Set(urls)].map(async (url) => [url, await fetchTracker(url, token, fetcher)] as const));
  return Object.fromEntries(entries);
}
