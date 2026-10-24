import { cn, SiteLink } from "@gitlab-org/orbit-site";

const VERSION = import.meta.env.VITE_ORBIT_VERSION;
const RELEASE = import.meta.env.VITE_ORBIT_RELEASE;
const BADGE = "rounded-full border border-border-strong px-2 py-0.5 text-xs text-subtle no-underline";

export function Version() {
  if (RELEASE) return <SiteLink href={`https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases/${RELEASE}`} className={cn(BADGE, "hover:text-default")}>{RELEASE}</SiteLink>;
  return <span className={BADGE} title="This build is not a release.">v{VERSION} preview</span>;
}
