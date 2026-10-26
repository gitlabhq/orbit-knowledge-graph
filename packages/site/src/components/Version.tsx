import { SiteLink } from "@gitlab-org/orbit-site";

const VERSION = import.meta.env.VITE_ORBIT_VERSION;
const RELEASE = import.meta.env.VITE_ORBIT_RELEASE;
const BADGE = "gl-badge badge-neutral hidden whitespace-nowrap no-underline sm:inline-flex";

export function Version() {
  if (RELEASE) return <SiteLink href={`https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases/${RELEASE}`} className={BADGE}>{RELEASE}</SiteLink>;
  return <span className={BADGE} title="This build is not a release.">v{VERSION} preview</span>;
}
