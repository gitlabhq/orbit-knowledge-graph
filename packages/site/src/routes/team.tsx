import { Avatar, cn, findPage, Icon, PageFrame, pageHeadOf, segment, SiteLink } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { Sidebar } from "../components/Sidebar";
import { mission, people, principles, profileUrl, type TeamMember } from "../team/team";

export const Route = createFileRoute("/team")({
  head: () => pageHeadOf(findPage("team")),
  component: TeamPage,
});

const PRINCIPLE_ICONS = ["code", "users", "tanuki-ai", "rocket"];

function MemberCard({ member }: { member: TeamMember }) {
  return (
    <article className="card glass group relative flex h-full items-center gap-3.5 px-4 py-3.5">
      <SiteLink
        href={profileUrl(member.username)}
        aria-label={`@${member.username} on GitLab (opens in a new tab)`}
        className="group/avatar relative z-10 shrink-0 rounded-full no-underline"
      >
        <Avatar name={member.name} src={member.avatar} size={44} />
        <span
          aria-hidden="true"
          className="pointer-events-none absolute top-full left-1/2 z-10 mt-2 -translate-x-1/2 rounded-[var(--gl-nav-item-border-radius)] bg-[#28272d] px-2 py-1 text-xs whitespace-nowrap text-white opacity-0 transition-opacity group-hover/avatar:opacity-100 group-focus-visible/avatar:opacity-100 dark:bg-[#ececef] dark:text-[#18171d]"
        >
          @{member.username}
        </span>
      </SiteLink>
      <div className="min-w-0">
        <h3 className="m-0 text-[0.9375rem] font-semibold text-default">
          <SiteLink href={profileUrl(member.username)} aria-label={`${member.name} on GitLab (opens in a new tab)`} className="text-default no-underline after:absolute after:inset-0 after:content-['']">
            {member.name}
          </SiteLink>
        </h3>
        <p className="m-0 text-[0.8125rem] text-default">{member.title}</p>
        <p className="mt-1 mb-0 text-[0.8125rem] leading-snug text-subtle">{member.bio}</p>
      </div>
    </article>
  );
}

function TeamPage() {
  const page = findPage("team");
  return (
    <PageFrame
      path="team"
      nav={<Sidebar />}
      title="Meet the team"
      description={mission}
      footer={page && <PageFooter page={page} />}
    >
      <div className="flex flex-col gap-10">
        {people.length > 0 && (
          <section aria-label="The team" className="page-glow page-glow-strong">
            <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-2 lg:grid-cols-3">
              {people.map((member) => (
                <li key={member.username}>
                  <MemberCard member={member} />
                </li>
              ))}
            </ul>
          </section>
        )}
        {principles.length > 0 && (
          <section aria-labelledby="how-we-work">
            <h2 id="how-we-work" className="m-0 mb-4 text-xl font-semibold text-default">
              How we work
            </h2>
            <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-2 lg:grid-cols-4">
              {principles.map((principle, index) => (
                <li key={principle.title} className="card glass px-4 py-3.5">
                  <span className="flex items-center gap-2">
                    <Icon name={PRINCIPLE_ICONS[index % PRINCIPLE_ICONS.length]} size={16} className="tile-icon shrink-0" />
                    <h3 className="m-0 text-[0.9375rem] font-semibold text-default">{principle.title}</h3>
                  </span>
                  <p className="mt-1.5 mb-0 text-[0.8125rem] leading-snug text-subtle">{principle.text}</p>
                </li>
              ))}
            </ul>
          </section>
        )}
        <section className="card glass flex flex-col items-start justify-between gap-5 px-6 py-5 md:flex-row md:items-center">
          <div>
            <h2 className="m-0 text-lg font-semibold text-default">Build with us</h2>
            <p className="mt-1 mb-0 text-sm text-subtle">The code is public. Read it or open an issue.</p>
          </div>
          <div className="flex flex-wrap gap-2">
            <SiteLink
              href="https://gitlab.com/gitlab-org/orbit/knowledge-graph"
              className={cn(segment(false, true), "border border-border")}
            >
              View the source
              <Icon name="external-link" size={12} />
            </SiteLink>
            <Link to="/about" className={segment(false, true)}>
              See what's new
            </Link>
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
