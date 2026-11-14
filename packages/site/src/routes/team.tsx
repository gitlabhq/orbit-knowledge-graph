import { Avatar, cn, findPage, Icon, pageHeadOf, SiteLink, segment } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Frame } from "../components/Frame";
import { PageFooter } from "../components/PageFooter";
import { mission, people, principles, profileUrl, type TeamMember } from "../team/team";

export const Route = createFileRoute("/team")({
  head: () => pageHeadOf(findPage("team")),
  component: TeamPage,
});

const PRINCIPLE_ICONS = ["code", "users", "tanuki-ai", "rocket"];

function ProfileLink({ member }: { member: TeamMember }) {
  return (
    <SiteLink href={profileUrl(member.username)} className="inline-flex items-center gap-1 text-sm text-link no-underline hover:underline">
      @{member.username}
    </SiteLink>
  );
}

function MemberCard({ member }: { member: TeamMember }) {
  return (
    <article className="card tile-quiet flex h-full flex-col px-5 pt-4 pb-5">
      <div className="flex items-center gap-4">
        <Avatar name={member.name} src={member.avatar} size={48} />
        <div className="min-w-0">
          <h3 className="m-0 text-base font-semibold text-default">{member.name}</h3>
          <p className="mt-0.5 mb-0 text-sm text-subtle">{member.title}</p>
        </div>
      </div>
      <p className="mt-4 mb-0 text-sm leading-relaxed text-default">{member.bio}</p>
      <div className="mt-auto pt-4">
        <ProfileLink member={member} />
      </div>
    </article>
  );
}

function TeamPage() {
  const page = findPage("team");
  return (
    <Frame
      title="Meet the team"
      description={`We work like a startup inside GitLab.${mission ? ` Our mission: ${mission}` : ""}`}
      footer={page && <PageFooter page={page} />}
    >
      <div className="flex flex-col gap-12">
        {people.length > 0 && (
          <section aria-labelledby="team">
            <h2 id="team" className="m-0 text-xl font-semibold text-default">
              The team
            </h2>
            <p className="mt-1 mb-6 text-subtle">Engineering, product, and design work as one group.</p>
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
            <h2 id="how-we-work" className="m-0 text-xl font-semibold text-default">
              How we work
            </h2>
            <ul className="m-0 mt-4 grid list-none gap-3 p-0 sm:grid-cols-2">
              {principles.map((principle, index) => (
                <li key={principle.title} className="card tile-quiet px-5 pt-4 pb-5">
                  <Icon name={PRINCIPLE_ICONS[index % PRINCIPLE_ICONS.length]} size={16} className="tile-icon" />
                  <h3 className="mt-3 mb-0 text-base font-semibold text-default">{principle.title}</h3>
                  <p className="mt-1 mb-0 text-sm text-subtle">{principle.text}</p>
                </li>
              ))}
            </ul>
          </section>
        )}
        <section className="card tile-quiet flex flex-col items-start justify-between gap-6 p-6 md:flex-row md:items-center">
          <div>
            <h2 className="m-0 text-xl font-semibold text-default">Build with us</h2>
            <p className="mt-1 mb-0 max-w-xl text-subtle">The Orbit source code is public. You can read the code or open an issue.</p>
          </div>
          <div className="flex flex-wrap gap-2">
            <SiteLink
              href="https://gitlab.com/gitlab-org/orbit/knowledge-graph"
              className={cn(segment(false, true), "border border-border")}
            >
              View the source
              <Icon name="external-link" size={12} />
            </SiteLink>
            <Link to="/content" className={segment(false, true)}>
              Read the content
            </Link>
          </div>
        </section>
      </div>
    </Frame>
  );
}
