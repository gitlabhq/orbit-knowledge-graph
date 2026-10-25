import { Avatar, findPage, Icon, pageHeadOf, SiteLink } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { mission, people, principles, profileUrl, type TeamMember } from "../team/team";

export const Route = createFileRoute("/team")({
  head: () => pageHeadOf(findPage("team")),
  component: TeamPage,
});

const PRINCIPLE_ICONS = ["code", "users", "tanuki-ai", "rocket"];

function Hero() {
  return (
    <section className="border-b border-border">
      <div className="page-container py-10">
        <h1 className="m-0 text-3xl font-semibold text-default">Meet the team</h1>
        <p className="mt-2 mb-0 max-w-2xl text-base text-subtle">We work like a startup inside GitLab.{mission && ` Our mission: ${mission}`}</p>
      </div>
    </section>
  );
}

function ProfileLink({ member }: { member: TeamMember }) {
  return (
    <SiteLink href={profileUrl(member.username)} className="inline-flex items-center gap-1 text-sm text-link no-underline hover:underline">
      @{member.username}
    </SiteLink>
  );
}

function MemberCard({ member }: { member: TeamMember }) {
  return (
    <article className="card flex h-full flex-col p-6">
      <div className="flex items-center gap-4">
        <Avatar name={member.name} src={member.avatar} size={64} />
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
  return (
    <>
      <Hero />
      <div className="page-container flex flex-col gap-12 py-10">
        {people.length > 0 && (
          <section aria-labelledby="team">
            <h2 id="team" className="m-0 text-xl font-semibold text-default">
              The team
            </h2>
            <p className="mt-1 mb-6 text-subtle">Engineering, product, and design work as one group.</p>
            <ul className="m-0 grid list-none gap-4 p-0 sm:grid-cols-2 lg:grid-cols-4">
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
            <ul className="m-0 mt-6 grid list-none gap-4 p-0 sm:grid-cols-2 lg:grid-cols-4">
              {principles.map((principle, index) => (
                <li key={principle.title} className="rounded-[1rem] bg-surface-subtle p-5">
                  <span className="flex size-9 items-center justify-center rounded-control bg-page text-default shadow-sm">
                    <Icon name={PRINCIPLE_ICONS[index % PRINCIPLE_ICONS.length]} size={18} />
                  </span>
                  <h3 className="mt-4 mb-0 text-base font-semibold text-default">{principle.title}</h3>
                  <p className="mt-1 mb-0 text-sm text-subtle">{principle.text}</p>
                </li>
              ))}
            </ul>
          </section>
        )}
        <section className="card flex flex-col items-start justify-between gap-6 p-8 md:flex-row md:items-center">
          <div>
            <h2 className="m-0 text-xl font-semibold text-default">Build with us</h2>
            <p className="mt-1 mb-0 max-w-xl text-subtle">The Orbit source code is public. You can read the code, open an issue, or follow the blog.</p>
          </div>
          <div className="flex flex-wrap gap-3">
            <SiteLink
              href="https://gitlab.com/gitlab-org/orbit/knowledge-graph"
              className="inline-flex items-center gap-2 rounded-control bg-[var(--gl-button-confirm-primary-background-color-default)] px-5 py-2.5 text-sm font-semibold text-[var(--gl-button-confirm-primary-foreground-color-default)] no-underline hover:bg-[var(--gl-button-confirm-primary-background-color-hover)]"
            >
              View the source
              <Icon name="external-link" size={12} />
            </SiteLink>
            <Link to="/content" className="rounded-control border border-border-strong bg-page px-5 py-2.5 text-sm font-semibold text-default no-underline hover:bg-hover">
              Read the content
            </Link>
          </div>
        </section>
      </div>
    </>
  );
}
