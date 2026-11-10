import data from "@data/team/team.yaml";

export type TeamMember = { username: string; name: string; title: string; bio: string; avatar?: string };

const team = data as { mission: string; people: TeamMember[]; principles: { title: string; text: string }[] };

export const mission = team.mission;
export const people = team.people;
export const principles = team.principles;

export const profileUrl = (username: string) => `https://gitlab.com/${username}`;
