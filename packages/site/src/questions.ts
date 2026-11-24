import { HOME, USER_DOCS } from "./sections";

export type Question = { question: string; answer: string; link?: { label: string; href: string } };

export const QUESTIONS: Question[] = [
  {
    question: "What is GitLab Orbit?",
    answer: "GitLab Orbit is a context layer for all your AI agents. It connects your code, merge requests, pipelines, work items, and vulnerabilities in one graph that your agents can query.",
  },
  {
    question: "What does GitLab Orbit need?",
    answer: "The graph of your group needs GitLab Premium or Ultimate with GitLab Orbit purchased.",
    link: { label: "GitLab Orbit product page", href: "https://about.gitlab.com/gitlab-orbit/" },
  },
  {
    question: "Which agents work with GitLab Orbit?",
    answer: "GitLab Orbit is built into GitLab Duo. Other agents connect with the CLI or MCP: glab orbit setup sets up Claude Code, Codex, GitLab Duo, OpenCode, or Pi.",
  },
  {
    question: "Which data can my agent see?",
    answer: "Each answer shows only the data that your GitLab role lets you see. Orbit stores no permission data, and the graph is read-only.",
  },
  {
    question: "Where are the docs?",
    answer: "To use GitLab Orbit, see the GitLab Orbit documentation. To see how Orbit is built, or to contribute, see the developer docs on this site.",
    link: { label: "GitLab Orbit documentation", href: USER_DOCS.href },
  },
  {
    question: "What is GitLab Orbit building next?",
    answer: "Code search across every repository and branch, and data from your other tools.",
    link: { label: "Orbit Next", href: "/next" },
  },
];

export const questionsMarkdown = () =>
  QUESTIONS.map(({ question, answer, link }) => `### ${question}\n\n${answer}${link ? ` [${link.label}](${link.href.startsWith("/") ? `https://gitlaborbit.dev${link.href}` : link.href})` : ""}`).join("\n\n");

export const questionsJsonLd = () => ({
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: QUESTIONS.map(({ question, answer }) => ({ "@type": "Question", name: question, acceptedAnswer: { "@type": "Answer", text: answer } })),
  about: { "@type": "SoftwareApplication", name: HOME.title },
});
