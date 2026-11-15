import { createFileRoute, redirect } from "@tanstack/react-router";

// The roadmap is part of the About page now; old links keep working.
export const Route = createFileRoute("/roadmap/")({
  validateSearch: (search: Record<string, unknown>): { theme?: string } => ({
    theme: typeof search.theme === "string" ? search.theme : undefined,
  }),
  beforeLoad: ({ search }) => {
    throw redirect({ to: "/about", search, hash: "roadmap", replace: true });
  },
});
