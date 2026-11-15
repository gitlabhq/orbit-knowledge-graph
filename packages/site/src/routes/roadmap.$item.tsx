import { createFileRoute, redirect } from "@tanstack/react-router";

// Roadmap items have no pages of their own now; old links go to the roadmap.
export const Route = createFileRoute("/roadmap/$item")({
  beforeLoad: () => {
    throw redirect({ to: "/about", hash: "roadmap", replace: true });
  },
});
