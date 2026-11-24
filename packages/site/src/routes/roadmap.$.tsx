import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/roadmap/$")({
  beforeLoad: () => {
    throw redirect({ to: "/next", replace: true });
  },
});
