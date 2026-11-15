import { createFileRoute, redirect } from "@tanstack/react-router";

// Articles and videos are part of the About page now; old links keep working.
export const Route = createFileRoute("/content/")({
  beforeLoad: () => {
    throw redirect({ to: "/about", hash: "related", replace: true });
  },
});
