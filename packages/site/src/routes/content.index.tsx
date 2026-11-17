import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/content/")({
  beforeLoad: () => {
    throw redirect({ to: "/about", hash: "related", replace: true });
  },
});
