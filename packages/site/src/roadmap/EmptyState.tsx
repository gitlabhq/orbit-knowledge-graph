import { Link } from "@tanstack/react-router";

export function EmptyState({ message, reset = true }: { message: string; reset?: boolean }) {
  return (
    <div className="rounded-card border border-dashed border-border-strong px-6 py-12 text-center">
      <p className="m-0 text-subtle">{message}</p>
      {reset && (
        <Link to="." search={(prev) => ({ ...prev, theme: undefined })} className="gl-button mt-4 no-underline">
          Show all themes
        </Link>
      )}
    </div>
  );
}
