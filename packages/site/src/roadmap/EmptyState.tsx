import { cn, segment } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";

export function EmptyState({ message, reset = true }: { message: string; reset?: boolean }) {
  return (
    <div className="rounded-[var(--radius-card)] border border-dashed border-[var(--tile-border)] px-6 py-12 text-center">
      <p className="m-0 text-subtle">{message}</p>
      {reset && (
        <Link to="." search={(prev) => ({ ...prev, theme: undefined })} className={cn(segment(false, true), "mt-4 border border-border")}>
          Show all themes
        </Link>
      )}
    </div>
  );
}
