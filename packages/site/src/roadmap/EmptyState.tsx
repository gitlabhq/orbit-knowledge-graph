import { Link } from "@tanstack/react-router";

export function EmptyState({ message, reset = true }: { message: string; reset?: boolean }) {
  return (
    <div className="rounded-[1rem] border border-dashed border-border-strong px-6 py-12 text-center">
      <p className="m-0 text-subtle">{message}</p>
      {reset && (
        <Link to="." search={(prev) => ({ ...prev, theme: undefined })} className="mt-4 inline-block rounded-control border border-border-strong bg-page px-4 py-2 text-sm font-semibold text-default no-underline hover:bg-hover">
          Show all themes
        </Link>
      )}
    </div>
  );
}
