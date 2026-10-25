import { cn } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { themes } from "./model";

const CHIP = "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-sm no-underline transition-colors";
const ACTIVE = { className: "border-transparent bg-[var(--gl-action-selected-background-color-default)] text-[var(--gl-action-selected-foreground-color-default)]" };
const INACTIVE = { className: "border-border-strong bg-page text-default hover:bg-hover" };
export const EXACT = { exact: true, includeSearch: true };

export function ThemeFilter() {
  return (
    <div role="group" aria-label="Filter by theme" className="flex flex-wrap gap-2">
      <Link to="." search={(prev) => ({ ...prev, theme: undefined })} activeOptions={EXACT} activeProps={ACTIVE} inactiveProps={INACTIVE} className={CHIP}>
        All themes
      </Link>
      {themes.map((theme) => (
        <Link
          key={theme.id}
          to="."
          search={(prev) => ({ ...prev, theme: theme.id })}
          activeOptions={EXACT}
          activeProps={ACTIVE}
          inactiveProps={INACTIVE}
          className={cn(CHIP, `tone-${theme.id}`)}
        >
          <span className="tone-dot" />
          {theme.title}
        </Link>
      ))}
    </div>
  );
}
