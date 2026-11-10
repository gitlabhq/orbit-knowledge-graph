import { Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { themes } from "./model";

const CHIP = "gl-button btn-sm no-underline";
const ACTIVE = { className: "selected" };
export const EXACT = { exact: true, includeSearch: true };

export function ThemeFilter() {
  return (
    <div role="group" aria-label="Filter by theme" className="flex flex-wrap gap-2">
      <Link to="." search={(prev) => ({ ...prev, theme: undefined })} activeOptions={EXACT} activeProps={ACTIVE} className={CHIP}>
        All themes
      </Link>
      {themes.map((theme) => (
        <Link
          key={theme.id}
          to="."
          search={(prev) => ({ ...prev, theme: theme.id })}
          activeOptions={EXACT}
          activeProps={ACTIVE}
          className={CHIP}
        >
          <Icon name={theme.icon} size={16} />
          {theme.title}
        </Link>
      ))}
    </div>
  );
}
