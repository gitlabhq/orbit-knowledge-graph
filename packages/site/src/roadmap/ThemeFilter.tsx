import { chip, Icon, SEGMENTS } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { themes } from "./model";

// The router marks the chip that matches the whole URL, search included, with aria-current.
export const EXACT = { exact: true, includeSearch: true };

export function ThemeFilter({ theme }: { theme?: string }) {
  return (
    <div role="group" aria-label="Filter by theme" className={SEGMENTS}>
      <Link to="." search={(prev) => ({ ...prev, theme: undefined })} activeOptions={EXACT} className={chip(!theme)}>
        All themes
      </Link>
      {themes.map((item) => (
        <Link key={item.id} to="." search={(prev) => ({ ...prev, theme: item.id })} activeOptions={EXACT} className={chip(theme === item.id)}>
          <Icon name={item.icon} size={12} />
          {item.title}
        </Link>
      ))}
    </div>
  );
}
