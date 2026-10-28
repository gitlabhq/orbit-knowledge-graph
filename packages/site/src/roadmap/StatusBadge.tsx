import { Badge, type BadgeIconSize, type Tone } from "@gitlab-org/orbit-site";
import type { Status } from "./schema";

const STATUS: Record<Status, { label: string; icon: string; tone: Tone }> = {
  shipped: { label: "Shipped", icon: "status-success", tone: "success" },
  "in-progress": { label: "In progress", icon: "status-running", tone: "info" },
  planned: { label: "Planned", icon: "status-waiting", tone: "neutral" },
  exploring: { label: "Exploring", icon: "status-neutral", tone: "neutral" },
};

export const statusLabel = (status: Status) => STATUS[status].label;

export const statusIcon = (status: Status) => STATUS[status].icon;

export function StatusBadge({ status, iconSize, className }: { status: Status; iconSize?: BadgeIconSize; className?: string }) {
  const { label, icon, tone } = STATUS[status];
  return (
    <Badge tone={tone} icon={icon} iconSize={iconSize} className={className}>
      {label}
    </Badge>
  );
}
