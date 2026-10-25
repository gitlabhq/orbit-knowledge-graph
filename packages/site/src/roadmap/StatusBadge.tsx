import { Badge, type Tone } from "@gitlab-org/orbit-site";
import type { Status } from "./schema";

const STATUS: Record<Status, { label: string; icon: string; tone: Tone }> = {
  shipped: { label: "Shipped", icon: "check-circle-filled", tone: "success" },
  "in-progress": { label: "In progress", icon: "status-running", tone: "info" },
  planned: { label: "Planned", icon: "status-scheduled", tone: "neutral" },
  exploring: { label: "Exploring", icon: "bulb", tone: "brand" },
};

export const statusLabel = (status: Status) => STATUS[status].label;

export const statusIcon = (status: Status) => STATUS[status].icon;

export function StatusBadge({ status, className }: { status: Status; className?: string }) {
  const { label, icon, tone } = STATUS[status];
  return (
    <Badge tone={tone} icon={icon} className={className}>
      {label}
    </Badge>
  );
}
