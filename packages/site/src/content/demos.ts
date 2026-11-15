import data from "@data/content/demos.yaml";
import type { VideoSource } from "@gitlab-org/orbit-site";

export type DemoVideo = VideoSource & { description?: string };
export type DemoLink = { href: string; title: string; description: string; image?: string; meta: string };

const demos = data as { videos: DemoVideo[]; links: DemoLink[] };

export const videos = demos.videos;
export const links = demos.links;
