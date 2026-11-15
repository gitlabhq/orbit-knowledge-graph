import data from "@data/about/demos.yaml";
import type { VideoSource } from "@gitlab-org/orbit-site";

export type DemoVideo = VideoSource & { description?: string; featured?: boolean };
export type DemoLink = { href: string; title: string; description: string; image?: string; meta: string };

const demos = data as { videos: DemoVideo[]; links: DemoLink[] };

export const featured = demos.videos.find((video) => video.featured);
export const videos = demos.videos.filter((video) => !video.featured);
export const links = demos.links;
