import { cn, embedUrl, Icon, providerName, SiteLink, thumbnailUrl, watchUrl } from "@gitlab-org/orbit-site";
import { useState } from "react";
import type { DemoVideo } from "../content/demos";

export function Video({ video, featured = false }: { video: DemoVideo; featured?: boolean }) {
  const [playing, setPlaying] = useState(false);
  const thumbnail = thumbnailUrl(video);
  return (
    <figure className={cn("card m-0 flex h-full flex-col overflow-hidden", featured && "lg:grid lg:grid-cols-[1.6fr_1fr]")}>
      <div className="relative aspect-video bg-black">
        {playing ? (
          <iframe
            src={embedUrl(video)}
            title={video.title}
            allow="autoplay; fullscreen; picture-in-picture; encrypted-media"
            allowFullScreen
            className="absolute inset-0 size-full border-0"
          />
        ) : (
          <button type="button" onClick={() => setPlaying(true)} aria-label={`Play ${video.title}`} className="group absolute inset-0 size-full cursor-pointer">
            {thumbnail && <img src={thumbnail} alt="" loading="lazy" className="size-full object-cover opacity-90 transition-opacity group-hover:opacity-100" />}
            <span className="absolute inset-0 m-auto flex size-14 items-center justify-center rounded-full bg-black/70 text-white backdrop-blur transition-transform group-hover:scale-110">
              <Icon name="play" size={22} />
            </span>
          </button>
        )}
      </div>
      <figcaption className={cn("flex flex-1 flex-col gap-1.5 p-5", featured && "lg:justify-center lg:p-8")}>
        <span className={cn("font-semibold text-default", featured ? "text-2xl tracking-[-0.01em]" : "text-base")}>{video.title}</span>
        {video.description && <span className="text-sm text-subtle">{video.description}</span>}
        <SiteLink href={watchUrl(video)} className="mt-auto inline-flex items-center gap-1 pt-2 text-xs text-subtle no-underline hover:text-default hover:underline">
          Open on {providerName(video)}
          <Icon name="external-link" size={12} />
        </SiteLink>
      </figcaption>
    </figure>
  );
}
