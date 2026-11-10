import { SiteLink } from "@gitlab-org/orbit-site";
import { useState } from "react";
import type { DemoLink } from "../content/demos";

export function LinkCard({ link }: { link: DemoLink }) {
  const [showImage, setShowImage] = useState(Boolean(link.image));
  return (
    <SiteLink href={link.href} className="card flex h-full overflow-hidden no-underline">
      {showImage && <img src={link.image} alt="" loading="lazy" onError={() => setShowImage(false)} className="hidden w-44 shrink-0 object-cover sm:block" />}
      <span className="flex min-w-0 flex-col gap-1 p-4">
        <span className="text-xs font-medium text-subtle">{link.meta}</span>
        <span className="text-base font-semibold text-default">{link.title}</span>
        <span className="line-clamp-2 text-sm text-subtle">{link.description}</span>
      </span>
    </SiteLink>
  );
}
