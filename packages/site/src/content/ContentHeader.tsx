export function ContentHeader({ title, description }: { title: string; description?: string }) {
  return (
    <header className="flex flex-wrap items-baseline gap-x-3 gap-y-1 border-b border-border pt-8 pb-4 mb-6">
      <h1 className="m-0 text-2xl font-bold text-default">{title}</h1>
      {description && <p className="m-0 text-sm text-subtle">{description}</p>}
    </header>
  );
}
