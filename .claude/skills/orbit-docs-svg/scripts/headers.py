# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
import argparse
import math
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from svgkit import (
    BLUE, GRAY, MUTED, ORANGE, PURPLE, RED, SUB, TEAL, TITLE,
    Canvas, arrow, curved, labelled, load_edges, load_nodes, trimmed,
)

SDLC_DOMAINS = [
    ("core", "Core", "project", ["Project", "Group", "User", "Note"]),
    ("plan", "Work items", "work-items", ["WorkItem", "Milestone", "Label"]),
    ("code_review", "Merge requests", "merge-request", ["MergeRequest", "MergeRequestDiff", "MergeRequestDiffFile"]),
    ("ci", "Pipelines", "pipeline", ["Pipeline", "Stage", "Job", "Runner", "Deployment", "Environment"]),
    ("packages", "Packages", "package", ["Package", "PackageFile", "Dependency"]),
    ("security", "Vulnerabilities", "shield", ["Vulnerability", "Finding", "SecurityScan", "VulnerabilityOccurrence", "VulnerabilityIdentifier", "VulnerabilityScanner"]),
    ("container_registry", "Container registry", "container-image", ["ContainerRepository"]),
    ("agents", "AI agents", "tanuki-ai", ["AgentSession"]),
]
SHORT_NAMES = {
    "MergeRequestDiff": "Diff", "MergeRequestDiffFile": "DiffFile",
    "VulnerabilityOccurrence": "Occurrence", "VulnerabilityIdentifier": "Identifier",
    "VulnerabilityScanner": "Scanner", "ContainerRepository": "Repository", "AgentSession": "Session",
}


def sdlc_graph(sprite, ontology_dir):
    colors, domains = load_nodes(ontology_dir)
    sdlc_nodes = {name for *_rest, entities in SDLC_DOMAINS for name in entities}
    edges = load_edges(ontology_dir, sdlc_nodes)

    canvas = Canvas(sprite)
    ring = SDLC_DOMAINS[1:]
    positions = {"core": (0, 0)}
    for index, (key, *_rest) in enumerate(ring):
        angle = math.radians(index * 360 / len(ring))
        positions[key] = (round(560 * math.sin(angle)), round(-300 * math.cos(angle)))

    for key, *_rest in ring:
        start, end = trimmed(positions["core"], positions[key], 70, 52)
        canvas.line(f"M{start[0]:.0f},{start[1]:.0f} L{end[0]:.0f},{end[1]:.0f}", width=2.5, opacity=0.55)
    related = {tuple(sorted((domains[a], domains[b]))) for a, b, _label in edges}
    for a, b in sorted(pair for pair in related if pair[0] != pair[1] and "core" not in pair):
        curved(canvas, positions[a], positions[b])

    for key, title, icon, entities in SDLC_DOMAINS:
        x, y = positions[key]
        radius = 64 if key == "core" else 48
        canvas.node(x, y, colors[entities[0]], icon, radius=radius, icon_size=52 if key == "core" else 40)
        canvas.text(x, y + radius + 30, title, size=27, weight=650, fill=TITLE)
        names = [SHORT_NAMES.get(name, name) for name in entities]
        for row, start in enumerate(range(0, len(names), 3)):
            canvas.text(x, y + radius + 64 + row * 26, " · ".join(names[start : start + 3]), size=19, weight=450, fill=SUB)
    return canvas.render((-780, -420, 780, 470), 720, "Orbit SDLC graph: domains")


def code_indexing(sprite, _ontology_dir):
    canvas = Canvas(sprite)
    push, task, repo, graph = (0, 0), (290, 0), (580, 0), (1160, 0)
    arrow(canvas, push, task)
    arrow(canvas, task, repo, 58, 66)
    for name, y, indexed in [("main", 0, True), ("feature/search", -175, False), ("fix/login", 175, False)]:
        branch = (870, y)
        arrow(canvas, repo, branch, 66, 52, dashed=not indexed, opacity=0.55 if indexed else 0.3)
        if indexed:
            arrow(canvas, branch, graph, 52, 70)
        canvas.node(*branch, TEAL if indexed else "#c4c3ca", "branch", radius=44, icon_size=36)
        canvas.text(branch[0], y + 72, name, size=23, weight=600, fill=TITLE if indexed else MUTED)
        canvas.text(branch[0], y + 100, "Default branch" if indexed else "Not indexed", size=18, weight=450, fill=SUB if indexed else MUTED)
    labelled(canvas, *push, "Push", "commit", ORANGE, ["Code lands on", "the default branch"])
    labelled(canvas, *task, "Task", "work-item-task", RED, ["GitLab records", "the event"])
    labelled(canvas, *repo, "Repository", "project", PURPLE, ["Downloaded", "from Gitaly"], radius=56)
    labelled(canvas, *graph, "Code graph", "orbit", BLUE, ["Written to", "ClickHouse"], radius=62)
    canvas.text(870, 330, "Other branches stay local with the Orbit CLI", size=19, weight=450, fill=MUTED, knockout=False)
    return canvas.render((-150, -260, 1310, 360), 720, "Orbit code indexing: from a push to the code graph")


def schema_evolution(sprite, _ontology_dir):
    canvas = Canvas(sprite)
    change, new, copied, reindexed, switched, retired = (0, 0), (290, 0), (580, -120), (580, 120), (870, 0), (1160, 0)
    arrow(canvas, change, new)
    arrow(canvas, new, copied, 56, 54)
    arrow(canvas, new, reindexed, 56, 54)
    arrow(canvas, copied, switched, 54, 58)
    arrow(canvas, reindexed, switched, 54, 58)
    arrow(canvas, switched, retired)
    labelled(canvas, *change, "Ontology change", "pencil", ORANGE, ["Bumps the schema", "to N+1"])
    labelled(canvas, *new, "Version N+1", "tag", PURPLE, ["Built next to N"])
    labelled(canvas, *copied, "Copied", "duplicate", TEAL, ["Unchanged", "tables"], radius=44)
    labelled(canvas, *reindexed, "Re-indexed", "retry", ORANGE, ["Changed", "tables"], radius=44)
    labelled(canvas, *switched, "Switched", "check-circle", BLUE, ["N+1 goes live,", "no downtime"])
    labelled(canvas, *retired, "Retired", "archive", GRAY, ["Version N is", "cleaned up"])

    axis_y, switch_x = 340, switched[0]
    left, right = -110, retired[0] + 110
    canvas.line(f"M{left},{axis_y} L{right},{axis_y}", arrow=True, width=2.5, opacity=0.5)
    for x0, x1, color, label in [(left, switch_x, GRAY, "N serves queries"), (switch_x, right, BLUE, "N+1 serves queries")]:
        canvas.under(f'<rect x="{x0}" y="{axis_y - 7}" width="{x1 - x0}" height="14" rx="7" fill="{color}" fill-opacity="0.85"/>')
        canvas.text((x0 + x1) / 2, axis_y + 36, label, size=21, weight=650, fill=color, knockout=False)
    canvas.over(f'<circle cx="{switch_x}" cy="{axis_y}" r="13" fill="{BLUE}" stroke="#ffffff" stroke-width="4"/>')
    canvas.line(f"M{switch_x},{58} L{switch_x},{axis_y - 16}", dashed=True, width=2, opacity=0.45)
    canvas.text(switch_x, axis_y - 40, "switch", size=19, weight=600, fill=BLUE, knockout=False)
    return canvas.render((-150, -200, retired[0] + 150, 400), 720, "Orbit schema evolution: copy, re-index, switch live")


HEADERS = {
    "sdlc-graph": sdlc_graph,
    "code-indexing": code_indexing,
    "schema-evolution": schema_evolution,
}


def main():
    parser = argparse.ArgumentParser(description="Build the header images of the Orbit architecture pages.")
    parser.add_argument("names", nargs="*", metavar="NAME", help=f"Headers to build: {', '.join(HEADERS)}. Default: all.")
    parser.add_argument("--icons", type=Path, default=os.environ.get("GITLAB_SVGS_ICONS"), help="Path to @gitlab/svgs dist/icons.svg")
    parser.add_argument("--ontology", type=Path, default=Path("config/ontology"))
    parser.add_argument("--out", type=Path, default=Path("docs/assets/architecture"))
    args = parser.parse_args()
    if not args.icons:
        parser.error("pass --icons or set GITLAB_SVGS_ICONS to the @gitlab/svgs icons.svg sprite")
    unknown = [name for name in args.names if name not in HEADERS]
    if unknown:
        parser.error(f"unknown header: {', '.join(unknown)}")

    sprite = args.icons.read_text()
    args.out.mkdir(parents=True, exist_ok=True)
    for name in args.names or HEADERS:
        target = args.out / f"{name}.svg"
        target.write_text(HEADERS[name](sprite, args.ontology))
        print(target)


main()
