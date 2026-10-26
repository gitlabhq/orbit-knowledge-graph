import math
import re
from pathlib import Path

import yaml

FONT = "'GitLab Sans', Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif"
LINE = "#8f8e96"
TEXT = "#7b7a82"
TITLE = "#6b6a72"
SUB = "#8f8e96"
MUTED = "#a8a7af"

ORANGE = "#FC6D26"
RED = "#E24329"
PURPLE = "#8B5CF6"
TEAL = "#14B8A6"
BLUE = "#3B82F6"
GRAY = "#94A3B8"


def load_nodes(ontology_dir: Path):
    colors, domains = {}, {}
    for path in (ontology_dir / "nodes").rglob("*.yaml"):
        doc = yaml.safe_load(path.read_text())
        name = doc.get("node_type")
        if name:
            domains[name] = path.parent.name
            colors[name] = (doc.get("style") or {}).get("color", "#6B7280")
    return colors, domains


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def load_edges(ontology_dir: Path, node_names):
    edges = set()
    for path in ontology_dir.rglob("*.yaml"):
        label = path.stem.upper() if "edges" in path.parts else None
        for item in _walk(yaml.safe_load(path.read_text())):
            if "from_node" in item and "to_node" in item:
                ends = (item["from_node"].get("type"), item["to_node"].get("type"), label)
            elif isinstance(item.get("from"), dict) and isinstance(item.get("to"), dict):
                ends = (item["from"].get("kind"), item["to"].get("kind"), item.get("label") or label)
            else:
                continue
            if all(isinstance(end, str) and end in node_names for end in ends[:2]) and ends[0] != ends[1]:
                edges.add(ends)
    return edges


def luminance(hex_color):
    channels = [int(hex_color[i : i + 2], 16) / 255 for i in (1, 3, 5)]
    linear = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def text_width(text, size):
    return len(text) * size * 0.56


def trimmed(a, b, start_gap, end_gap):
    (x1, y1), (x2, y2) = a, b
    length = math.hypot(x2 - x1, y2 - y1)
    ux, uy = (x2 - x1) / length, (y2 - y1) / length
    return (x1 + ux * start_gap, y1 + uy * start_gap), (x2 - ux * end_gap, y2 - uy * end_gap)


class Canvas:
    def __init__(self, sprite: str):
        self.sprite = sprite
        self.lines, self.nodes, self.labels, self.holes, self.icons = [], [], [], [], set()

    def node(self, x, y, color, icon, radius=34, icon_size=30):
        self.icons.add(icon)
        tone = "#1f1e24" if luminance(color) > 0.28 else "#ffffff"
        self.nodes.append(f'<circle cx="{x}" cy="{y}" r="{radius}" fill="{color}"/>')
        self.nodes.append(
            f'<use href="#i-{icon}" x="{x - icon_size / 2}" y="{y - icon_size / 2}" '
            f'width="{icon_size}" height="{icon_size}" style="fill:{tone}"/>'
        )

    def text(self, x, y, value, size=24, weight=500, fill=TEXT, knockout=True):
        self.labels.append(
            f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="middle" dominant-baseline="central">{value}</text>'
        )
        if knockout:
            width = text_width(value, size) + 14
            self.holes.append((x - width / 2, y - size * 0.75, width, size * 1.5))

    def line(self, d, arrow=False, dashed=False, width=2, opacity=0.5):
        extra = ' marker-end="url(#arrow)"' if arrow else ""
        dash = ' stroke-dasharray="7 7"' if dashed else ""
        self.lines.append(
            f'<path d="{d}" fill="none" stroke="{LINE}" stroke-opacity="{opacity}" stroke-width="{width}"{dash}{extra}/>'
        )

    def under(self, fragment):
        self.labels.insert(0, fragment)

    def over(self, fragment):
        self.labels.append(fragment)

    def render(self, box, display_width, title):
        x0, y0, x1, y1 = box
        width, height = x1 - x0, y1 - y0
        holes = "".join(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="black"/>' for x, y, w, h in self.holes)
        return "\n".join(
            [
                f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{x0} {y0} {width} {height}" '
                f'width="{display_width}" height="{round(display_width * height / width)}" font-family="{FONT}">',
                f"<title>{title}</title>",
                "<defs>",
                *[self._symbol(icon) for icon in sorted(self.icons)],
                f'<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{LINE}" fill-opacity="0.8"/></marker>',
                f'<mask id="knockout" maskUnits="userSpaceOnUse" x="{x0}" y="{y0}" width="{width}" height="{height}">'
                f'<rect x="{x0}" y="{y0}" width="{width}" height="{height}" fill="white"/>{holes}</mask>',
                "</defs>",
                f'<g mask="url(#knockout)">{"".join(self.lines)}</g>',
                *self.nodes,
                *self.labels,
                "</svg>",
            ]
        ) + "\n"

    def _symbol(self, icon):
        match = re.search(r'<symbol[^>]*id="' + re.escape(icon) + r'"[^>]*>.*?</symbol>', self.sprite, re.S)
        if not match:
            raise KeyError(f"icon {icon!r} is not in the @gitlab/svgs sprite")
        return re.sub(r'id="' + re.escape(icon) + '"', f'id="i-{icon}"', match.group(0))


def labelled(canvas, x, y, name, icon, color, lines, radius=48):
    canvas.node(x, y, color, icon, radius=radius, icon_size=round(radius * 0.83))
    canvas.text(x, y + radius + 30, name, size=27, weight=650, fill=TITLE)
    for row, line in enumerate(lines):
        canvas.text(x, y + radius + 64 + row * 26, line, size=19, weight=450, fill=SUB)


def arrow(canvas, a, b, start_gap=58, end_gap=62, dashed=False, opacity=0.55):
    start, end = trimmed(a, b, start_gap, end_gap)
    canvas.line(f"M{start[0]:.0f},{start[1]:.0f} L{end[0]:.0f},{end[1]:.0f}", arrow=True, dashed=dashed, width=2.5, opacity=opacity)


def curved(canvas, a, b, bend=1.55, start_gap=52, end_gap=52, arrow=False, dashed=True, opacity=0.45):
    (x1, y1), (x2, y2) = a, b
    cx, cy = (x1 + x2) / 2 * bend, (y1 + y2) / 2 * bend
    start, _ = trimmed(a, (cx, cy), start_gap, 0)
    _, end = trimmed((cx, cy), b, 0, end_gap)
    canvas.line(f"M{start[0]:.0f},{start[1]:.0f} Q{cx:.0f},{cy:.0f} {end[0]:.0f},{end[1]:.0f}", arrow=arrow, dashed=dashed, width=2, opacity=opacity)
