# /// script
# requires-python = ">=3.11"
# ///
import argparse
import os
import re
import subprocess
from pathlib import Path

CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
BACKGROUNDS = {"light": "#ffffff", "dark": "#18171d"}

PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>{title}</title>
<style>
:root{{--bg:#ffffff;--fg:#28272d;--muted:#626168;--frame:#ececef}}
body.dark{{--bg:#18171d;--fg:#ececef;--muted:#a4a3a8;--frame:#333238}}
body{{background:var(--bg);color:var(--fg);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:820px;margin:32px auto;padding:0 16px}}
h2{{font-size:18px;margin:32px 0 8px}}
.frame{{border:1px solid var(--frame);border-radius:12px;padding:16px;display:flex;justify-content:center}}
button{{position:fixed;top:16px;right:16px;padding:6px 12px;border-radius:8px;border:1px solid var(--frame);background:var(--bg);color:var(--fg);cursor:pointer}}
</style></head><body>
<button onclick="document.body.classList.toggle('dark')">Toggle light / dark</button>
<h1>{title}</h1>
{sections}
</body></html>
"""


def inline(svg_path: Path):
    prefix = svg_path.stem
    svg = svg_path.read_text()
    svg = re.sub(r'id="([^"]+)"', lambda m: f'id="{prefix}-{m.group(1)}"', svg)
    svg = re.sub(r'href="#([^"]+)"', lambda m: f'href="#{prefix}-{m.group(1)}"', svg)
    return re.sub(r"url\(#([^)]+)\)", lambda m: f"url(#{prefix}-{m.group(1)})", svg)


def write_compare_page(svgs, out: Path, title):
    sections = "".join(f"<section><h2>{svg.stem}</h2><div class='frame'>{inline(svg)}</div></section>" for svg in svgs)
    page = out / "compare.html"
    page.write_text(PAGE.format(title=title, sections=sections))
    return page


def screenshot(svg: Path, out: Path, theme):
    html = out / f"{svg.stem}-{theme}.html"
    png = out / f"{svg.stem}-{theme}.png"
    html.write_text(f'<html><body style="margin:0;background:{BACKGROUNDS[theme]}"><img src="file://{svg.resolve()}"></body></html>')
    width, height = (re.search(rf'{side}="(\d+)"', svg.read_text()).group(1) for side in ("width", "height"))
    subprocess.run(
        [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
         f"--window-size={width},{height}", f"--screenshot={png}", f"file://{html.resolve()}"],
        check=True, capture_output=True, timeout=60,
    )
    return png


def main():
    parser = argparse.ArgumentParser(description="Compare SVG drafts side by side, in light and dark.")
    parser.add_argument("svgs", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, required=True, help="A private folder for this session, not a shared one")
    parser.add_argument("--title", default="Header ideas")
    parser.add_argument("--png", action="store_true", help="Also render each SVG on a light and a dark page")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    print(write_compare_page(args.svgs, args.out, args.title))
    if args.png:
        for svg in args.svgs:
            for theme in BACKGROUNDS:
                print(screenshot(svg, args.out, theme))


main()
