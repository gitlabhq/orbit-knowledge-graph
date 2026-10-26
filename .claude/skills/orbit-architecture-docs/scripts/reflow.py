# /// script
# requires-python = ">=3.11"
# ///
import re
import sys
import textwrap
from pathlib import Path

WIDTH = 78
UNBREAKABLE = re.compile(r"\[[^\]]*\]\([^)]*\)|`[^`]*`")
LIST_ITEM = re.compile(r"^(- |\d+\. )")
VERBATIM = ("#", "!", "|", "<")


def wrap(text, width):
    glued = UNBREAKABLE.sub(lambda match: match.group(0).replace(" ", "\x00"), text)
    lines = textwrap.wrap(glued, width, break_on_hyphens=False, break_long_words=False)
    return [line.replace("\x00", " ") for line in lines]


def wrap_block(block):
    text = " ".join(line.strip() for line in block)
    marker = LIST_ITEM.match(text)
    if not marker:
        return wrap(text, WIDTH)
    indent = " " * len(marker.group(0))
    first, *rest = wrap(text[len(indent) :], WIDTH - len(indent))
    return [marker.group(0) + first, *(indent + line for line in rest)]


def split_frontmatter(text):
    if not text.startswith("---\n"):
        return "", text
    end = text.index("\n---\n", 4) + len("\n---\n")
    return text[:end], text[end:]


def reflow(text):
    frontmatter, body = split_frontmatter(text)
    out, block, fenced = [], [], False
    for line in body.split("\n"):
        if line.startswith("```"):
            fenced = not fenced
            out.extend(wrap_block(block) if block else [])
            block = []
            out.append(line)
            continue
        if fenced or not line.strip() or line.startswith(VERBATIM):
            out.extend(wrap_block(block) if block else [])
            block = []
            out.append(line)
            continue
        if LIST_ITEM.match(line) and block:
            out.extend(wrap_block(block))
            block = []
        block.append(line)
    out.extend(wrap_block(block) if block else [])
    return frontmatter + "\n".join(out)


for name in sys.argv[1:]:
    path = Path(name)
    path.write_text(reflow(path.read_text()))
