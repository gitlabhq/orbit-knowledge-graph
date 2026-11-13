import { closeSync, fstatSync, openSync, readSync } from "node:fs";

export const DEFAULT_OUTPUT_LIMIT = 10_000;
const MAX_UTF8_BYTES_PER_CHAR = 4;

export function parseOutputLimit(value, warn = console.warn) {
  if (value === undefined) return DEFAULT_OUTPUT_LIMIT;

  const normalised = value.trim();
  if (normalised === "" || ["0", "off", "false", "no"].includes(normalised.toLowerCase())) return undefined;

  const match = normalised.match(/^(\d+)(k)?$/i);
  const limit = match ? Number(match[1]) * (match[2] ? 1_000 : 1) : Number.NaN;
  if (Number.isSafeInteger(limit) && limit > 0) return limit;

  warn(`orbit: invalid ORBIT_OUTPUT_GUARD=${JSON.stringify(value)}; using ${DEFAULT_OUTPUT_LIMIT}.`);
  return DEFAULT_OUTPUT_LIMIT;
}

const MAX_FRAMING_BYTES = 300;
const GUARD_MARKER = /\n\[\.\.\. (\d+) of (\d+) bytes omitted\. Full output: [^\n]*\]\n/g;

/**
 * Quoting a marker is not enough: the kept head and tail and the omitted count must add up to the full size,
 * give or take the framing a Pi guard adds around them.
 */
export function alreadyGuarded(text) {
  const markers = [...text.matchAll(GUARD_MARKER)];
  if (markers.length !== 1) return false;
  const [marker, omitted, total] = markers[0];
  const index = markers[0].index ?? 0;
  const kept = Buffer.byteLength(text.slice(0, index)) + Buffer.byteLength(text.slice(index + marker.length));
  const surplus = kept + Number(omitted) - Number(total);
  return surplus >= 0 && surplus <= MAX_FRAMING_BYTES;
}

export function wholeOutput(text) {
  return { head: text, tail: "", totalBytes: Buffer.byteLength(text), complete: true };
}

export function shrinkEnds(ends, limit, fullOutputPath) {
  if (ends.complete && ends.head.length <= limit) return undefined;

  const half = Math.floor(limit / 2);
  const headSource = ends.head;
  const tailSource = ends.complete ? ends.head : ends.tail;
  const headEnd = headSource.lastIndexOf("\n", half);
  const head = headSource.length <= half ? headSource : headSource.slice(0, headEnd > 0 ? headEnd : half);
  const tailStart = tailSource.indexOf("\n", tailSource.length - half);
  const tail =
    tailSource.length <= half
      ? tailSource
      : tailSource.slice(tailStart !== -1 && tailStart < tailSource.length - 1 ? tailStart + 1 : tailSource.length - half);

  const omitted = ends.totalBytes - Buffer.byteLength(head) - Buffer.byteLength(tail);
  const marker = `[... ${omitted} of ${ends.totalBytes} bytes omitted. Full output: ${fullOutputPath} (read a range of it or rg it; do not cat it) ...]`;
  return `${head}\n${marker}\n${tail}`;
}

export function shrinkOutput(text, limit, fullOutputPath) {
  return shrinkEnds(wholeOutput(text), limit, fullOutputPath);
}

function wholeLines(text, end) {
  if (end === "head") {
    const cut = text.lastIndexOf("\n");
    return cut === -1 ? text.replace(/\uFFFD+$/, "") : text.slice(0, cut);
  }
  const cut = text.indexOf("\n");
  return cut === -1 ? text.replace(/^\uFFFD+/, "") : text.slice(cut + 1);
}

export function readEnds(path, limit) {
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const read = (length, position) => {
      const buffer = Buffer.alloc(length);
      readSync(fd, buffer, 0, length, position);
      return buffer.toString("utf8");
    };
    if (size <= MAX_UTF8_BYTES_PER_CHAR * limit) return { head: read(size, 0), tail: "", totalBytes: size, complete: true };
    return {
      head: wholeLines(read(limit, 0), "head"),
      tail: wholeLines(read(limit, size - limit), "tail"),
      totalBytes: size,
      complete: false,
    };
  } finally {
    closeSync(fd);
  }
}
