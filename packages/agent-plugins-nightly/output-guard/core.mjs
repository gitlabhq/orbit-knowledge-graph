import { closeSync, fstatSync, openSync, readSync } from "node:fs";

export const DEFAULT_OUTPUT_LIMIT = 10_000;

/**
 * `ORBIT_OUTPUT_GUARD`: unset uses the default limit, `off` disables, `20k` or `20000` sets it in characters.
 * @param {string | undefined} value
 * @param {(message: string) => void} [warn]
 * @returns {number | undefined}
 */
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

/**
 * Output to shorten: all of it in `head` (`complete`), or its first and last bytes with the size of the whole.
 * @typedef {{ head: string; tail: string; totalBytes: number; complete: boolean }} OutputEnds
 */

/** @param {string} text @returns {OutputEnds} */
export function wholeOutput(text) {
  return { head: text, tail: "", totalBytes: Buffer.byteLength(text), complete: true };
}

/**
 * The first and last part of the output, about `limit` characters in all, cut at line boundaries,
 * with a marker that names where the full output is and how many bytes it leaves out.
 * Undefined when the output fits.
 * @param {OutputEnds} ends
 * @param {number} limit
 * @param {string} fullOutputPath
 * @returns {string | undefined}
 */
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

/** @param {string} text @param {number} limit @param {string} fullOutputPath */
export function shrinkOutput(text, limit, fullOutputPath) {
  return shrinkEnds(wholeOutput(text), limit, fullOutputPath);
}

/**
 * Drops the line a read cut in two (and with it any half character): the last of a head, the first of a tail.
 * @param {string} text
 * @param {"head" | "tail"} end
 */
function wholeLines(text, end) {
  if (end === "head") {
    const cut = text.lastIndexOf("\n");
    return cut === -1 ? text.replace(/\uFFFD+$/, "") : text.slice(0, cut);
  }
  const cut = text.indexOf("\n");
  return cut === -1 ? text.replace(/^\uFFFD+/, "") : text.slice(cut + 1);
}

/**
 * A file's first and last `limit` bytes, so a huge file is never read whole. A line cut by
 * either read is dropped. A file of up to four times the limit is read whole, because it may
 * still fit.
 * @param {string} path
 * @param {number} limit
 * @returns {OutputEnds}
 */
export function readEnds(path, limit) {
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const read = (/** @type {number} */ length, /** @type {number} */ position) => {
      const buffer = Buffer.alloc(length);
      readSync(fd, buffer, 0, length, position);
      return buffer.toString("utf8");
    };
    if (size <= 4 * limit) return { head: read(size, 0), tail: "", totalBytes: size, complete: true };
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
