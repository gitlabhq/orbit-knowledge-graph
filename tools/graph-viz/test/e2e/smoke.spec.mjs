import { test, expect } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { inflateSync } from 'node:zlib';

const __dirname = dirname(fileURLToPath(import.meta.url));
const screenshotDir = join(__dirname, '../../tmp/screenshots');

// Load the committed fixture by default so CI runs without orbit/DuckDB. Set
// VIZ_DATA=data to screenshot a real adapter-generated graph instead.
const DATA = process.env.VIZ_DATA || '/test/fixtures';
const HOME = `/?data=${encodeURIComponent(DATA)}`;

test.beforeAll(() => {
  mkdirSync(screenshotDir, { recursive: true });
});

// Decode a PNG buffer enough to count pixels brighter than a small threshold.
// Avoids an image dependency; supports the 8-bit RGBA, no-interlace PNGs that
// Playwright produces.
function countNonBlackPixels(pngBuffer) {
  let pos = 8; // skip signature
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = 0;
  const idat = [];
  while (pos < pngBuffer.length) {
    const len = pngBuffer.readUInt32BE(pos);
    const type = pngBuffer.toString('ascii', pos + 4, pos + 8);
    const data = pngBuffer.subarray(pos + 8, pos + 8 + len);
    if (type === 'IHDR') {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      bitDepth = data[8];
      colorType = data[9];
    } else if (type === 'IDAT') {
      idat.push(data);
    } else if (type === 'IEND') {
      break;
    }
    pos += 12 + len;
  }
  if (bitDepth !== 8 || (colorType !== 6 && colorType !== 2)) {
    throw new Error(`unsupported PNG (bitDepth=${bitDepth} colorType=${colorType})`);
  }
  const channels = colorType === 6 ? 4 : 3;
  const raw = inflateSync(Buffer.concat(idat));
  const stride = width * channels;
  const out = Buffer.alloc(height * stride);
  let lit = 0;
  for (let y = 0; y < height; y += 1) {
    const filter = raw[y * (stride + 1)];
    const srcRow = y * (stride + 1) + 1;
    const dstRow = y * stride;
    for (let x = 0; x < stride; x += 1) {
      const rawByte = raw[srcRow + x];
      const a = x >= channels ? out[dstRow + x - channels] : 0;
      const b = y > 0 ? out[dstRow - stride + x] : 0;
      const c = x >= channels && y > 0 ? out[dstRow - stride + x - channels] : 0;
      let val;
      switch (filter) {
        case 1:
          val = rawByte + a;
          break;
        case 2:
          val = rawByte + b;
          break;
        case 3:
          val = rawByte + ((a + b) >> 1);
          break;
        case 4:
          val = rawByte + paeth(a, b, c);
          break;
        default:
          val = rawByte;
      }
      out[dstRow + x] = val & 0xff;
    }
  }
  for (let i = 0; i < out.length; i += channels) {
    if (out[i] + out[i + 1] + out[i + 2] > 60) lit += 1;
  }
  return lit;
}

function paeth(a, b, c) {
  const p = a + b - c;
  const pa = Math.abs(p - a);
  const pb = Math.abs(p - b);
  const pc = Math.abs(p - c);
  if (pa <= pb && pa <= pc) return a;
  if (pb <= pc) return b;
  return c;
}

test('renders the 3D graph scene with nodes and no console errors', async ({ page }) => {
  const consoleErrors = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') consoleErrors.push(msg.text());
  });
  page.on('pageerror', (err) => consoleErrors.push(String(err)));

  await page.goto(HOME);

  // The renderer sets window.__graphReady once graph.json + styles.json load and
  // the scene is initialized.
  await page.waitForFunction(() => window.__graphReady === true, { timeout: 30_000 });

  // A WebGL canvas must exist and have non-zero size.
  const canvas = page.locator('#graph canvas');
  await expect(canvas).toBeVisible();
  const box = await canvas.boundingBox();
  expect(box.width).toBeGreaterThan(100);
  expect(box.height).toBeGreaterThan(100);

  // The force graph must hold the seeded nodes and some links.
  const counts = await page.evaluate(() => {
    const data = window.__graph.graphData();
    return { nodes: data.nodes.length, links: data.links.length };
  });
  expect(counts.nodes).toBeGreaterThan(0);
  expect(counts.links).toBeGreaterThan(0);

  // WebGL actually drew something: capture a compositor screenshot and assert
  // it is not a uniform black frame. (A direct gl.readPixels is unreliable
  // under swiftshader's double-buffered swap, so we sample the captured PNG.)
  await page.waitForTimeout(2500);
  const shotPath = join(screenshotDir, 'graph-overview.png');
  const buf = await page.screenshot({ path: shotPath });
  const litPixels = countNonBlackPixels(buf);
  expect(litPixels, 'rendered frame should not be uniformly black').toBeGreaterThan(500);

  // Status line reflects the loaded graph.
  await expect(page.locator('#status')).toContainText('nodes');

  expect(consoleErrors, `console errors: ${consoleErrors.join('\n')}`).toEqual([]);
});

test('search and inspector interactions work', async ({ page }) => {
  await page.goto(HOME);
  await page.waitForFunction(() => window.__graphReady === true, { timeout: 30_000 });

  // Pick a real Definition label from the loaded data to search for.
  const term = await page.evaluate(() => {
    const data = window.__graph.graphData();
    const def = data.nodes.find((n) => n.kind === 'Definition' && n.label.length > 3);
    return def ? def.label : data.nodes[0].label;
  });

  await page.fill('#search', term.slice(0, Math.min(term.length, 6)));
  const results = page.locator('#search-results li');
  await expect(results.first()).toBeVisible({ timeout: 5000 });
  await results.first().click();

  // Inspector opens for the selected node.
  await expect(page.locator('#inspector')).toBeVisible();
  await expect(page.locator('#inspector .inspector-body')).toContainText('kind');

  await page.waitForTimeout(1500);
  await page.screenshot({ path: join(screenshotDir, 'graph-inspector.png') });

  // Legend has all five edge-type toggles.
  await expect(page.locator('#legend-items .legend-item')).toHaveCount(5);
});

test('"who calls this" ripple animates over CALLS edges', async ({ page }) => {
  await page.goto(HOME);
  await page.waitForFunction(() => window.__graphReady === true, { timeout: 30_000 });

  // Select a node that has callers so the ripple has CALLS edges to animate.
  const selected = await page.evaluate(() => {
    const data = window.__graph.graphData();
    const counts = new Map();
    for (const l of data.links) {
      if (l.kind !== 'CALLS') continue;
      const t = typeof l.target === 'object' ? l.target.id : l.target;
      counts.set(t, (counts.get(t) || 0) + 1);
    }
    let best = null;
    let bestN = 0;
    for (const [id, n] of counts) {
      if (n > bestN) {
        bestN = n;
        best = id;
      }
    }
    return { id: best, callers: bestN };
  });
  expect(selected.callers).toBeGreaterThan(0);

  // Drive selection + ripple directly through the page hooks the UI uses.
  await page.evaluate((id) => {
    window.__selectForTest(id);
  }, selected.id);
  await expect(page.locator('#inspector .who-calls')).toHaveAttribute('data-disabled', 'false');
  await page.locator('#inspector .who-calls').click();
  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(screenshotDir, 'graph-ripple.png') });
});
