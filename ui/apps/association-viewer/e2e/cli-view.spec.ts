import { expect, test } from '@playwright/test';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';

const repoRoot = resolve(process.cwd(), '..');
const binary = process.env.GENOHYPE_E2E_BINARY ?? join(repoRoot, 'target/release/genohype');
const fixture = join(repoRoot, 'data/test/association/single-variant.ht');

async function startViewer(root: string): Promise<{ process: ChildProcessWithoutNullStreams; url: string; stderr: () => string }> {
  const home = join(root, 'home');
  const config = join(root, 'config');
  const cache = join(root, 'cache');
  await Promise.all([mkdir(home), mkdir(config), mkdir(cache)]);
  const child = spawn(binary, [
    'manhattan',
    '--table', fixture,
    '--y-field', 'Pvalue',
    '--output', join(root, 'plot'),
    '--width', '800',
    '--height', '400',
    '--view',
    '--no-open',
    '--port', '0',
  ], {
    cwd: repoRoot,
    env: { ...process.env, HOME: home, XDG_CONFIG_HOME: config, XDG_CACHE_HOME: cache },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk: string) => { stderr = (stderr + chunk).slice(-64 * 1024); });
  child.stdout.setEncoding('utf8');
  let url: string;
  try {
    url = await new Promise<string>((resolveUrl, reject) => {
      let stdout = '';
      const timer = setTimeout(() => reject(new Error(`timed out waiting for viewer URL; stdout=${stdout}; stderr=${stderr}`)), 60_000);
      const finish = (result: () => void) => { clearTimeout(timer); result(); };
      child.once('error', (error) => finish(() => reject(error)));
      child.once('exit', (code, signal) => finish(() => reject(new Error(`viewer exited before URL (code=${code}, signal=${signal}); stderr=${stderr}`))));
      child.stdout.on('data', (chunk: string) => {
        stdout += chunk;
        const match = stdout.match(/Viewer URL: (http:\/\/127\.0\.0\.1:\d+\/[0-9a-f-]+\/)/);
        if (match) finish(() => resolveUrl(match[1]));
      });
    });
  } catch (error) {
    if (child.exitCode == null && child.signalCode == null) child.kill('SIGKILL');
    throw error;
  }
  return { process: child, url, stderr: () => stderr };
}

async function stopViewer(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (child.exitCode != null || child.signalCode != null) return;
  child.kill('SIGINT');
  await Promise.race([
    new Promise<void>((resolveExit) => child.once('exit', () => resolveExit())),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error('viewer did not exit after SIGINT')), 10_000)),
  ]).catch((error) => {
    child.kill('SIGKILL');
    throw error;
  });
  if (child.exitCode !== 0) throw new Error(`viewer exited with ${child.exitCode}: ${child.stderr.read() ?? ''}`);
}

test('production CLI viewer renders bounded artifacts and synchronizes plot/table selection', async ({ page }) => {
  const root = await mkdtemp(join(tmpdir(), 'genohype-browser-viewer-'));
  let viewer: Awaited<ReturnType<typeof startViewer>> | undefined;
  try {
    viewer = await startViewer(root);
    const resourceUrls: string[] = [];
    page.on('response', (response) => resourceUrls.push(response.url()));

    await page.goto(viewer.url);
    await expect(page.locator('main[data-state="ready"]')).toBeVisible();
    const raster = page.locator('img.association-raster');
    await expect(raster).toBeVisible();
    await expect.poll(() => raster.evaluate((image: HTMLImageElement) => [image.naturalWidth, image.naturalHeight])).toEqual([800, 400]);
    await expect(page.getByLabel('Chromosomes')).toBeVisible();
    await expect(page.getByRole('table')).toBeVisible();

    const manifestResponse = await page.request.get(new URL('api/manifest', viewer.url).href);
    expect(manifestResponse.ok()).toBeTruthy();
    const manifestBody = await manifestResponse.body();
    expect(manifestBody.length).toBeLessThan(16 * 1024 * 1024);
    const manifest = JSON.parse(manifestBody.toString('utf8'));
    expect(manifest.resources.raster.url).toBe('raster');
    expect(manifest.top_hits.length).toBeGreaterThan(1);
    expect(manifest.top_hits.length).toBeLessThanOrEqual(1000);

    const plotHits = page.locator('svg[aria-label="Interactive top association hits"] circle[role="button"]');
    await expect(plotHits.first()).toBeVisible();
    await plotHits.first().click();
    const selectedRow = page.locator('tbody tr[aria-selected="true"]');
    await expect(selectedRow).toHaveCount(1);
    const firstSelectionX = await page.locator('.association-hit-selection').getAttribute('cx');

    await selectedRow.focus();
    await selectedRow.press('ArrowDown');
    await expect(page.locator('tbody tr[aria-selected="true"]')).toHaveCount(1);
    await expect.poll(() => page.locator('.association-hit-selection').getAttribute('cx')).not.toBe(firstSelectionX);

    await page.reload();
    await expect(page.locator('main[data-state="ready"]')).toBeVisible();
    await expect(page.locator('img.association-raster')).toBeVisible();

    const tokenPath = new URL(viewer.url).pathname;
    const applicationRequests = resourceUrls
      .map((url) => new URL(url))
      .filter((url) => url.origin === new URL(viewer!.url).origin && url.pathname.startsWith(tokenPath))
      .map((url) => url.pathname.slice(tokenPath.length));
    expect(applicationRequests.some((path) => path === 'api/manifest')).toBeTruthy();
    expect(applicationRequests.some((path) => path === 'api/raster')).toBeTruthy();
    expect(applicationRequests.every((path) => path === '' || path === 'api/manifest' || path === 'api/raster' || path.startsWith('assets/'))).toBeTruthy();
    expect(applicationRequests.some((path) => /points|rows|table/i.test(path))).toBeFalsy();

    if (process.env.GENOHYPE_E2E_SCREENSHOT) {
      await page.screenshot({ path: process.env.GENOHYPE_E2E_SCREENSHOT, fullPage: true });
    }
  } finally {
    try {
      if (viewer) await stopViewer(viewer.process);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});
