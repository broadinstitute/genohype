import { expect, test, type APIResponse, type Browser, type BrowserContext } from '@playwright/test';
import { mkdtemp, mkdir, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';

const repoRoot = resolve(process.cwd(), '..');
const binary = process.env.GENOHYPE_E2E_BINARY ?? join(repoRoot, 'target/release/genohype');
const fixture = join(repoRoot, 'data/test/association/single-variant.tsv');
const mapping = 'contig=CHR,position=POS,ref=REF,alt=ALT,pvalue=P,neg_log10_p=NEG_LOG10_P,beta=BETA,se=SE,af=AF,ac=AC,series=SERIES';

interface ViewerProcess {
  process: ChildProcessWithoutNullStreams;
  url: string;
  cache: string;
  output: () => { stdout: string; stderr: string };
}

async function startAssociationViewer(root: string): Promise<ViewerProcess> {
  const home = join(root, 'home');
  const config = join(root, 'config');
  const cache = join(root, 'cache');
  await mkdir(home, { mode: 0o700 });
  await Promise.all([
    mkdir(config, { mode: 0o700 }),
    mkdir(cache, { mode: 0o700 }),
    mkdir(join(home, 'Library', 'Caches'), { mode: 0o700, recursive: true }),
  ]);
  const cacheBase = process.platform === 'darwin' ? join(home, 'Library', 'Caches') : cache;
  const child = spawn(binary, [
    'association', 'view', fixture,
    '--build', 'GRCh38',
    '--map', mapping,
    '--ephemeral',
    '--no-open',
    '--port', '0',
  ], {
    cwd: repoRoot,
    env: { ...process.env, HOME: home, XDG_CONFIG_HOME: config, XDG_CACHE_HOME: cache },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk: string) => { stdout = (stdout + chunk).slice(-128 * 1024); });
  child.stderr.on('data', (chunk: string) => { stderr = (stderr + chunk).slice(-128 * 1024); });
  try {
    const url = await new Promise<string>((resolveUrl, reject) => {
      const timer = setTimeout(() => reject(new Error(`timed out waiting for association viewer URL; stdout=${stdout}; stderr=${stderr}`)), 120_000);
      const finish = (result: () => void) => { clearTimeout(timer); result(); };
      child.once('error', (error) => finish(() => reject(error)));
      child.once('exit', (code, signal) => finish(() => reject(new Error(`association viewer exited before URL (code=${code}, signal=${signal}); stdout=${stdout}; stderr=${stderr}`))));
      child.stdout.on('data', () => {
        const match = stdout.match(/Viewer URL: (http:\/\/127\.0\.0\.1:\d+\/[0-9a-f-]+\/)/);
        if (match) finish(() => resolveUrl(match[1]));
      });
    });
    return {
      process: child,
      url,
      cache: join(cacheBase, 'genohype', 'association', 'v1'),
      output: () => ({ stdout, stderr }),
    };
  } catch (error) {
    if (child.exitCode == null && child.signalCode == null) child.kill('SIGKILL');
    throw error;
  }
}

async function stopViewer(viewer: ViewerProcess): Promise<void> {
  const child = viewer.process;
  if (child.exitCode == null && child.signalCode == null) {
    child.kill('SIGINT');
    await Promise.race([
      new Promise<void>((resolveExit) => child.once('exit', () => resolveExit())),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('association viewer did not exit after SIGINT')), 10_000)),
    ]).catch((error) => {
      child.kill('SIGKILL');
      throw error;
    });
  }
  const output = viewer.output();
  if (child.exitCode !== 0) throw new Error(`association viewer exited with ${child.exitCode}: stdout=${output.stdout}; stderr=${output.stderr}`);
}

async function sessionEntries(cache: string): Promise<string[]> {
  return readdir(join(cache, 'sessions'));
}

async function boundedJson(response: APIResponse, maximum: number): Promise<{ body: Buffer; value: any }> {
  expect(response.ok(), `${response.url()} status`).toBeTruthy();
  expect(response.headers()['content-type']).toContain('application/json');
  expect(response.headers()['cache-control']).toBe('no-store');
  const body = await response.body();
  expect(body.length, `${response.url()} payload bound`).toBeLessThanOrEqual(maximum);
  return { body, value: JSON.parse(body.toString('utf8')) };
}

function resolveDescriptor(manifestResponseUrl: string, descriptor: { url: string }): string {
  return new URL(descriptor.url, new URL('./', manifestResponseUrl)).href;
}

function assertNoPrivatePaths(bodies: readonly Buffer[], root: string, cache: string): void {
  const text = Buffer.concat(bodies).toString('utf8');
  for (const forbidden of [fixture, root, cache, join(root, 'home'), join(root, 'config')]) {
    expect(text, `dynamic bodies must not disclose ${forbidden}`).not.toContain(forbidden);
  }
}

async function newDprContext(browser: Browser): Promise<BrowserContext> {
  return browser.newContext({ viewport: { width: 1000, height: 900 }, deviceScaleFactor: 2 });
}

test('production association view serves indexed Phase 5 drilldown safely and cleans ephemeral state', async ({ browser }) => {
  const root = await mkdtemp(join(tmpdir(), 'genohype-association-view-e2e-'));
  let viewer: ViewerProcess | undefined;
  let context: BrowserContext | undefined;
  const dynamicBodies: Buffer[] = [];
  try {
    viewer = await startAssociationViewer(root);
    context = await newDprContext(browser);
    const page = await context.newPage();
    const tokenUrl = new URL(viewer.url);
    const tokenPath = tokenUrl.pathname;
    const browserApiRequests: URL[] = [];
    const browserApiResponses: Array<{ url: URL; status: number; body: Buffer }> = [];
    const responseReads: Promise<void>[] = [];
    page.on('request', (request) => {
      const url = new URL(request.url());
      if (url.origin === tokenUrl.origin && url.pathname.startsWith(`${tokenPath}api/`)) {
        expect(request.method()).toBe('GET');
        browserApiRequests.push(url);
      }
    });
    page.on('response', (response) => {
      const url = new URL(response.url());
      if (url.origin === tokenUrl.origin && url.pathname.startsWith(`${tokenPath}api/`)) {
        responseReads.push(response.body().then((body) => {
          browserApiResponses.push({ url, status: response.status(), body });
        }).catch(() => undefined));
      }
    });

    await page.goto(viewer.url);
    await expect(page.locator('main[data-state="ready"][data-route="root"]')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Association dataset' })).toBeVisible();
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.locator('.association-table-toolbar [role="status"]')).toHaveText('1–10 of 10 hits');
    await expect(page.getByRole('searchbox', { name: 'Search hits' })).toBeVisible();

    const rootManifestUrl = new URL('api/manifest', viewer.url).href;
    const rootManifestResponse = await context.request.get(rootManifestUrl);
    const rootManifestResult = await boundedJson(rootManifestResponse, 16 * 1024 * 1024);
    dynamicBodies.push(rootManifestResult.body);
    const manifest = rootManifestResult.value;
    expect(manifest).toMatchObject({ assembly: 'GRCh38', valid_row_count: 10, rendered_point_count: 10 });
    expect(manifest.resources).toMatchObject({
      raster: { url: 'raster', media_type: 'image/png' },
      table_api: { media_type: 'application/json' },
      region_api: { media_type: 'application/json' },
    });
    const datasetId = manifest.plot_id as string;
    expect(datasetId).toMatch(/^[0-9a-f-]{36}$/);

    const tableUrl = resolveDescriptor(rootManifestResponse.url(), manifest.resources.table_api);
    const tableResult = await boundedJson(await context.request.get(`${tableUrl}?offset=0&limit=100&sort=pvalue&direction=ascending`), 8 * 1024 * 1024);
    dynamicBodies.push(tableResult.body);
    expect(tableResult.value).toMatchObject({ dataset_id: datasetId, offset: 0, limit: 100, total: 10 });
    expect(tableResult.value.points).toHaveLength(10);

    await page.getByRole('button', { name: 'Chromosome 4' }).click();
    await expect(page).toHaveURL(new RegExp(`${tokenPath}chromosome/4(?:\\?.*)?$`));
    await expect(page.locator('main[data-state="ready"][data-route="chromosome"]')).toBeVisible();
    await expect(page.locator('main[data-route="chromosome"] header')).toContainText('Chromosome 4');
    await page.reload();
    await expect(page.locator('main[data-state="ready"][data-route="chromosome"]')).toBeVisible();

    const chromosomeManifestUrl = new URL(`api/datasets/${datasetId}/chromosomes/4/manifest`, viewer.url).href;
    const chromosomeResult = await boundedJson(await context.request.get(chromosomeManifestUrl), 16 * 1024 * 1024);
    dynamicBodies.push(chromosomeResult.body);
    expect(chromosomeResult.value.layout.contigs).toHaveLength(1);
    expect(chromosomeResult.value.layout.contigs[0].name).toBe('4');
    expect(chromosomeResult.value.valid_row_count).toBe(7);

    const locusUrl = new URL(`locus/4/68000000-69000000`, viewer.url).href;
    await page.goto(locusUrl);
    await expect(page.locator('main[data-state="ready"][data-route="locus"]')).toBeVisible();
    await expect(page.getByText('Gene annotations unavailable.', { exact: false })).toBeVisible();
    await expect(page.getByText('Ordered by locus', { exact: true })).toBeVisible();
    await expect(page.getByRole('searchbox')).toHaveCount(0);
    await expect(page.locator('.association-table-toolbar [role="status"]')).toHaveText('1–7 of 7 hits');

    const observedRegionManifestRequest = browserApiRequests.find((url) => url.pathname.endsWith('/region/manifest') && url.searchParams.get('start') === '68000000');
    expect(observedRegionManifestRequest, 'browser requested locus manifest').toBeTruthy();
    const regionRequestUrl = observedRegionManifestRequest!.href;
    expect(observedRegionManifestRequest!.searchParams.get('contig')).toBe('4');
    expect(observedRegionManifestRequest!.searchParams.get('end')).toBe('69000000');
    expect(observedRegionManifestRequest!.searchParams.get('height')).toBe('320');
    expect(observedRegionManifestRequest!.searchParams.get('dpr')).toBe('2');
    const requestedWidth = Number(observedRegionManifestRequest!.searchParams.get('width'));
    expect(requestedWidth).toBeGreaterThanOrEqual(50);
    expect(requestedWidth).toBeLessThanOrEqual(4050);

    const firstRegionResponse = await context.request.get(regionRequestUrl);
    const firstRegion = await boundedJson(firstRegionResponse, 16 * 1024 * 1024);
    const secondRegionResponse = await context.request.get(regionRequestUrl);
    const secondRegion = await boundedJson(secondRegionResponse, 16 * 1024 * 1024);
    dynamicBodies.push(firstRegion.body, secondRegion.body);
    expect(secondRegion.body.equals(firstRegion.body)).toBeTruthy();
    expect(secondRegion.value.resources).toEqual(firstRegion.value.resources);
    expect(firstRegion.value).toMatchObject({
      dataset_id: datasetId,
      assembly: 'GRCh38',
      layout: {
        contig: '4', requested_start: 68000000, requested_end: 69000000,
        requested_width: requestedWidth, height: 320, dpr: 2, physical_height: 640,
      },
      counts: { source_point_count: 7 },
    });

    const logicalWidth = firstRegion.value.layout.width as number;
    expect(firstRegion.value.layout.physical_width).toBe(logicalWidth * 2);
    const rasterUrl = resolveDescriptor(firstRegionResponse.url(), firstRegion.value.resources.raster);
    const overlayUrl = resolveDescriptor(firstRegionResponse.url(), firstRegion.value.resources.overlay);
    const rasterResponse = await context.request.get(rasterUrl);
    expect(rasterResponse.ok()).toBeTruthy();
    expect(rasterResponse.headers()['content-type']).toContain('image/png');
    expect(rasterResponse.headers()['cache-control']).toBe('no-store');
    const rasterBody = await rasterResponse.body();
    expect(rasterBody.length).toBe(firstRegion.value.resources.raster.byte_size);
    const overlayResult = await boundedJson(await context.request.get(overlayUrl), 16 * 1024 * 1024);
    dynamicBodies.push(overlayResult.body);
    expect(overlayResult.value).toMatchObject({
      render_hash: firstRegion.value.render_hash,
      gene_annotations: { state: 'unavailable', features: [] },
    });
    expect(overlayResult.value.points).toHaveLength(7);

    const locusRaster = page.locator('img.association-locus-raster');
    await expect(locusRaster).toBeVisible();
    await expect.poll(() => locusRaster.evaluate((image: HTMLImageElement) => [image.naturalWidth, image.naturalHeight])).toEqual([logicalWidth * 2, 640]);
    const overlaySvg = page.locator('svg.association-locus-overlay');
    await expect(overlaySvg).toHaveAttribute('viewBox', `0 0 ${logicalWidth} 320`);
    const logicalBox = await overlaySvg.boundingBox();
    expect(logicalBox).not.toBeNull();
    expect(logicalBox!.width).toBeLessThanOrEqual(logicalWidth);
    expect(logicalBox!.height / logicalBox!.width).toBeCloseTo(320 / logicalWidth, 2);

    const locusTableUrl = `${resolveDescriptor(rootManifestResponse.url(), manifest.resources.region_api)}?contig=4&start=68000000&end=69000000&offset=0&limit=100`;
    const locusTable = await boundedJson(await context.request.get(locusTableUrl), 8 * 1024 * 1024);
    dynamicBodies.push(locusTable.body);
    expect(locusTable.value).toMatchObject({ dataset_id: datasetId, offset: 0, limit: 100, total: 7 });
    expect(locusTable.value.points.map((point: any) => point.position)).toEqual([68000001, 68000010, 68123456, 68500000, 68750000, 68900000, 69000000]);

    const rowPoint = locusTable.value.points.find((point: any) => point.position === 68000010);
    const row = page.getByRole('row').filter({ hasText: '4:68,000,010 C>G' });
    await row.click();
    await expect(page).toHaveURL(new RegExp(`[?&]selected=${rowPoint.source_ordinal}(?:&|$)`));
    await expect(page.locator(`.association-locus-point[data-source-ordinal="${rowPoint.source_ordinal}"]`)).toHaveAttribute('aria-pressed', 'true');
    await expect(row).toHaveAttribute('aria-selected', 'true');

    const plotPoint = overlayResult.value.points.find((entry: any) => entry.point.position === 68123456).point;
    await page.locator(`.association-locus-point[data-source-ordinal="${plotPoint.source_ordinal}"]`).click();
    await expect(page).toHaveURL(new RegExp(`[?&]selected=${plotPoint.source_ordinal}(?:&|$)`));
    await expect(page.getByRole('row').filter({ hasText: '4:68,123,456 T>A' })).toHaveAttribute('aria-selected', 'true');

    await page.reload();
    await expect(page.locator('main[data-state="ready"][data-route="locus"]')).toBeVisible();
    await expect(page.locator(`.association-locus-point[data-source-ordinal="${plotPoint.source_ordinal}"]`)).toHaveAttribute('aria-pressed', 'true');
    await page.getByRole('button', { name: 'Whole genome' }).click();
    await expect(page.locator('main[data-route="root"]')).toBeVisible();
    await page.goBack();
    await expect(page.locator('main[data-route="locus"]')).toBeVisible();
    await expect(page).toHaveURL(new RegExp(`${tokenPath}locus/4/68000000-69000000`));
    await page.goForward();
    await expect(page.locator('main[data-route="root"]')).toBeVisible();

    const wrongToken = await context.request.get(new URL('/wrong-token/api/manifest', tokenUrl.origin).href);
    expect(wrongToken.status()).toBe(404);
    expect(await wrongToken.text()).toBe('not found\n');
    const unsafeAsset = await context.request.get(new URL('assets/%252e%252e%252findex.html', viewer.url).href);
    expect(unsafeAsset.status()).toBe(404);
    expect(await unsafeAsset.text()).toBe('not found\n');
    const wrongResource = await context.request.get(new URL(`api/datasets/${datasetId}/region/renders/${'a'.repeat(64)}/raster`, viewer.url).href);
    expect(wrongResource.status()).toBe(404);
    expect(await wrongResource.text()).toBe('not found\n');
    const badBounds = await context.request.get(new URL(`api/datasets/${datasetId}/region?contig=4&start=0&end=1`, viewer.url).href);
    expect(badBounds.status()).toBe(400);
    expect(await badBounds.text()).toBe('bad request\n');
    dynamicBodies.push(Buffer.from(await wrongToken.body()), Buffer.from(await unsafeAsset.body()), Buffer.from(await wrongResource.body()), Buffer.from(await badBounds.body()));

    await Promise.all(responseReads);
    expect(browserApiResponses.length).toBeGreaterThan(0);
    for (const response of browserApiResponses) {
      const relative = response.url.pathname.slice(tokenPath.length);
      expect(response.status, response.url.href).toBe(200);
      expect(response.body.length, `${response.url.href} response bound`).toBeLessThanOrEqual(
        relative.endsWith('/raster') || relative === 'api/raster' ? 64 * 1024 * 1024 : 16 * 1024 * 1024,
      );
      if (!relative.endsWith('/raster') && relative !== 'api/raster') dynamicBodies.push(response.body);
    }
    for (const request of browserApiRequests) {
      const relative = request.pathname.slice(tokenPath.length);
      expect(relative).toMatch(/^api\/(?:manifest|raster|datasets\/[0-9a-f-]{36}\/(?:table|region(?:\/manifest|\/renders\/[0-9a-f]{64}\/(?:raster|overlays\/[0-9a-f]{64}))?|chromosomes\/4\/(?:manifest|raster)))$/);
      if (relative.endsWith('/table')) {
        expect(Number(request.searchParams.get('offset'))).toBeGreaterThanOrEqual(0);
        expect(Number(request.searchParams.get('limit'))).toBeGreaterThanOrEqual(1);
        expect(Number(request.searchParams.get('limit'))).toBeLessThanOrEqual(1000);
      }
      if (relative.endsWith('/region')) {
        expect(Number(request.searchParams.get('start'))).toBeGreaterThanOrEqual(1);
        expect(Number(request.searchParams.get('end'))).toBeGreaterThanOrEqual(Number(request.searchParams.get('start')));
        expect(Number(request.searchParams.get('limit'))).toBeLessThanOrEqual(1000);
      }
      if (relative.endsWith('/region/manifest')) {
        expect(Number(request.searchParams.get('width'))).toBeGreaterThanOrEqual(50);
        expect(Number(request.searchParams.get('width'))).toBeLessThanOrEqual(4050);
        expect(Number(request.searchParams.get('height'))).toBe(320);
        expect(Number(request.searchParams.get('dpr'))).toBeGreaterThanOrEqual(1);
        expect(Number(request.searchParams.get('dpr'))).toBeLessThanOrEqual(4);
      }
    }
    assertNoPrivatePaths(dynamicBodies, root, viewer.cache);

    if (process.env.GENOHYPE_E2E_SCREENSHOT) {
      await page.goto(locusUrl);
      await expect(page.locator('main[data-route="locus"]')).toBeVisible();
      await page.screenshot({ path: process.env.GENOHYPE_E2E_SCREENSHOT, fullPage: true });
    }
  } finally {
    await context?.close();
    try {
      if (viewer) {
        await stopViewer(viewer);
        expect(await sessionEntries(viewer.cache), 'ephemeral generation after clean SIGINT').toEqual([]);
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});
