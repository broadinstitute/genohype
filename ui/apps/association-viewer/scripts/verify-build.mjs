import { readFileSync } from 'node:fs';

const html = readFileSync('dist/index.html', 'utf8');
for (const attribute of html.matchAll(/(?:src|href)="([^"]+)"/g)) {
  const url = attribute[1];
  if (url.startsWith('/') || /^[a-z][a-z0-9+.-]*:/i.test(url)) {
    throw new Error(`built asset URL is not relative: ${url}`);
  }
}
if (!html.includes('./assets/')) throw new Error('Vite did not emit relative application assets');
