import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

for (const required of ['dist/index.js', 'dist/index.d.ts', 'dist/styles.css']) {
  if (!existsSync(required)) throw new Error(`missing published output ${required}`);
}
const walk = (directory) => readdirSync(directory).flatMap((name) => {
  const path = join(directory, name);
  return statSync(path).isDirectory() ? walk(path) : [path];
});
const javascript = walk('dist').filter((path) => path.endsWith('.js')).map((path) => readFileSync(path, 'utf8')).join('\n');
if (!javascript.includes('react/jsx-runtime')) throw new Error('React JSX runtime is not externalized as an import');
for (const bundledMarker of ['function useState(', 'REACT_ELEMENT_TYPE = Symbol.for']) {
  if (javascript.includes(bundledMarker)) throw new Error(`React appears bundled (${bundledMarker})`);
}
const css = readFileSync('dist/styles.css', 'utf8');
if (!css.includes('.association-manhattan') || !css.includes('.association-hit-table')) {
  throw new Error('published CSS is incomplete');
}
