import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const schemaPath = resolve(
  scriptDirectory,
  '../../../../association/schema/association-visualization-v1.schema.json',
);
const outputPath = resolve(scriptDirectory, '../src/contracts/types.ts');
const schemaBytes = readFileSync(schemaPath);
const schema = JSON.parse(schemaBytes.toString('utf8'));

// Preserve the package's existing public name while deriving its shape from the
// Rust type named `YScale` in the schema.
const publicName = (name) => name === 'YScale' ? 'YScaleV1' : name;
const refName = (ref) => publicName(ref.split('/').at(-1));

function typeExpression(node) {
  if (node.$ref) return refName(node.$ref);
  if (node.anyOf) return node.anyOf.map(typeExpression).join(' | ');
  if (node.enum) return node.enum.map((value) => JSON.stringify(value)).join(' | ');
  if (Array.isArray(node.type)) {
    return node.type.map((type) => typeExpression({ type })).join(' | ');
  }
  switch (node.type) {
    case 'string': return 'string';
    case 'number':
    case 'integer': return 'number';
    case 'boolean': return 'boolean';
    case 'null': return 'null';
    case 'array': {
      const item = typeExpression(node.items ?? {});
      return item.includes(' | ') ? `(${item})[]` : `${item}[]`;
    }
    case 'object': return 'Record<string, unknown>';
    default: return 'unknown';
  }
}

function propertyName(name) {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name) ? name : JSON.stringify(name);
}

function emitNamedType(name, node) {
  const typeName = publicName(name);
  if (node.type !== 'object' || !node.properties) {
    return `export type ${typeName} = ${typeExpression(node)};`;
  }
  const required = new Set(node.required ?? []);
  const lines = [`export interface ${typeName} {`];
  for (const [property, propertySchema] of Object.entries(node.properties)) {
    const optional = required.has(property) ? '' : '?';
    lines.push(`  ${propertyName(property)}${optional}: ${typeExpression(propertySchema)};`);
  }
  if (node.additionalProperties !== false) lines.push('  [minorField: string]: unknown;');
  lines.push('}');
  return lines.join('\n');
}

const digest = createHash('sha256').update(schemaBytes).digest('hex');
const blocks = [
  '/**',
  ' * GENERATED FILE — DO NOT EDIT.',
  ' * Source: association/schema/association-visualization-v1.schema.json',
  ` * Schema SHA-256: ${digest}`,
  ' * Regenerate with: npm run contracts:generate -w @genohype/association-viz',
  ' */',
  '',
];
for (const [name, definition] of Object.entries(schema.$defs ?? {})) {
  blocks.push(emitNamedType(name, definition), '');
}
blocks.push(emitNamedType(schema.title, schema), '');

const resourcesRef = schema.properties?.resources?.$ref;
const resourcesDefinition = resourcesRef
  ? schema.$defs?.[resourcesRef.split('/').at(-1)]
  : undefined;
if (!resourcesDefinition?.properties) {
  throw new Error('manifest resources definition is missing from the schema');
}
const resourceNames = Object.keys(resourcesDefinition.properties)
  .map((name) => JSON.stringify(name))
  .join(' | ');
blocks.push(`export type AssociationResourceName = ${resourceNames};`, '');
const generated = `${blocks.join('\n').trimEnd()}\n`;

if (process.argv.includes('--check')) {
  const checkedIn = readFileSync(outputPath, 'utf8');
  if (checkedIn !== generated) {
    console.error(
      'TypeScript contract drift: run `npm run contracts:generate -w @genohype/association-viz` from ui/.',
    );
    process.exitCode = 1;
  }
} else {
  writeFileSync(outputPath, generated);
}
