import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const schemaDirectory = resolve(scriptDirectory, '../../../../association/schema');
const schemaFiles = [
  'association-visualization-v1.schema.json',
  'association-page-v1.schema.json',
];
const schemas = schemaFiles.map((file) => {
  const bytes = readFileSync(resolve(schemaDirectory, file));
  return { file, bytes, schema: JSON.parse(bytes.toString('utf8')) };
});
const outputPath = resolve(scriptDirectory, '../src/contracts/types.ts');

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

const blocks = [
  '/**',
  ' * GENERATED FILE — DO NOT EDIT.',
  ` * Sources: ${schemaFiles.map((file) => `association/schema/${file}`).join(', ')}`,
  ...schemas.map(({ file, bytes }) => ` * ${file} SHA-256: ${createHash('sha256').update(bytes).digest('hex')}`),
  ' * Regenerate with: npm run contracts:generate -w @genohype/association-viz',
  ' */',
  '',
];

const definitions = new Map();
for (const { file, schema } of schemas) {
  for (const [name, definition] of Object.entries(schema.$defs ?? {})) {
    const existing = definitions.get(name);
    if (existing && JSON.stringify(existing.definition) !== JSON.stringify(definition)) {
      throw new Error(`conflicting ${name} definitions in ${existing.file} and ${file}`);
    }
    if (!existing) definitions.set(name, { file, definition });
  }
}
for (const [name, { definition }] of definitions) {
  blocks.push(emitNamedType(name, definition), '');
}
for (const { schema } of schemas) {
  blocks.push(emitNamedType(schema.title, schema), '');
}

const manifestSchema = schemas[0].schema;
const resourcesRef = manifestSchema.properties?.resources?.$ref;
const resourcesDefinition = resourcesRef
  ? manifestSchema.$defs?.[resourcesRef.split('/').at(-1)]
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
