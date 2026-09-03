import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

afterEach(cleanup);

if (!globalThis.CSS) Object.assign(globalThis, { CSS: {} });
if (!CSS.escape) CSS.escape = (value: string) => value.replace(/[^a-zA-Z0-9_-]/g, (character) => `\\${character}`);
