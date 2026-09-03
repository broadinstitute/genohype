import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

afterEach(cleanup);

Object.defineProperty(HTMLImageElement.prototype, 'complete', { configurable: true, get: () => false });
Object.defineProperty(HTMLImageElement.prototype, 'naturalWidth', { configurable: true, get: () => 0 });
Object.defineProperty(HTMLImageElement.prototype, 'naturalHeight', { configurable: true, get: () => 0 });
