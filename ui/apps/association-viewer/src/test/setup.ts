import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

window.history.replaceState(null, '', '/token/');
afterEach(() => {
  cleanup();
  window.history.replaceState(null, '', '/token/');
  Object.defineProperty(window, 'innerWidth', { configurable: true, value: 1024 });
  Object.defineProperty(window, 'devicePixelRatio', { configurable: true, value: 1 });
});

Object.defineProperty(HTMLImageElement.prototype, 'complete', { configurable: true, get: () => false });
Object.defineProperty(HTMLImageElement.prototype, 'naturalWidth', { configurable: true, get: () => 0 });
Object.defineProperty(HTMLImageElement.prototype, 'naturalHeight', { configurable: true, get: () => 0 });
