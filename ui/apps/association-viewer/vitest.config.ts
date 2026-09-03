import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    environmentOptions: { jsdom: { url: 'https://example.test/token/index.html' } },
    setupFiles: ['./src/test/setup.ts'],
    restoreMocks: true,
  },
});
