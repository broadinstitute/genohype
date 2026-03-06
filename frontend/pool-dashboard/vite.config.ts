import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  // Base path for production - dashboard is served at /dashboard
  base: '/dashboard/',
  server: {
    port: parseInt(process.env.PORT || '5174'),
    proxy: {
      // Proxy all /api requests to the local Rust coordinator
      '/api': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
      },
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
})
