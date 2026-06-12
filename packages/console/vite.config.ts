import { fileURLToPath, URL } from 'node:url';
import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://10.200.30.77:30980',
        changeOrigin: true,
        ws: true,
      },
    },
  },
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      '@tdesign/ai-chat-engine': fileURLToPath(new URL('../chat-engine/index.ts', import.meta.url)),
      '@tdesign/ai-shared': fileURLToPath(new URL('../shared/index.ts', import.meta.url)),
    },
  },
});
