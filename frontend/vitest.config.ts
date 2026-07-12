// Vitest for the pure lib/component logic (no DOM — nothing here renders). The `@/` alias mirrors
// tsconfig's paths so component modules resolve the same way they do under Next.
import path from 'node:path';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: { '@': path.resolve(__dirname, 'src') },
  },
  test: {
    include: ['src/**/*.test.{ts,tsx}'],
    environment: 'node',
  },
});
