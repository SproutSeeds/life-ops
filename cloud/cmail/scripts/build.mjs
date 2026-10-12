import { build } from 'esbuild';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

await build({
  entryPoints: ['src/index.mjs'],
  outfile: 'dist/index.js',
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  minify: false,
  sourcemap: false,
  loader: { '.html': 'text', '.css': 'text', '.png': 'base64' },
  plugins: [{
    name: 'raw-client-assets',
    setup(builder) {
      builder.onResolve({ filter: /\?raw$/ }, (args) => ({
        path: path.resolve(args.resolveDir, args.path.slice(0, -4)), namespace: 'raw',
      }));
      builder.onLoad({ filter: /.*/, namespace: 'raw' }, async (args) => ({ contents: await readFile(args.path, 'utf8'), loader: 'text' }));
    },
  }],
});
console.log('Built dist/index.js');
