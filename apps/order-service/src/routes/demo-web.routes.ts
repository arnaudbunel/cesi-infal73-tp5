import { readFile } from 'node:fs/promises';
import type { FastifyPluginAsync } from 'fastify';

/** Dossier des fichiers statiques de la démonstration (identique depuis `src/` et `dist/`). */
const PUBLIC_DIR = new URL('../../../demo-web/public/', import.meta.url);

// Liste blanche explicite : aucun chemin fourni par le client n'atteint le système de fichiers.
const FILES: Record<string, { file: string; type: string }> = {
  '/demo/': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/demo/app.js': { file: 'app.js', type: 'text/javascript; charset=utf-8' },
  '/demo/sync.js': { file: 'sync.js', type: 'text/javascript; charset=utf-8' },
  '/demo/live-client.js': { file: 'live-client.js', type: 'text/javascript; charset=utf-8' },
  '/demo/style.css': { file: 'style.css', type: 'text/css; charset=utf-8' },
};

/** Interface locale de démonstration, servie par Commandes (même origine : cookie de session et WebSocket). */
export const demoWebRoutes: FastifyPluginAsync = async (app) => {
  app.get('/demo', async (_request, reply) => reply.redirect('/demo/'));
  for (const [route, { file, type }] of Object.entries(FILES)) {
    app.get(route, async (_request, reply) => {
      const body = await readFile(new URL(file, PUBLIC_DIR));
      return reply
        .header('content-security-policy', "default-src 'self'; connect-src 'self'")
        .header('cache-control', 'no-store')
        .type(type)
        .send(body);
    });
  }
};
