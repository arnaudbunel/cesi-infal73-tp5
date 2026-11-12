import { createServer } from 'node:http';
import type { Server, ServerResponse } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { DEFAULT_STOCK_CONFIG } from './config.js';
import type { StockConfig } from './config.js';
import { lookupStock } from './catalog.js';

/**
 * Simulateur Stocks. Aucune route de contrôle : le comportement est fixé à la construction
 * (variables d'environnement au démarrage, ou `config` injectée localement par les tests).
 * Outil pédagogique : ne pas déployer.
 */

export function buildStockApp(partial: Partial<StockConfig> = {}): Server {
  const config: StockConfig = { ...DEFAULT_STOCK_CONFIG, ...partial };

  const server = createServer((req, res) => {
    const path = (req.url ?? '/').split('?')[0] ?? '/';
    void handle(req.method ?? 'GET', path, res, config).catch(() => {
      res.socket?.destroy();
    });
  });
  return server;
}

async function handle(method: string, path: string, res: ServerResponse, config: StockConfig): Promise<void> {
  const match = /^\/stock\/([^/]+)$/.exec(path);
  if (method !== 'GET' || !match) {
    return json(res, 404, { error: 'not found' });
  }
  const productId = decodeURIComponent(match[1] ?? '');

  if (config.latencyMs > 0) await sleep(config.latencyMs);
  if (res.destroyed) return; // le client a abandonné pendant la latence

  switch (config.mode) {
    case 'normal':
    {
      // Catalogue partagé avec gRPC : P12 et P13 sont connus ; le reste est inconnu (404).
      const available = lookupStock(productId, config.available);
      return available === undefined ? json(res, 404, { error: 'unknown product' }) : json(res, 200, { productId, available });
    }
    case 'status503':
      return json(res, 503, { error: 'service unavailable' });
    case 'invalid_json': {
      const body = '{"productId":"P12","available":';
      res.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
      res.end(body);
      return;
    }
    case 'disconnect': {
      // En-têtes annonçant 64 octets, 22 seulement envoyés, puis fermeture forcée de la connexion.
      const partialBody = '{"productId":"P12","a';
      res.writeHead(200, { 'content-type': 'application/json', 'content-length': 64 });
      res.write(partialBody, () => res.socket?.destroy());
      return;
    }
  }
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}
