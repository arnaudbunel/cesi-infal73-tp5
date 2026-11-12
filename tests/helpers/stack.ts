import { createServer } from 'node:http';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../../apps/order-service/src/app.js';
import type { BuildAppOptions } from '../../apps/order-service/src/app.js';
import { buildStockApp } from '../../apps/stock-service/src/app.js';
import type { StockConfig } from '../../apps/stock-service/src/config.js';

export async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as AddressInfo).port;
}

export async function close(server: Server): Promise<void> {
  server.closeAllConnections();
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

export interface Stack {
  app: FastifyInstance;
  stockPort: number;
  stop: () => Promise<void>;
}

/** Démarre un vrai serveur Stocks (port éphémère) et une application Commandes pointant dessus. */
export async function startStack(stock: Partial<StockConfig> = {}, orders: BuildAppOptions = {}): Promise<Stack> {
  const stockServer = buildStockApp(stock);
  const stockPort = await listen(stockServer);
  const app = buildApp({ stockBaseUrl: `http://127.0.0.1:${stockPort}`, ...orders });
  await app.ready();
  return {
    app,
    stockPort,
    stop: async () => {
      await app.close();
      await close(stockServer);
    },
  };
}

/** Serveur Stocks « sur mesure » pour fournir des corps arbitraires (tests de validation). */
export async function startCustomStock(status: number, body: string): Promise<{ port: number; stop: () => Promise<void> }> {
  const server = createServer((_req, res) => {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(body);
  });
  const port = await listen(server);
  return { port, stop: () => close(server) };
}

/** Port local sur lequel plus rien n'écoute (connexion refusée). */
export async function freePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await close(server);
  return port;
}

// ───────────── TP3 : gRPC et WebSocket ─────────────
import type { AddressInfo as _Addr } from 'node:net';
import * as grpc from '@grpc/grpc-js';
import WebSocket from 'ws';
import { buildGrpcStockServer } from '../../apps/stock-service/src/grpc/server.js';
import type { GrpcStockConfig } from '../../apps/stock-service/src/config.js';
import { loadStockServiceClient } from '../../apps/order-service/src/grpc/stock-proto.js';
import type { StockResponse } from '../../apps/order-service/src/grpc/stock-proto.js';

export interface FullStack {
  /** URL de base de Commandes (vrai serveur HTTP, port éphémère, 127.0.0.1). */
  baseUrl: string;
  wsUrl: string;
  stockPort: number;
  grpcTarget: string;
  grpcPort: number;
  app: FastifyInstance;
  stop: () => Promise<void>;
  stopGrpc: () => Promise<void>;
}

/**
 * Stocks HTTP + Stocks gRPC + Commandes, sur des ports éphémères isolés.
 * `orders.stockBackend` vaut 'http' par défaut (comme la configuration livrée).
 */
export async function startFullStack(
  options: { grpc?: Partial<GrpcStockConfig>; stock?: Partial<StockConfig>; orders?: BuildAppOptions } = {},
): Promise<FullStack> {
  const stockServer = buildStockApp(options.stock);
  const stockPort = await listen(stockServer);
  const grpcServer = buildGrpcStockServer({ port: 0, latencyMs: 0, ...options.grpc });
  const grpcPort = await grpcServer.start();
  const grpcTarget = `127.0.0.1:${grpcPort}`;
  const app = buildApp({
    stockBaseUrl: `http://127.0.0.1:${stockPort}`,
    stockGrpcTarget: grpcTarget,
    ...options.orders,
  });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const port = (app.server.address() as _Addr).port;
  let grpcStopped = false;
  const stopGrpc = async () => {
    if (grpcStopped) return;
    grpcStopped = true;
    await grpcServer.stop(200);
  };
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    wsUrl: `ws://127.0.0.1:${port}/ws/orders`,
    stockPort,
    grpcTarget,
    grpcPort,
    app,
    stopGrpc,
    stop: async () => {
      for (const client of [...openClients]) client.socket.terminate(); // sockets restées ouvertes par un test en échec
      openClients.clear();
      await app.close();
      await stopGrpc();
      await close(stockServer);
    },
  };
}

/** Appel gRPC réel (connexion locale) vers un serveur Stocks. `deadlineMs` : deadline côté client. */
export function callGetStock(
  target: string,
  productId: string,
  deadlineMs = 2000,
): Promise<{ error: grpc.ServiceError | null; response?: StockResponse }> {
  const StockService = loadStockServiceClient();
  const client = new StockService(target, grpc.credentials.createInsecure());
  return new Promise((resolve) => {
    client.getStock({ productId }, { deadline: Date.now() + deadlineMs }, (error, response) => {
      client.close();
      resolve(response ? { error, response } : { error });
    });
  });
}

/** Ouvre une session démo (cookie signé) et retourne l'en-tête Cookie à renvoyer. */
export async function demoLogin(baseUrl: string, user: 'alice' | 'bob'): Promise<string> {
  const res = await fetch(`${baseUrl}/api/demo/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ user }),
  });
  if (res.status !== 200) throw new Error(`demo login failed: ${res.status}`);
  const setCookie = res.headers.getSetCookie()[0];
  if (!setCookie) throw new Error('no session cookie');
  return setCookie.split(';')[0] ?? '';
}

/** Client WebSocket réel (`ws`) avec file de messages. Aucune temporisation : on attend des événements. */
const openClients = new Set<WsClient>();

export class WsClient {
  private readonly queue: unknown[] = [];
  private waiter: ((message: unknown) => void) | null = null;
  readonly closed: Promise<number>;

  private constructor(readonly socket: WebSocket) {
    socket.on('message', (data) => {
      const message: unknown = JSON.parse(String(data));
      if (this.waiter) {
        const resolve = this.waiter;
        this.waiter = null;
        resolve(message);
      } else this.queue.push(message);
    });
    this.closed = new Promise((resolve) => socket.on('close', (code) => resolve(code)));
  }

  static async connect(url: string, cookie?: string): Promise<WsClient> {
    const socket = new WebSocket(url, cookie ? { headers: { cookie } } : {});
    await new Promise<void>((resolve, reject) => {
      socket.once('open', resolve);
      socket.once('error', reject);
      socket.once('unexpected-response', (_req, res) => reject(new Error(`handshake refused: ${res.statusCode}`)));
    });
    const client = new WsClient(socket);
    openClients.add(client);
    return client;
  }

  send(message: unknown): void {
    this.socket.send(typeof message === 'string' ? message : JSON.stringify(message));
  }

  next(): Promise<unknown> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    // Garde-fou : une réponse qui n'arrive jamais devient un échec lisible (pas un blocage de la suite).
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiter = null;
        reject(new Error('aucun message WebSocket reçu après 5 s'));
      }, 5000);
      this.waiter = (message) => {
        clearTimeout(timer);
        resolve(message);
      };
    });
  }

  /** Envoie une demande et attend la réponse suivante. */
  async request(message: unknown): Promise<unknown> {
    this.send(message);
    return this.next();
  }

  /**
   * Barrière déterministe : un message invalide provoque une réponse INVALID_MESSAGE, et les messages
   * d'une socket sont ordonnés. Tout ce qui a été émis avant est donc retourné ici (hors sentinelle).
   */
  async drain(): Promise<unknown[]> {
    this.send('{"sentinel":true}');
    const received: unknown[] = [];
    for (;;) {
      const message = await this.next();
      if (typeof message === 'object' && message !== null && (message as { code?: string }).code === 'INVALID_MESSAGE') return received;
      received.push(message);
    }
  }

  async close(): Promise<void> {
    openClients.delete(this);
    if (this.socket.readyState === WebSocket.OPEN) this.socket.close();
    await this.closed;
  }
}

export async function patchStatus(baseUrl: string, orderId: string, status: string): Promise<Response> {
  return fetch(`${baseUrl}/api/orders/${orderId}`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ status }),
  });
}

export async function getOrder(baseUrl: string, orderId: string): Promise<{ status: string; sequence: number }> {
  const res = await fetch(`${baseUrl}/api/orders/${orderId}`);
  return (await res.json()) as { status: string; sequence: number };
}
