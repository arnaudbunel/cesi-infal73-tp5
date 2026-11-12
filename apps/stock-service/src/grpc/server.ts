import { setTimeout as sleep } from 'node:timers/promises';
import * as grpc from '@grpc/grpc-js';
import { lookupStock } from '../catalog.js';
import { DEFAULT_GRPC_STOCK_CONFIG } from '../config.js';
import type { GrpcStockConfig } from '../config.js';
import { getStock } from './get-stock.js';
import { loadStockService } from './stock-proto.js';
import type { StockRequest, StockResponse } from './stock-proto.js';

/** Compteurs d'observation (preuve de cause dans les tests : une deadline n'annule pas forcément le traitement). */
export interface GrpcStockStats {
  /** Appels reçus. */
  received: number;
  /** Appels dont le handler a répondu. */
  answered: number;
  /** Appels abandonnés par le client (deadline expirée, annulation) avant réponse. */
  cancelled: number;
}

export interface GrpcStockServer {
  stats(): GrpcStockStats;
  /** Démarre l'écoute et retourne le port réellement lié (utile avec le port 0 en test). */
  start(): Promise<number>;
  /** Arrêt propre ; force la fermeture si des appels restent en cours après `graceMs`. */
  stop(graceMs?: number): Promise<void>;
}

/** Serveur gRPC Stocks, indépendant du serveur HTTP, partageant le même catalogue (`catalog.ts`). */
export function buildGrpcStockServer(partial: Partial<GrpcStockConfig> = {}): GrpcStockServer {
  const config: GrpcStockConfig = { ...DEFAULT_GRPC_STOCK_CONFIG, ...partial };
  const server = new grpc.Server();
  const stats: GrpcStockStats = { received: 0, answered: 0, cancelled: 0 };
  const deps = { lookup: (productId: string) => lookupStock(productId, config.available) };

  server.addService(loadStockService(), {
    getStock: async (call: grpc.ServerUnaryCall<StockRequest, StockResponse>, callback: grpc.sendUnaryData<StockResponse>) => {
      stats.received += 1;
      // Latence déterministe, avant le handler.
      if (config.latencyMs > 0) await sleep(config.latencyMs);
      if (call.cancelled) {
        stats.cancelled += 1; // le client a abandonné (deadline expirée) : rien à renvoyer
        return;
      }
      getStock(call, callback, deps);
      stats.answered += 1;
    },
  });

  return {
    stats: () => ({ ...stats }),
    start: () =>
      new Promise<number>((resolve, reject) => {
        // Liaison locale non chiffrée : laboratoire uniquement (TLS exigé en production).
        server.bindAsync(`${config.host}:${config.port}`, grpc.ServerCredentials.createInsecure(), (error, port) =>
          error ? reject(error) : resolve(port),
        );
      }),
    stop: (graceMs = 500) =>
      new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          server.forceShutdown();
          resolve();
        }, graceMs);
        server.tryShutdown(() => {
          clearTimeout(timer);
          resolve();
        });
      }),
  };
}
