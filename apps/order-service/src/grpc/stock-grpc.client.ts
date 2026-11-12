import * as grpc from '@grpc/grpc-js';
import type { OrderServiceConfig } from '../config.js';
import type { StockGateway } from '../services/stock-gateway.js';
import type { StockCallContext, StockInfo } from '../services/stock.client.js';
import { UpstreamError } from '../services/upstream-errors.js';
import { mapGrpcError } from './grpc-error-mapping.js';
import { loadStockServiceClient } from './stock-proto.js';
import type { StockServiceClient } from './stock-proto.js';

/**
 * Client gRPC du service Stocks (consultation uniquement, aucun retry).
 * Fourni : création du canal (connexion locale non chiffrée, laboratoire uniquement), `close()`,
 * cible et deadline lues dans la configuration (`stockGrpcTarget`, `stockGrpcDeadlineMs`).
 */
export class StockGrpcClient implements StockGateway {
  // Utilisé par votre implémentation de getStock().
  protected readonly client: StockServiceClient;

  constructor(protected readonly config: OrderServiceConfig) {
    const StockService = loadStockServiceClient();
    // En production : TLS (credentials.createSsl) et authentification entre services.
    // Reconnexion rapide après une panne de Stocks (retour à la normale sans redémarrer Commandes).
    this.client = new StockService(config.stockGrpcTarget, grpc.credentials.createInsecure(), {
      'grpc.initial_reconnect_backoff_ms': 100,
      'grpc.min_reconnect_backoff_ms': 100,
      'grpc.max_reconnect_backoff_ms': 1000,
    });
  }

  /**
   * Appel unary avec deadline absolue (`stockGrpcDeadlineMs`). Fail-fast : pas de `waitForReady`,
   * pas de retry, pas de repli HTTP. La réponse est validée (forme et valeurs) avant d'être exposée.
   * Un stock à 0 est une réponse valide.
   */
  async getStock(productId: string, _context: StockCallContext = {}): Promise<StockInfo> {
    const startedAt = performance.now();
    const elapsed = () => Math.round(performance.now() - startedAt);
    return new Promise<StockInfo>((resolve, reject) => {
      this.client.getStock({ productId }, { deadline: Date.now() + this.config.stockGrpcDeadlineMs }, (error, response) => {
        if (error) return reject(mapGrpcError(error, elapsed()));
        const valid =
          response !== undefined &&
          response.productId === productId &&
          Number.isSafeInteger(response.available) &&
          response.available >= 0 &&
          typeof response.reservable === 'boolean';
        if (!valid) return reject(new UpstreamError('invalid-payload', elapsed()));
        resolve({ productId: response.productId, available: response.available });
      });
    });
  }

  close(): void {
    this.client.close();
  }
}
