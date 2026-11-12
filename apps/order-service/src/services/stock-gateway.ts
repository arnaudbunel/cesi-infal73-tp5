import type { StockCallContext, StockInfo } from './stock.client.js';

/** Abstraction du service Stocks côté Commandes : même contrat quel que soit le protocole (HTTP, gRPC). */
export interface StockGateway {
  getStock(productId: string, context?: StockCallContext): Promise<StockInfo>;
  /** Libère les ressources (canal gRPC...). */
  close?(): void | Promise<void>;
}
