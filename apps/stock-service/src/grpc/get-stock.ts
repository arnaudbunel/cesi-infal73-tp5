import * as grpc from '@grpc/grpc-js';
import type { StockRequest, StockResponse } from './stock-proto.js';

/** Accès au catalogue partagé avec l'interface HTTP : quantité, ou `undefined` si le produit est inconnu. */
export interface StockDeps {
  lookup(productId: string): number | undefined;
}

/**
 * GetStock : consultation pure (aucune réservation).
 *  - identifiant absent/vide (après trim) -> INVALID_ARGUMENT ;
 *  - produit inconnu -> NOT_FOUND ;
 *  - produit connu -> OK. `available = 0` est une réponse valide (reservable=false) : on compare à
 *    `undefined`, jamais `if (!available)`.
 */
export function getStock(
  call: grpc.ServerUnaryCall<StockRequest, StockResponse>,
  callback: grpc.sendUnaryData<StockResponse>,
  deps: StockDeps,
): void {
  const productId = typeof call.request.productId === 'string' ? call.request.productId.trim() : '';
  if (productId === '') {
    return void callback({ code: grpc.status.INVALID_ARGUMENT, details: 'product_id is required' });
  }
  const available = deps.lookup(productId);
  if (available === undefined) {
    return void callback({ code: grpc.status.NOT_FOUND, details: 'unknown product' });
  }
  callback(null, { productId, available, reservable: available > 0 });
}
